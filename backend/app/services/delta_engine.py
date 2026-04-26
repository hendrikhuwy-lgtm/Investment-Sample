from __future__ import annotations

import json
import sqlite3
import statistics
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from app.services.data_lag import classify_lag_cause, compute_lag_days
from app.services.narrative_engine import build_alert_narrative, no_material_change_text
from app.services.portfolio_impact import quantify_portfolio_impact


STATE_LEVEL: dict[str, int] = {
    "stable": 1,
    "normalizing": 2,
    "elevated": 3,
    "stress_emerging": 4,
    "stress_regime": 5,
}
ACTIVE_STATES = {"elevated", "stress_emerging", "stress_regime"}

METRIC_FRIENDLY_NAMES: dict[str, str] = {
    "DGS10": "US 10Y Treasury Yield",
    "T10YIE": "US 10Y Breakeven Inflation",
    "VIXCLS": "CBOE VIX",
    "BAMLH0A0HYM2": "US High Yield OAS",
    "SP500": "SP 500 Index",
}

PERCENTILE_THRESHOLDS: tuple[dict[str, Any], ...] = (
    {"name": "p60_elevated", "value": 60.0, "direction": "up"},
    {"name": "p60_stress_emerging", "value": 80.0, "direction": "up"},
    {"name": "p60_stress_regime", "value": 90.0, "direction": "up"},
    {"name": "p60_revert_below_elevated", "value": 60.0, "direction": "down"},
)


def ensure_delta_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS metric_snapshots (
              snapshot_id TEXT PRIMARY KEY,
              asof_ts TEXT NOT NULL,
              metric_id TEXT NOT NULL,
              metric_name TEXT,
              value REAL NOT NULL,
              delta_1d REAL,
              window_5_change REAL,
              window_20_change REAL,
              window_60_range_low REAL,
              window_60_range_high REAL,
              percentile_60 REAL,
              prev_percentile_60 REAL,
              percentile_shift REAL,
              stddev_60 REAL,
              state_short TEXT,
              days_in_state_short INTEGER,
              citations_json TEXT NOT NULL
            )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_metric_snapshots_metric_asof
        ON metric_snapshots (metric_id, asof_ts DESC)
        """
    )
    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS regime_snapshots (
              snapshot_id TEXT PRIMARY KEY,
              asof_ts TEXT NOT NULL,
              horizon TEXT NOT NULL,
              state TEXT NOT NULL,
              days_in_state INTEGER NOT NULL DEFAULT 1,
              confidence REAL NOT NULL,
              contributors_json TEXT NOT NULL,
              citations_json TEXT NOT NULL
            )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_regime_snapshots_horizon_asof
        ON regime_snapshots (horizon, asof_ts DESC)
        """
    )
    conn.execute(
        """
            CREATE TABLE IF NOT EXISTS alert_events_v2 (
          alert_id TEXT PRIMARY KEY,
          asof_ts TEXT NOT NULL,
          horizon TEXT NOT NULL,
          metric_id TEXT NOT NULL,
          prev_state TEXT NOT NULL,
          curr_state TEXT NOT NULL,
          lifecycle TEXT NOT NULL,
          severity INTEGER NOT NULL,
          threshold_name TEXT NOT NULL,
          threshold_value REAL NOT NULL,
          current_value REAL NOT NULL,
          delta_value REAL NOT NULL,
          delta_window TEXT NOT NULL,
          percentile_60 REAL,
          days_in_state INTEGER NOT NULL,
          impact_score REAL NOT NULL,
          impact_map_json TEXT NOT NULL,
          narrative_md TEXT NOT NULL,
          citations_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_alert_events_v2_asof
        ON alert_events_v2 (asof_ts DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_alert_events_v2_metric
        ON alert_events_v2 (metric_id, horizon, asof_ts DESC)
        """
    )
    metric_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(metric_snapshots)").fetchall()}
    if "metric_name" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN metric_name TEXT")
    if "delta_1d" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN delta_1d REAL")
    if "prev_percentile_60" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN prev_percentile_60 REAL")
    if "percentile_shift" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN percentile_shift REAL")
    if "state_short" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN state_short TEXT")
    if "days_in_state_short" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN days_in_state_short INTEGER")
    if "observed_at" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN observed_at TEXT")
    if "retrieved_at" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN retrieved_at TEXT")
    if "lag_days" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN lag_days INTEGER")
    if "lag_class" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN lag_class TEXT")
    if "lag_cause" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN lag_cause TEXT")

    regime_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(regime_snapshots)").fetchall()}
    if "days_in_state" not in regime_columns:
        conn.execute("ALTER TABLE regime_snapshots ADD COLUMN days_in_state INTEGER NOT NULL DEFAULT 1")
    conn.commit()


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalized_citations(citation_payload: Any) -> list[dict[str, Any]]:
    citations: list[dict[str, Any]] = []
    payloads: list[Any]
    if citation_payload is None:
        payloads = []
    elif isinstance(citation_payload, list):
        payloads = citation_payload
    else:
        payloads = [citation_payload]
    for item in payloads:
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        retrieved_at = str(item.get("retrieved_at") or "").strip()
        source_id = str(item.get("source_id") or "").strip() or "unknown_source"
        importance = str(item.get("importance") or "source reference").strip()
        if not url or not retrieved_at:
            continue
        payload: dict[str, Any] = {
            "url": url,
            "source_id": source_id,
            "retrieved_at": retrieved_at,
            "importance": importance,
        }
        observed_at = str(item.get("observed_at") or "").strip()
        if observed_at:
            payload["observed_at"] = observed_at
        if item.get("lag_days") is not None:
            try:
                payload["lag_days"] = int(item.get("lag_days"))
            except Exception:  # noqa: BLE001
                pass
        lag_class = str(item.get("lag_class") or "").strip()
        if lag_class:
            payload["lag_class"] = lag_class
        lag_cause = str(item.get("lag_cause") or "").strip()
        if lag_cause:
            payload["lag_cause"] = lag_cause
        citations.append(payload)
    return citations


def _extract_lag_fields(metric_bundle: dict[str, Any], asof_text: str) -> tuple[str | None, str, int | None, str | None, str]:
    citation_payload = metric_bundle.get("citation")
    observed_at: str | None = str(metric_bundle.get("latest_date") or "").strip() or None
    retrieved_at: str = asof_text
    lag_days: int | None = None
    lag_class: str | None = None
    lag_cause: str = "unknown"
    retrieval_succeeded = True

    citation_dict: dict[str, Any] | None = None
    if isinstance(citation_payload, dict):
        citation_dict = citation_payload
    elif hasattr(citation_payload, "model_dump"):
        try:
            dumped = citation_payload.model_dump(mode="json")
            if isinstance(dumped, dict):
                citation_dict = dumped
        except Exception:  # noqa: BLE001
            citation_dict = None

    if citation_dict:
        if str(citation_dict.get("observed_at") or "").strip():
            observed_at = str(citation_dict.get("observed_at")).strip()
        if str(citation_dict.get("retrieved_at") or "").strip():
            retrieved_at = str(citation_dict.get("retrieved_at")).strip()
        if citation_dict.get("lag_days") is not None:
            try:
                lag_days = int(citation_dict.get("lag_days"))
            except Exception:  # noqa: BLE001
                lag_days = None
        lag_class = str(citation_dict.get("lag_class") or "").strip() or None
        lag_cause = str(citation_dict.get("lag_cause") or "unknown").strip() or "unknown"
        importance = str(citation_dict.get("importance") or "").lower()
        retrieval_succeeded = "retrieval=cached" not in importance

    if lag_days is None or lag_class is None:
        lag_days, lag_class = compute_lag_days(observed_at, retrieved_at, timezone="Asia/Shanghai")
    if lag_cause not in {"expected_publication_lag", "unexpected_ingestion_lag", "unknown"}:
        lag_cause = classify_lag_cause(
            series_key=str(metric_bundle.get("series_code") or metric_bundle.get("metric_id") or ""),
            observed_at=observed_at,
            retrieved_at=retrieved_at,
            lag_days=lag_days,
            retrieval_succeeded=retrieval_succeeded,
            cache_fallback_used=not retrieval_succeeded,
            latest_available_matches_observed=True,
            previous_observed_at=None,
        )

    return observed_at, retrieved_at, lag_days, lag_class, lag_cause


def _percentile_rank(values: list[float], current: float) -> float:
    if not values:
        return 50.0
    count = sum(1 for value in values if value <= current)
    return round((count / len(values)) * 100.0, 2)


def _threshold_crossing_detector(
    previous_value: float | None,
    current_value: float | None,
    threshold_name: str,
    threshold_value: float,
    direction: str,
    threshold_kind: str = "percentile",
) -> dict[str, Any]:
    if previous_value is None or current_value is None:
        return {
            "threshold_name": threshold_name,
            "threshold_value": threshold_value,
            "threshold_kind": threshold_kind,
            "crossed_up": False,
            "crossed_down": False,
            "distance_to_revert": None,
        }
    crossed_up = previous_value < threshold_value <= current_value
    crossed_down = previous_value > threshold_value >= current_value
    if threshold_kind == "percentile":
        distance_to_revert = round(threshold_value - current_value, 4)
        distance_unit = "pct pts"
    else:
        distance_to_revert = round(threshold_value - current_value, 4)
        distance_unit = "value units"
    return {
        "threshold_name": threshold_name,
        "threshold_value": threshold_value,
        "threshold_kind": threshold_kind,
        "crossed_up": crossed_up,
        "crossed_down": crossed_down,
        "distance_to_revert": distance_to_revert,
        "distance_unit": distance_unit,
    }


def friendly_metric_name(metric_id: str) -> str:
    code = str(metric_id or "").upper()
    return METRIC_FRIENDLY_NAMES.get(code, code or "Unknown metric")


def compute_state(
    metric_bundle: dict[str, Any],
    horizon: str,
    previous_state: str | None = None,
) -> str:
    if str(horizon).lower() == "long":
        percentile = _to_float(metric_bundle.get("score_percentile"))
        if percentile is None:
            percentile = _to_float(metric_bundle.get("long_percentile"))
        if percentile is None:
            percentile = _to_float(metric_bundle.get("percentile_60"))
    else:
        percentile = _to_float(metric_bundle.get("percentile_60"))
    percentile = percentile if percentile is not None else 50.0
    if percentile >= 90:
        return "stress_regime"
    if percentile >= 80:
        return "stress_emerging"
    if percentile >= 60:
        return "elevated"
    if previous_state in ACTIVE_STATES:
        return "normalizing"
    return "stable"


def _bound_pct_shift(value: float) -> float:
    return round(max(-100.0, min(100.0, float(value))), 3)


def _parse_observed_date(value: Any) -> datetime.date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.strptime(text[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def _observation_change_details(
    *,
    observed_at: str | None,
    prior_observed_at: str | None,
    lag_cause: str | None = None,
) -> tuple[bool, str, str]:
    observed = _parse_observed_date(observed_at)
    prior_observed = _parse_observed_date(prior_observed_at)
    if observed is not None and (prior_observed is None or observed > prior_observed):
        return True, "new_observation", (
            f"New observation at observed_at {observed.isoformat()}."
        )
    if str(lag_cause or "").strip().lower() == "unexpected_ingestion_lag":
        if observed is None:
            return False, "cached_fallback", "No new observation and observed_at unavailable; cached fallback context detected."
        return False, "cached_fallback", f"No new observation since observed_at {observed.isoformat()}."
    if observed is None:
        return False, "no_new_observation", "No new observation; observed_at unavailable."
    return False, "no_new_observation", f"No new observation since observed_at {observed.isoformat()}."


def _material_change(delta_1d: float, stddev_60: float, threshold_crossed: bool) -> bool:
    if threshold_crossed:
        return True
    if stddev_60 <= 1e-12:
        return abs(delta_1d) > 0
    return abs(delta_1d) >= (0.5 * stddev_60)


def compute_metric_deltas(
    conn: sqlite3.Connection,
    graph_metadata: list[dict[str, Any]],
    asof_ts: datetime,
) -> list[dict[str, Any]]:
    metrics: list[dict[str, Any]] = []
    asof_text = asof_ts.isoformat()
    for row in graph_metadata:
        metric_id = str(row.get("series_code") or "").strip()
        if not metric_id:
            continue
        long_horizon = dict(row.get("long_horizon") or {})
        current_value = _to_float(long_horizon.get("latest"))
        if current_value is None:
            continue

        previous_rows = conn.execute(
            """
            SELECT value, percentile_60, stddev_60, asof_ts, state_short, days_in_state_short, observed_at
            FROM metric_snapshots
            WHERE metric_id = ?
            ORDER BY asof_ts DESC
            LIMIT 60
            """,
            (metric_id,),
        ).fetchall()
        previous_values = [float(item["value"]) for item in previous_rows]
        previous_value = previous_values[0] if previous_values else None
        history_60 = list(reversed(previous_values[:59])) + [current_value]

        if len(previous_values) >= 5:
            window_5_change = current_value - previous_values[4]
        else:
            window_5_change = _to_float(dict(row.get("short_horizon") or {}).get("change_5obs")) or 0.0
        if len(previous_values) >= 20:
            window_20_change = current_value - previous_values[19]
        else:
            window_20_change = _to_float(dict(row.get("short_horizon") or {}).get("momentum_20obs")) or 0.0

        range_low = min(history_60) if history_60 else current_value
        range_high = max(history_60) if history_60 else current_value
        percentile_60 = _percentile_rank(history_60, current_value)
        stddev_60 = statistics.pstdev(history_60) if len(history_60) >= 2 else 0.0
        delta_1d = (current_value - previous_value) if previous_value is not None else 0.0
        previous_percentile = _to_float(previous_rows[0]["percentile_60"]) if previous_rows else None
        prior_observed_at = str(previous_rows[0]["observed_at"] or "").strip() if previous_rows else ""
        percentile_shift = _bound_pct_shift((percentile_60 - previous_percentile) if previous_percentile is not None else 0.0)
        previous_state = str(previous_rows[0]["state_short"]) if previous_rows and previous_rows[0]["state_short"] else compute_state(
            {"percentile_60": previous_percentile},
            "short",
            None,
        )
        current_state = compute_state({"percentile_60": percentile_60}, "short", previous_state)
        previous_days = int(previous_rows[0]["days_in_state_short"] or 1) if previous_rows else 0
        days_in_state_short = (previous_days + 1) if (previous_state == current_state and previous_rows) else 1
        threshold_crossed = False
        if previous_percentile is not None:
            for threshold in (60.0, 80.0, 90.0):
                crossed_up = previous_percentile < threshold <= percentile_60
                crossed_down = previous_percentile > threshold >= percentile_60
                if crossed_up or crossed_down:
                    threshold_crossed = True
                    break
        citations = _normalized_citations(row.get("citation"))
        metric_name = friendly_metric_name(metric_id)
        observed_at, retrieved_at, lag_days, lag_class, lag_cause = _extract_lag_fields(row, asof_text)
        new_observation_since_prior_run, change_type, observation_status_text = _observation_change_details(
            observed_at=observed_at,
            prior_observed_at=prior_observed_at,
            lag_cause=lag_cause,
        )

        snapshot_id = f"ms_{metric_id}_{uuid.uuid4().hex[:10]}"
        conn.execute(
            """
            INSERT INTO metric_snapshots (
              snapshot_id, asof_ts, metric_id, metric_name, observed_at, retrieved_at, lag_days, lag_class, lag_cause,
              value, delta_1d, window_5_change, window_20_change,
              window_60_range_low, window_60_range_high, percentile_60, prev_percentile_60, percentile_shift, stddev_60,
              state_short, days_in_state_short, citations_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                asof_text,
                metric_id,
                metric_name,
                observed_at,
                retrieved_at,
                lag_days,
                lag_class,
                lag_cause,
                float(current_value),
                float(delta_1d),
                float(window_5_change),
                float(window_20_change),
                float(range_low),
                float(range_high),
                float(percentile_60),
                previous_percentile,
                float(percentile_shift),
                float(stddev_60),
                current_state,
                int(days_in_state_short),
                json.dumps(citations),
            ),
        )
        metrics.append(
            {
                "snapshot_id": snapshot_id,
                "asof_ts": asof_text,
                "metric_id": metric_id,
                "metric_name": metric_name,
                "value": float(current_value),
                "observed_at": observed_at,
                "retrieved_at": retrieved_at,
                "lag_days": lag_days,
                "lag_class": lag_class,
                "lag_cause": lag_cause,
                "prior_observed_at": prior_observed_at or None,
                "new_observation_since_prior_run": new_observation_since_prior_run,
                "change_type": change_type,
                "observation_status_text": observation_status_text,
                "delta_1d": float(delta_1d),
                "window_5_change": float(window_5_change),
                "window_20_change": float(window_20_change),
                "window_60_range_low": float(range_low),
                "window_60_range_high": float(range_high),
                "percentile_60": float(percentile_60),
                "stddev_60": float(stddev_60),
                "prev_percentile_60": previous_percentile,
                "percentile_shift": float(percentile_shift),
                "prev_state": previous_state,
                "curr_state": current_state,
                "days_in_state_short": int(days_in_state_short),
                "threshold_crossed": threshold_crossed,
                "citations": citations,
            }
        )
    conn.commit()
    return metrics


def _metric_level_for_lifecycle(state: str) -> int:
    return STATE_LEVEL.get(state, 1)


def _days_in_state(
    conn: sqlite3.Connection,
    *,
    metric_id: str,
    horizon: str,
    curr_state: str,
) -> int:
    rows = conn.execute(
        """
        SELECT curr_state
        FROM alert_events_v2
        WHERE metric_id = ? AND horizon = ?
        ORDER BY asof_ts DESC
        LIMIT 60
        """,
        (metric_id, horizon),
    ).fetchall()
    days = 1
    for row in rows:
        if str(row["curr_state"]) != curr_state:
            break
        days += 1
    return days


def _lifecycle_state(
    conn: sqlite3.Connection,
    *,
    metric_id: str,
    horizon: str,
    prev_state: str,
    curr_state: str,
    asof_ts: datetime,
) -> str:
    prev_level = _metric_level_for_lifecycle(prev_state)
    curr_level = _metric_level_for_lifecycle(curr_state)
    if curr_level >= 3:
        lookback = (asof_ts - timedelta(days=30)).isoformat()
        seen = conn.execute(
            """
            SELECT 1
            FROM alert_events_v2
            WHERE metric_id = ? AND horizon = ? AND asof_ts >= ? AND curr_state IN ('elevated', 'stress_emerging', 'stress_regime')
            LIMIT 1
            """,
            (metric_id, horizon, lookback),
        ).fetchone()
        if seen is None:
            return "new"
    if prev_level >= 3 and curr_level < 3:
        return "de_escalated"
    if curr_level > prev_level:
        return "escalated"
    if curr_level >= 3:
        return "persisting"
    return "de_escalated" if prev_level >= 3 else "persisting"


def _state_from_score(score: float, previous_state: str | None) -> str:
    return compute_state({"score_percentile": score}, "long", previous_state)


def _confidence_from_score(score: float) -> float:
    confidence = abs(score - 50.0) / 50.0
    return round(max(0.0, min(1.0, confidence)), 3)


def _stress_adjusted_percentile(metric_id: str, percentile: float) -> float:
    if metric_id == "SP500":
        return 100.0 - percentile
    return percentile


def _build_regime_snapshot(
    conn: sqlite3.Connection,
    *,
    asof_ts: datetime,
    horizon: str,
    metric_rows: list[dict[str, Any]],
    score_fn: str,
) -> dict[str, Any]:
    if not metric_rows:
        previous = conn.execute(
            """
            SELECT state, confidence
            FROM regime_snapshots
            WHERE horizon = ?
            ORDER BY asof_ts DESC
            LIMIT 1
            """,
            (horizon,),
        ).fetchone()
        prev_state = str(previous["state"]) if previous is not None else "stable"
        prev_conf = float(previous["confidence"]) if previous is not None else 0.0
        state = "stable"
        confidence = 0.0
        transition = {
            "prev_state": prev_state,
            "curr_state": state,
            "confidence_prev": prev_conf,
            "confidence_curr": confidence,
            "confidence_delta": round(confidence - prev_conf, 3),
            "contributors_top": [],
        }
        return {
            "horizon": horizon,
            "state": state,
            "confidence": confidence,
            "contributors_top": [],
            "citations": [],
            "transition": transition,
        }

    values: list[float] = []
    contributors: list[dict[str, Any]] = []
    citations: list[dict[str, str]] = []
    for row in metric_rows:
        if score_fn == "long":
            percentile = _to_float(row.get("long_percentile"))
        else:
            percentile = _to_float(row.get("percentile_60"))
        if percentile is None:
            continue
        adjusted = _stress_adjusted_percentile(str(row.get("metric_id")), float(percentile))
        values.append(adjusted)
        contributors.append(
            {
                "metric_id": row.get("metric_id"),
                "percentile": round(float(percentile), 2),
                "adjusted_percentile": round(float(adjusted), 2),
                "delta_1d": round(float(row.get("delta_1d", 0.0)), 4),
                "value": round(float(row.get("value", 0.0)), 4),
                "citations": row.get("citations", []),
            }
        )
        citations.extend(list(row.get("citations", [])))

    score = statistics.mean(values) if values else 50.0
    previous = conn.execute(
        """
        SELECT state, confidence, days_in_state
        FROM regime_snapshots
        WHERE horizon = ?
        ORDER BY asof_ts DESC
        LIMIT 1
        """,
        (horizon,),
    ).fetchone()
    prev_state = str(previous["state"]) if previous is not None else "stable"
    prev_conf = float(previous["confidence"]) if previous is not None else 0.0
    prev_days = int(previous["days_in_state"] or 1) if previous is not None else 0
    state = _state_from_score(score, prev_state)
    confidence = _confidence_from_score(score)
    days_in_state = (prev_days + 1) if state == prev_state and previous is not None else 1
    contributors_top = sorted(
        contributors,
        key=lambda item: abs(float(item.get("adjusted_percentile", 50.0)) - 50.0),
        reverse=True,
    )[:3]

    dedup_citations: dict[tuple[str, str], dict[str, str]] = {}
    for citation in citations:
        key = (str(citation.get("url", "")), str(citation.get("retrieved_at", "")))
        if key[0] and key[1]:
            dedup_citations[key] = citation

    transition = {
        "prev_state": prev_state,
        "curr_state": state,
        "confidence_prev": prev_conf,
        "confidence_curr": confidence,
        "confidence_delta": round(confidence - prev_conf, 3),
        "days_in_state": days_in_state,
        "contributors_top": contributors_top,
    }
    snapshot_id = f"reg_{horizon}_{uuid.uuid4().hex[:10]}"
    payload = {
        "contributors_top": contributors_top,
        "transition": transition,
    }
    citations_json = list(dedup_citations.values())
    conn.execute(
        """
        INSERT INTO regime_snapshots (
          snapshot_id, asof_ts, horizon, state, days_in_state, confidence, contributors_json, citations_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            asof_ts.isoformat(),
            horizon,
            state,
            days_in_state,
            confidence,
            json.dumps(payload),
            json.dumps(citations_json),
        ),
    )
    return {
        "horizon": horizon,
        "state": state,
        "confidence": confidence,
        "days_in_state": days_in_state,
        "contributors_top": contributors_top,
        "citations": citations_json,
        "transition": transition,
    }


def _default_threshold(metric: dict[str, Any], crossing: dict[str, Any] | None) -> dict[str, Any]:
    if crossing is not None:
        return crossing
    percentile = float(metric.get("percentile_60", 0.0) or 0.0)
    if percentile >= 90:
        return {
            "threshold_name": "p60_stress_regime",
            "threshold_value": 90.0,
            "threshold_kind": "percentile",
            "distance_to_revert": 90.0 - percentile,
            "distance_unit": "pct pts",
        }
    if percentile >= 80:
        return {
            "threshold_name": "p60_stress_emerging",
            "threshold_value": 80.0,
            "threshold_kind": "percentile",
            "distance_to_revert": 80.0 - percentile,
            "distance_unit": "pct pts",
        }
    if percentile >= 60:
        return {
            "threshold_name": "p60_elevated",
            "threshold_value": 60.0,
            "threshold_kind": "percentile",
            "distance_to_revert": 60.0 - percentile,
            "distance_unit": "pct pts",
        }
    return {
        "threshold_name": "p60_revert_below_elevated",
        "threshold_value": 60.0,
        "threshold_kind": "percentile",
        "distance_to_revert": 60.0 - percentile,
        "distance_unit": "pct pts",
    }


def _persist_alerts_v2(
    conn: sqlite3.Connection,
    *,
    asof_ts: datetime,
    metrics: list[dict[str, Any]],
    sleeve_weights: dict[str, float],
    holdings_available: bool,
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    for metric in metrics:
        prev_pct = _to_float(metric.get("prev_percentile_60"))
        curr_pct = _to_float(metric.get("percentile_60"))
        crossings = [
            _threshold_crossing_detector(prev_pct, curr_pct, item["name"], float(item["value"]), str(item["direction"]))
            for item in PERCENTILE_THRESHOLDS
        ]
        crossed = [item for item in crossings if bool(item.get("crossed_up")) or bool(item.get("crossed_down"))]
        threshold_crossed = len(crossed) > 0
        curr_state = str(metric.get("curr_state", "stable"))
        prev_state = str(metric.get("prev_state", "stable"))
        curr_level = _metric_level_for_lifecycle(curr_state)
        prev_level = _metric_level_for_lifecycle(prev_state)
        material = _material_change(
            delta_1d=float(metric.get("delta_1d", 0.0)),
            stddev_60=float(metric.get("stddev_60", 0.0)),
            threshold_crossed=threshold_crossed,
        )
        if not material and curr_level < 3 and prev_level < 3 and curr_state == prev_state:
            continue

        threshold = _default_threshold(metric, crossed[0] if crossed else None)
        lifecycle = _lifecycle_state(
            conn,
            metric_id=str(metric.get("metric_id")),
            horizon="short",
            prev_state=prev_state,
            curr_state=curr_state,
            asof_ts=asof_ts,
        )
        days_in_state = int(metric.get("days_in_state_short") or 1)
        impact_map = quantify_portfolio_impact(
            str(metric.get("metric_id")),
            sleeve_weights,
            holdings_available=holdings_available,
        )
        citations = list(metric.get("citations", []))
        blocked = len(citations) == 0
        block_reason = "Missing citations for external metric statement."
        since_when = (asof_ts - timedelta(days=max(0, days_in_state - 1))).date().isoformat()
        alert_payload = {
            "alert_id": f"al2_{uuid.uuid4().hex[:12]}",
            "asof_ts": asof_ts.isoformat(),
            "horizon": "short",
            "metric_id": str(metric.get("metric_id")),
            "metric_name": str(metric.get("metric_name") or friendly_metric_name(str(metric.get("metric_id")))),
            "prev_state": prev_state,
            "curr_state": curr_state,
            "lifecycle": lifecycle,
            "severity": curr_level,
            "threshold_name": str(threshold["threshold_name"]),
            "threshold_value": float(threshold["threshold_value"]),
            "current_value": float(metric.get("value", 0.0)),
            "delta_value": float(metric.get("delta_1d", 0.0)),
            "delta_window": "run",
            "percentile_60": curr_pct,
            "percentile_shift": float(metric.get("percentile_shift", 0.0)),
            "days_in_state": int(days_in_state),
            "impact_score": float(impact_map["impact_score"]),
            "impact_map": impact_map,
            "distance_to_revert": float(threshold.get("distance_to_revert") or 0.0),
            "distance_unit": str(threshold.get("distance_unit") or "pct pts"),
            "threshold_kind": str(threshold.get("threshold_kind") or "percentile"),
            "since_when": since_when,
            "citations": citations,
            "blocked": blocked,
            "block_reason": block_reason if blocked else None,
        }
        alert_payload["narrative_md"] = (
            f"BLOCKED: {block_reason}"
            if blocked
            else build_alert_narrative(alert_payload)
        )
        conn.execute(
            """
            INSERT INTO alert_events_v2 (
              alert_id, asof_ts, horizon, metric_id, prev_state, curr_state, lifecycle, severity,
              threshold_name, threshold_value, current_value, delta_value, delta_window, percentile_60,
              days_in_state, impact_score, impact_map_json, narrative_md, citations_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert_payload["alert_id"],
                alert_payload["asof_ts"],
                alert_payload["horizon"],
                alert_payload["metric_id"],
                alert_payload["prev_state"],
                alert_payload["curr_state"],
                alert_payload["lifecycle"],
                alert_payload["severity"],
                alert_payload["threshold_name"],
                alert_payload["threshold_value"],
                alert_payload["current_value"],
                alert_payload["delta_value"],
                alert_payload["delta_window"],
                alert_payload["percentile_60"],
                alert_payload["days_in_state"],
                alert_payload["impact_score"],
                json.dumps(alert_payload["impact_map"]),
                alert_payload["narrative_md"],
                json.dumps(citations),
            ),
        )
        alerts.append(alert_payload)
    conn.commit()
    return alerts


def _driver_priority(metric: dict[str, Any]) -> float:
    stddev = float(metric.get("stddev_60", 0.0))
    scale = stddev if stddev > 1e-12 else 1.0
    normalized_move = abs(float(metric.get("delta_1d", 0.0))) / scale
    level_boost = _metric_level_for_lifecycle(str(metric.get("curr_state", "stable"))) * 0.25
    return normalized_move + level_boost


def _summary_from_alerts_and_metrics(
    *,
    alerts: list[dict[str, Any]],
    metrics: list[dict[str, Any]],
    long_regime: dict[str, Any],
    short_regime: dict[str, Any],
) -> dict[str, Any]:
    metric_map = {str(metric.get("metric_id") or ""): metric for metric in metrics}
    lifecycle_counts = {
        "new_alerts_count": sum(1 for item in alerts if item["lifecycle"] == "new"),
        "escalated_count": sum(1 for item in alerts if item["lifecycle"] == "escalated"),
        "de_escalated_count": sum(1 for item in alerts if item["lifecycle"] == "de_escalated"),
        "persisting_count": sum(1 for item in alerts if item["lifecycle"] == "persisting"),
    }

    candidate_metrics = [
        metric
        for metric in metrics
        if _material_change(
            delta_1d=float(metric.get("delta_1d", 0.0)),
            stddev_60=float(metric.get("stddev_60", 0.0)),
            threshold_crossed=bool(metric.get("threshold_crossed")),
        )
        or bool(metric.get("threshold_crossed"))
    ]
    top_change_drivers: list[dict[str, Any]] = []
    for metric in sorted(candidate_metrics, key=_driver_priority, reverse=True)[:3]:
        citations = list(metric.get("citations", []))
        blocked = len(citations) == 0
        top_change_drivers.append(
            {
                "metric_id": metric.get("metric_id"),
                "metric_name": metric.get("metric_name"),
                "delta": round(float(metric.get("delta_1d", 0.0)), 4),
                "window": "run",
                "percentile_shift": _bound_pct_shift(float(metric.get("percentile_shift", 0.0))),
                "percentile_shift_unit": "pct pts",
                "percentile_60": metric.get("percentile_60"),
                "state_transition": f"{metric.get('prev_state')} -> {metric.get('curr_state')}",
                "days_in_state": int(metric.get("days_in_state_short") or 1),
                "observed_at": metric.get("observed_at"),
                "retrieved_at": metric.get("retrieved_at"),
                "lag_days": metric.get("lag_days"),
                "lag_cause": metric.get("lag_cause"),
                "new_observation_since_prior_run": bool(metric.get("new_observation_since_prior_run")),
                "change_type": str(metric.get("change_type") or "no_new_observation"),
                "observation_status_text": str(metric.get("observation_status_text") or ""),
                "citations": citations,
                "blocked": blocked,
                "block_reason": "Missing citations for change driver." if blocked else None,
            }
        )

    no_material_movers = not any(
        _material_change(
            delta_1d=float(metric.get("delta_1d", 0.0)),
            stddev_60=float(metric.get("stddev_60", 0.0)),
            threshold_crossed=bool(metric.get("threshold_crossed")),
        )
        for metric in metrics
    )
    transition_changed = (
        str((short_regime.get("transition") or {}).get("prev_state", ""))
        != str((short_regime.get("transition") or {}).get("curr_state", ""))
        or str((long_regime.get("transition") or {}).get("prev_state", ""))
        != str((long_regime.get("transition") or {}).get("curr_state", ""))
    )
    no_material = no_material_movers and not transition_changed

    if no_material:
        daily_text = no_material_change_text(
            state=str(short_regime.get("state", "stable")),
            confidence=float(short_regime.get("confidence", 0.0)),
        )
    else:
        daily_text = (
            f"{lifecycle_counts['new_alerts_count']} new, {lifecycle_counts['escalated_count']} escalated, "
            f"{lifecycle_counts['de_escalated_count']} de-escalated, {lifecycle_counts['persisting_count']} persisting."
        )
    new_since_last_run: list[dict[str, Any]] = []
    for item in alerts[:5]:
        citations = list(item.get("citations", []))
        blocked = len(citations) == 0
        metric_id = str(item.get("metric_id") or "")
        metric_name = str(item.get("metric_name") or friendly_metric_name(metric_id))
        metric_row = metric_map.get(metric_id, {})
        observed_at = str(metric_row.get("observed_at") or "").strip() or None
        new_observation = bool(metric_row.get("new_observation_since_prior_run"))
        change_type = str(metric_row.get("change_type") or "no_new_observation")
        observation_status = str(metric_row.get("observation_status_text") or "").strip()
        current_pct = _to_float(item.get("percentile_60"))
        current_pct_text = f"{current_pct:.1f}" if current_pct is not None else "n/a"
        threshold_value = float(item.get("threshold_value", 0.0) or 0.0)
        if new_observation:
            change_line = (
                f"{metric_name} moved {float(item.get('delta_value', 0.0)):+.2f} over last run, "
                f"crossed {item.get('threshold_name')} at {threshold_value:.1f} pct, state moved "
                f"{item.get('prev_state')} to {item.get('curr_state')}, duration {int(item.get('days_in_state', 1))} day(s), "
                f"percentile shift {float(item.get('percentile_shift', 0.0)):+.1f} pct pts, current percentile {current_pct_text}. "
                f"{observation_status or ''}".strip()
            )
        else:
            observed_text = observed_at or "unknown date"
            change_line = (
                f"No new observation since observed_at {observed_text}. "
                f"{metric_name} run-to-run delta was {float(item.get('delta_value', 0.0)):+.2f} with state "
                f"{item.get('prev_state')} to {item.get('curr_state')} and percentile {current_pct_text}."
            )
        new_since_last_run.append(
            {
                "text": change_line,
                "metric_id": item.get("metric_id"),
                "metric_name": metric_name,
                "observed_at": observed_at,
                "retrieved_at": metric_row.get("retrieved_at"),
                "lag_days": metric_row.get("lag_days"),
                "lag_cause": metric_row.get("lag_cause"),
                "new_observation_since_prior_run": new_observation,
                "change_type": change_type,
                "blocked": blocked,
                "block_reason": "Missing citations for new-since-last-run item." if blocked else None,
                "citations": citations,
            }
        )

    return {
        **lifecycle_counts,
        "top_change_drivers": top_change_drivers,
        "new_since_last_run": new_since_last_run,
        "new_since_yesterday": new_since_last_run,
        "regime_transitions": {
            "short": short_regime.get("transition", {}),
            "long": long_regime.get("transition", {}),
        },
        "no_material_threshold_crossings": no_material,
        "summary_text": daily_text,
    }


def build_and_persist_delta_state(
    conn: sqlite3.Connection,
    *,
    macro_result: dict[str, Any],
    sleeve_weights: dict[str, float],
    holdings_available: bool = True,
    asof_ts: datetime | None = None,
) -> dict[str, Any]:
    ensure_delta_tables(conn)
    asof_ts = asof_ts or datetime.now(UTC)
    graph_metadata = list(dict(macro_result.get("signals_summary", {})).get("graph_metadata", []))
    metrics = compute_metric_deltas(conn, graph_metadata=graph_metadata, asof_ts=asof_ts)

    long_metric_rows: list[dict[str, Any]] = []
    for row in graph_metadata:
        metric_id = str(row.get("series_code") or "").strip()
        if not metric_id:
            continue
        long_horizon = dict(row.get("long_horizon") or {})
        long_pct = _to_float(long_horizon.get("percentile_10y"))
        if long_pct is None:
            long_pct = _to_float(long_horizon.get("percentile_5y"))
        metric_row = next((item for item in metrics if item["metric_id"] == metric_id), None)
        if metric_row is None:
            continue
        long_metric_rows.append(
            {
                **metric_row,
                "long_percentile": long_pct,
            }
        )

    short_regime = _build_regime_snapshot(
        conn,
        asof_ts=asof_ts,
        horizon="short",
        metric_rows=metrics,
        score_fn="short",
    )
    long_regime = _build_regime_snapshot(
        conn,
        asof_ts=asof_ts,
        horizon="long",
        metric_rows=long_metric_rows,
        score_fn="long",
    )
    conn.commit()

    alerts_v2 = _persist_alerts_v2(
        conn,
        asof_ts=asof_ts,
        metrics=metrics,
        sleeve_weights=sleeve_weights,
        holdings_available=holdings_available,
    )
    summary = _summary_from_alerts_and_metrics(
        alerts=alerts_v2,
        metrics=metrics,
        long_regime=long_regime,
        short_regime=short_regime,
    )
    return {
        "asof_ts": asof_ts.isoformat(),
        "metrics_count": len(metrics),
        "regime_short": short_regime,
        "regime_long": long_regime,
        "daily_state_change_summary": summary,
        "alerts_v2": alerts_v2,
    }


def load_latest_delta_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    ensure_delta_tables(conn)
    row = conn.execute(
        """
        SELECT asof_ts
        FROM metric_snapshots
        ORDER BY asof_ts DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return {
            "asof_ts": None,
            "daily_state_change_summary": {
                "new_alerts_count": 0,
                "escalated_count": 0,
                "de_escalated_count": 0,
                "persisting_count": 0,
                "top_change_drivers": [],
                "new_since_last_run": [],
                "new_since_yesterday": [],
                "regime_transitions": {"short": {}, "long": {}},
                "no_material_threshold_crossings": True,
                "summary_text": "No material threshold crossings observed. No persisted delta snapshots are available yet.",
                "blocked": True,
                "block_reason": "No persisted metric snapshots.",
            },
            "alerts_v2": [],
            "metrics": [],
        }
    asof_ts = str(row["asof_ts"])
    alert_rows = conn.execute(
        """
        SELECT alert_id, asof_ts, horizon, metric_id, prev_state, curr_state, lifecycle, severity,
               threshold_name, threshold_value, current_value, delta_value, delta_window, percentile_60,
               days_in_state, impact_score, impact_map_json, narrative_md, citations_json
        FROM alert_events_v2
        WHERE asof_ts = ?
        ORDER BY severity DESC, impact_score DESC, CASE lifecycle WHEN 'new' THEN 4 WHEN 'escalated' THEN 3 WHEN 'persisting' THEN 2 ELSE 1 END DESC
        """,
        (asof_ts,),
    ).fetchall()
    alerts_v2: list[dict[str, Any]] = []
    for item in alert_rows:
        citations = list(json.loads(str(item["citations_json"])))
        delta_window = str(item["delta_window"])
        if delta_window == "1d":
            delta_window = "run"
        alerts_v2.append(
            {
                "alert_id": str(item["alert_id"]),
                "asof_ts": str(item["asof_ts"]),
                "horizon": str(item["horizon"]),
                "metric_id": str(item["metric_id"]),
                "metric_name": friendly_metric_name(str(item["metric_id"])),
                "prev_state": str(item["prev_state"]),
                "curr_state": str(item["curr_state"]),
                "lifecycle": str(item["lifecycle"]),
                "severity": int(item["severity"]),
                "threshold_name": str(item["threshold_name"]),
                "threshold_value": float(item["threshold_value"]),
                "current_value": float(item["current_value"]),
                "delta_value": float(item["delta_value"]),
                "delta_window": delta_window,
                "percentile_60": _to_float(item["percentile_60"]),
                "percentile_shift": 0.0,
                "days_in_state": int(item["days_in_state"]),
                "impact_score": float(item["impact_score"]),
                "impact_map": dict(json.loads(str(item["impact_map_json"]))),
                "narrative_md": str(item["narrative_md"]),
                "citations": citations,
                "blocked": len(citations) == 0,
                "block_reason": "Missing citations for alert row." if len(citations) == 0 else None,
                "threshold_transparency": (
                    f"threshold {float(item['threshold_value']):.1f} pct, current {float(item['percentile_60'] or 0.0):.1f} pct, "
                    f"distance to exit state {(float(item['threshold_value']) - float(item['percentile_60'] or 0.0)):+.1f} pct pts "
                    "(positive means additional decline needed; negative means currently above threshold by this amount)"
                ),
            }
        )

    regime_rows = conn.execute(
        """
        SELECT horizon, state, days_in_state, confidence, contributors_json, citations_json
        FROM regime_snapshots
        WHERE asof_ts = ?
        """,
        (asof_ts,),
    ).fetchall()
    regimes: dict[str, dict[str, Any]] = {"short": {}, "long": {}}
    for item in regime_rows:
        horizon = str(item["horizon"])
        payload = dict(json.loads(str(item["contributors_json"])))
        citations = list(json.loads(str(item["citations_json"])))
        regimes[horizon] = {
            "state": str(item["state"]),
            "confidence": float(item["confidence"]),
            "days_in_state": int(item["days_in_state"] or 1) if "days_in_state" in item.keys() else 1,
            "contributors_top": payload.get("contributors_top", []),
            "transition": payload.get("transition", {}),
            "citations": citations,
            "blocked": len(citations) == 0,
            "block_reason": "Missing citations for regime transition." if len(citations) == 0 else None,
        }

    metric_rows = conn.execute(
        """
        SELECT metric_id, metric_name, value, delta_1d, percentile_60, prev_percentile_60, percentile_shift,
               stddev_60, window_5_change, window_20_change, state_short, days_in_state_short,
               observed_at, retrieved_at, lag_days, lag_class, lag_cause, citations_json
        FROM metric_snapshots
        WHERE asof_ts = ?
        """,
        (asof_ts,),
    ).fetchall()
    asof_rows = conn.execute(
        """
        SELECT DISTINCT asof_ts
        FROM metric_snapshots
        ORDER BY asof_ts DESC
        LIMIT 2
        """
    ).fetchall()
    prior_asof = str(asof_rows[1]["asof_ts"]) if len(asof_rows) >= 2 else None
    prior_observed_map: dict[str, str] = {}
    if prior_asof:
        prior_rows = conn.execute(
            """
            SELECT metric_id, observed_at
            FROM metric_snapshots
            WHERE asof_ts = ?
            """,
            (prior_asof,),
        ).fetchall()
        for row in prior_rows:
            metric_id = str(row["metric_id"] or "")
            observed = str(row["observed_at"] or "").strip()
            if metric_id and observed:
                prior_observed_map[metric_id] = observed
    metrics = []
    for item in metric_rows:
        prev_percentile = _to_float(item["prev_percentile_60"])
        curr_percentile = _to_float(item["percentile_60"])
        threshold_crossed = False
        if prev_percentile is not None and curr_percentile is not None:
            for threshold in (60.0, 80.0, 90.0):
                crossed_up = prev_percentile < threshold <= curr_percentile
                crossed_down = prev_percentile > threshold >= curr_percentile
                if crossed_up or crossed_down:
                    threshold_crossed = True
                    break
        metric_id = str(item["metric_id"])
        prior_observed_at = prior_observed_map.get(metric_id)
        new_observation_since_prior_run, change_type, observation_status_text = _observation_change_details(
            observed_at=str(item["observed_at"] or ""),
            prior_observed_at=prior_observed_at,
            lag_cause=str(item["lag_cause"] or "") or None,
        )
        metrics.append(
            {
                "metric_id": metric_id,
                "metric_name": str(item["metric_name"] or friendly_metric_name(metric_id)),
                "value": float(item["value"]),
                "percentile_60": curr_percentile,
                "stddev_60": float(item["stddev_60"] or 0.0),
                "delta_1d": float(item["delta_1d"] or 0.0),
                "observed_at": str(item["observed_at"] or ""),
                "retrieved_at": str(item["retrieved_at"] or ""),
                "lag_days": int(item["lag_days"]) if item["lag_days"] is not None else None,
                "lag_class": str(item["lag_class"] or "") or None,
                "lag_cause": str(item["lag_cause"] or "") or None,
                "prior_observed_at": prior_observed_at,
                "new_observation_since_prior_run": new_observation_since_prior_run,
                "change_type": change_type,
                "observation_status_text": observation_status_text,
                "citations": list(json.loads(str(item["citations_json"]))),
                "prev_percentile_60": prev_percentile,
                "percentile_shift": _bound_pct_shift(float(item["percentile_shift"] or 0.0)),
                "prev_state": "stable",
                "curr_state": str(item["state_short"] or compute_state({"percentile_60": curr_percentile}, "short", "stable")),
                "days_in_state_short": int(item["days_in_state_short"] or 1),
                "threshold_crossed": threshold_crossed,
            }
        )
    metric_shift_map = {str(item["metric_id"]): float(item.get("percentile_shift", 0.0) or 0.0) for item in metrics}
    for alert in alerts_v2:
        alert["percentile_shift"] = _bound_pct_shift(metric_shift_map.get(str(alert.get("metric_id")), 0.0))

    summary = _summary_from_alerts_and_metrics(
        alerts=alerts_v2,
        metrics=metrics,
        long_regime=regimes.get("long", {}),
        short_regime=regimes.get("short", {}),
    )
    summary["blocked"] = False
    summary["block_reason"] = None

    # Fetch previous run MCP summary for coverage delta
    previous_run_summary = None
    if prior_asof:
        # Find MCP run closest to prior snapshot timestamp
        mcp_row = conn.execute(
            """
            SELECT run_id, finished_at
            FROM mcp_connectivity_runs
            WHERE finished_at <= ?
            ORDER BY finished_at DESC
            LIMIT 1
            """,
            (prior_asof,),
        ).fetchone()

        if mcp_row:
            prior_run_id = str(mcp_row["run_id"])
            # Get MCP stats for that run
            mcp_stats = conn.execute(
                """
                SELECT
                    COUNT(*) as total_servers,
                    SUM(CASE WHEN connectable = 1 THEN 1 ELSE 0 END) as connectable_count,
                    SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) as success_count
                FROM mcp_connectivity_runs
                WHERE run_id = ?
                """,
                (prior_run_id,),
            ).fetchone()

            if mcp_stats:
                connectable = int(mcp_stats["connectable_count"] or 0)
                succeeded = int(mcp_stats["success_count"] or 0)
                coverage_ratio = float(succeeded / connectable) if connectable > 0 else 0.0

                previous_run_summary = {
                    "run_id": prior_run_id,
                    "mcp_live_coverage_ratio": coverage_ratio,
                    "mcp_servers_succeeded_count": succeeded,
                    "timestamp": str(mcp_row["finished_at"]),
                }

    summary["previous_run_summary"] = previous_run_summary

    return {
        "asof_ts": asof_ts,
        "daily_state_change_summary": summary,
        "alerts_v2": alerts_v2,
        "metrics": metrics,
        "regimes": regimes,
    }
