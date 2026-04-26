from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.services.data_lag import (
    build_data_trust_badge,
    classify_lag_cause,
    compute_lag_days,
    normalize_series_key,
)
from app.services.delta_engine import friendly_metric_name


METRIC_TO_SLEEVES: dict[str, tuple[str, ...]] = {
    "DGS10": ("ig_bond", "cash"),
    "T10YIE": ("ig_bond", "real_asset"),
    "SP500": ("global_equity", "alt"),
    "VIXCLS": ("convex", "global_equity"),
    "BAMLH0A0HYM2": ("ig_bond", "alt", "global_equity"),
}


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    return {str(row[1]) for row in rows}


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM sqlite_master
        WHERE type='table' AND name=?
        LIMIT 1
        """,
        (table_name,),
    ).fetchone()
    return row is not None


def _latest_series_rows(conn: sqlite3.Connection, metric_key: str, limit: int = 2) -> list[sqlite3.Row]:
    if not _table_exists(conn, "series_observations"):
        return []
    cols = _table_columns(conn, "series_observations")
    lag_days_col = "lag_days" if "lag_days" in cols else "NULL AS lag_days"
    lag_class_col = "lag_class" if "lag_class" in cols else "NULL AS lag_class"
    lag_cause_col = "lag_cause" if "lag_cause" in cols else "NULL AS lag_cause"
    retrieval_col = "retrieval_succeeded" if "retrieval_succeeded" in cols else "NULL AS retrieval_succeeded"
    return conn.execute(
        f"""
        SELECT metric_key, source_id, observation_date, retrieved_at,
               {lag_days_col}, {lag_class_col}, {lag_cause_col}, {retrieval_col}
        FROM series_observations
        WHERE UPPER(metric_key) = ?
        ORDER BY retrieved_at DESC
        LIMIT ?
        """,
        (metric_key.upper(), int(limit)),
    ).fetchall()


def _latest_metric_rows(conn: sqlite3.Connection, metric_key: str, limit: int = 2) -> list[sqlite3.Row]:
    if not _table_exists(conn, "metric_snapshots"):
        return []
    cols = _table_columns(conn, "metric_snapshots")
    lag_days_col = "lag_days" if "lag_days" in cols else "NULL AS lag_days"
    lag_class_col = "lag_class" if "lag_class" in cols else "NULL AS lag_class"
    lag_cause_col = "lag_cause" if "lag_cause" in cols else "NULL AS lag_cause"
    observed_col = "observed_at" if "observed_at" in cols else "NULL AS observed_at"
    retrieved_col = "retrieved_at" if "retrieved_at" in cols else "NULL AS retrieved_at"
    metric_name_col = "metric_name" if "metric_name" in cols else "NULL AS metric_name"
    citations_col = "citations_json" if "citations_json" in cols else "'[]' AS citations_json"
    return conn.execute(
        f"""
        SELECT metric_key, metric_id, {metric_name_col}, {observed_col}, {retrieved_col}, asof_ts,
               {lag_days_col}, {lag_class_col}, {lag_cause_col}, {citations_col}
        FROM metric_snapshots
        WHERE UPPER(COALESCE(metric_key, metric_id)) = ?
        ORDER BY COALESCE(retrieved_at, asof_ts) DESC
        LIMIT ?
        """,
        (metric_key.upper(), int(limit)),
    ).fetchall()


def _citation_cached(citations_json: str) -> bool:
    try:
        payload = json.loads(citations_json or "[]")
    except json.JSONDecodeError:
        return False
    if not isinstance(payload, list):
        return False
    for item in payload:
        if not isinstance(item, dict):
            continue
        importance = str(item.get("importance") or "").lower()
        if "retrieval=cached" in importance:
            return True
    return False


def _lag_row_from_series(
    rows: list[sqlite3.Row],
    metric_key: str,
    cache_fallback_used: bool,
) -> dict[str, Any] | None:
    if not rows:
        return None
    latest = rows[0]
    previous_observed = str(rows[1]["observation_date"] or "") if len(rows) > 1 else None
    observed_at = str(latest["observation_date"] or "")
    retrieved_at = str(latest["retrieved_at"] or "")
    lag_days = latest["lag_days"]
    lag_class = str(latest["lag_class"] or "")
    if lag_days is None or lag_class not in {"fresh", "lagged", "stale"}:
        lag_days, lag_class = compute_lag_days(observed_at, retrieved_at)
    retrieval_succeeded = bool(int(latest["retrieval_succeeded"] or 0)) if latest["retrieval_succeeded"] is not None else not cache_fallback_used
    lag_cause = str(latest["lag_cause"] or "")
    if lag_cause not in {"expected_publication_lag", "unexpected_ingestion_lag", "unknown"}:
        lag_cause = classify_lag_cause(
            series_key=metric_key,
            observed_at=observed_at,
            retrieved_at=retrieved_at,
            lag_days=lag_days,
            retrieval_succeeded=retrieval_succeeded,
            cache_fallback_used=cache_fallback_used,
            latest_available_matches_observed=True,
            previous_observed_at=previous_observed,
        )

    return {
        "metric_key": metric_key,
        "metric_name": friendly_metric_name(metric_key),
        "source_id": str(latest["source_id"] or f"series_{metric_key.lower()}"),
        "observed_at": observed_at or None,
        "retrieved_at": retrieved_at or None,
        "lag_days": lag_days,
        "lag_class": lag_class or None,
        "lag_cause": lag_cause or "unknown",
    }


def _lag_row_from_snapshot(
    rows: list[sqlite3.Row],
    metric_key: str,
    cache_fallback_used: bool,
) -> dict[str, Any] | None:
    if not rows:
        return None
    latest = rows[0]
    previous_observed = str(rows[1]["observed_at"] or "") if len(rows) > 1 else None
    observed_at = str(latest["observed_at"] or "")
    retrieved_at = str(latest["retrieved_at"] or latest["asof_ts"] or "")
    lag_days = latest["lag_days"]
    lag_class = str(latest["lag_class"] or "")
    if lag_days is None or lag_class not in {"fresh", "lagged", "stale"}:
        lag_days, lag_class = compute_lag_days(observed_at, retrieved_at)
    retrieval_succeeded = (not _citation_cached(str(latest["citations_json"] or "[]"))) and (not cache_fallback_used)
    lag_cause = str(latest["lag_cause"] or "")
    if lag_cause not in {"expected_publication_lag", "unexpected_ingestion_lag", "unknown"}:
        lag_cause = classify_lag_cause(
            series_key=metric_key,
            observed_at=observed_at,
            retrieved_at=retrieved_at,
            lag_days=lag_days,
            retrieval_succeeded=retrieval_succeeded,
            cache_fallback_used=cache_fallback_used,
            latest_available_matches_observed=True,
            previous_observed_at=previous_observed,
        )
    return {
        "metric_key": metric_key,
        "metric_name": str(latest["metric_name"] or friendly_metric_name(metric_key)),
        "source_id": f"metric_{metric_key.lower()}",
        "observed_at": observed_at or None,
        "retrieved_at": retrieved_at or None,
        "lag_days": lag_days,
        "lag_class": lag_class or None,
        "lag_cause": lag_cause or "unknown",
    }


def load_metric_lag_details(
    conn: sqlite3.Connection,
    *,
    metric_keys: list[str],
    cache_fallback_used: bool,
) -> list[dict[str, Any]]:
    details: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw_key in metric_keys:
        metric_key = normalize_series_key(raw_key)
        if not metric_key or metric_key in seen:
            continue
        seen.add(metric_key)

        row = _lag_row_from_series(
            _latest_series_rows(conn, metric_key, limit=2),
            metric_key=metric_key,
            cache_fallback_used=cache_fallback_used,
        )
        if row is None:
            row = _lag_row_from_snapshot(
                _latest_metric_rows(conn, metric_key, limit=2),
                metric_key=metric_key,
                cache_fallback_used=cache_fallback_used,
            )
        if row is None:
            row = {
                "metric_key": metric_key,
                "metric_name": friendly_metric_name(metric_key),
                "source_id": f"metric_{metric_key.lower()}",
                "observed_at": None,
                "retrieved_at": None,
                "lag_days": None,
                "lag_class": None,
                "lag_cause": "unknown",
            }
        details.append(row)
    return details


def _threshold_plan(curr_state: str, current_percentile: float) -> tuple[str, float]:
    state = str(curr_state or "stable")
    if state == "elevated":
        return "stress_emerging", 80.0
    if state == "stress_emerging":
        return "stress_regime", 90.0
    if state == "stress_regime":
        return "normalizing", 90.0
    if current_percentile >= 60.0:
        return "stress_emerging", 80.0
    return "elevated", 60.0


def _distance_direction(previous: float | None, current: float, threshold: float) -> str:
    current_distance = abs(current - threshold)
    if previous is None:
        return "unknown"
    previous_distance = abs(float(previous) - threshold)
    if current_distance < previous_distance:
        return "toward threshold"
    if current_distance > previous_distance:
        return "away"
    return "flat"


def _metric_sleeves(metric_key: str) -> tuple[str, ...]:
    return METRIC_TO_SLEEVES.get(normalize_series_key(metric_key), ("global_equity",))


def _threshold_crossed(prev_pct: float | None, curr_pct: float | None) -> bool:
    if prev_pct is None or curr_pct is None:
        return False
    for threshold in (60.0, 80.0, 90.0):
        if (prev_pct < threshold <= curr_pct) or (prev_pct > threshold >= curr_pct):
            return True
    return False


def _parse_observed_date(value: Any) -> datetime.date | None:
    text = str(value or "").strip()
    if not text:
        return None
    candidate = text[:10]
    try:
        return datetime.strptime(candidate, "%Y-%m-%d").date()
    except ValueError:
        return None


def _observation_change_type(
    *,
    observed_at: str | None,
    prior_observed_at: str | None,
    cache_fallback_used: bool,
) -> tuple[bool, str]:
    observed = _parse_observed_date(observed_at)
    prior_observed = _parse_observed_date(prior_observed_at)
    if observed is not None and (prior_observed is None or observed > prior_observed):
        return True, "new_observation"
    if cache_fallback_used:
        return False, "cached_fallback"
    return False, "no_new_observation"


def _observation_status_text(observed_at: str | None, *, new_observation: bool) -> str:
    observed = _parse_observed_date(observed_at)
    if new_observation:
        if observed is None:
            return "New observation detected in current run."
        return f"New observation at observed_at {observed.isoformat()}."
    if observed is None:
        return "No new observation; observed_at unavailable."
    return f"No new observation since observed_at {observed.isoformat()}."


def _load_prior_observed_by_metric(
    conn: sqlite3.Connection,
    *,
    metric_keys: list[str],
) -> dict[str, str]:
    if not metric_keys or not _table_exists(conn, "metric_snapshots"):
        return {}
    rows = conn.execute(
        """
        SELECT DISTINCT asof_ts
        FROM metric_snapshots
        ORDER BY asof_ts DESC
        LIMIT 2
        """
    ).fetchall()
    if len(rows) < 2:
        return {}
    prior_asof = str(rows[1]["asof_ts"] or "")
    unique_keys = sorted({normalize_series_key(key) for key in metric_keys if normalize_series_key(key)})
    if not unique_keys:
        return {}
    placeholders = ", ".join(["?"] * len(unique_keys))
    params: list[Any] = [prior_asof, *unique_keys]
    prior_rows = conn.execute(
        f"""
        SELECT UPPER(metric_id) AS metric_key, observed_at
        FROM metric_snapshots
        WHERE asof_ts = ?
          AND UPPER(metric_id) IN ({placeholders})
        """,
        params,
    ).fetchall()
    out: dict[str, str] = {}
    for row in prior_rows:
        key = normalize_series_key(str(row["metric_key"] or ""))
        observed_at = str(row["observed_at"] or "").strip()
        if key and observed_at:
            out[key] = observed_at
    return out


def _build_data_recency_badge(lag_rows: list[dict[str, Any]]) -> dict[str, Any]:
    if not lag_rows:
        return {
            "level": "unknown",
            "label": "Data recency unknown",
            "summary": "No lag diagnostics were available for this run.",
            "unexpected_ingestion_lag_count": 0,
            "expected_publication_lag_count": 0,
            "unknown_cause_count": 0,
        }
    unexpected_count = sum(
        1 for row in lag_rows if str(row.get("lag_cause") or "").lower() == "unexpected_ingestion_lag"
    )
    expected_count = sum(
        1 for row in lag_rows if str(row.get("lag_cause") or "").lower() == "expected_publication_lag"
    )
    unknown_count = sum(
        1 for row in lag_rows if str(row.get("lag_cause") or "").lower() == "unknown"
    )
    if unexpected_count > 0:
        return {
            "level": "unexpected_ingestion_lag",
            "label": "Data recency lagged",
            "summary": f"{unexpected_count} metric(s) show unexpected ingestion lag.",
            "unexpected_ingestion_lag_count": unexpected_count,
            "expected_publication_lag_count": expected_count,
            "unknown_cause_count": unknown_count,
        }
    if expected_count > 0:
        return {
            "level": "expected_publication_lag",
            "label": "Data recency lagged (expected)",
            "summary": f"{expected_count} metric(s) show expected publication lag.",
            "unexpected_ingestion_lag_count": unexpected_count,
            "expected_publication_lag_count": expected_count,
            "unknown_cause_count": unknown_count,
        }
    if unknown_count > 0:
        return {
            "level": "unknown",
            "label": "Data recency unknown",
            "summary": f"{unknown_count} metric(s) have unknown lag cause.",
            "unexpected_ingestion_lag_count": unexpected_count,
            "expected_publication_lag_count": expected_count,
            "unknown_cause_count": unknown_count,
        }
    return {
        "level": "fresh",
        "label": "Data recency fresh",
        "summary": "Observed dates advanced normally in this run.",
        "unexpected_ingestion_lag_count": unexpected_count,
        "expected_publication_lag_count": expected_count,
        "unknown_cause_count": unknown_count,
    }


def build_near_threshold_watchlist(
    metrics: list[dict[str, Any]],
    lag_by_metric: dict[str, dict[str, Any]],
    previous_observed_by_metric: dict[str, str],
    *,
    cache_fallback_used: bool,
    top_n: int = 5,
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for metric in metrics:
        metric_key = normalize_series_key(str(metric.get("metric_id") or metric.get("metric_key") or ""))
        current_pct = _to_float(metric.get("percentile_60"))
        if not metric_key or current_pct is None:
            continue
        current_state = str(metric.get("curr_state") or "stable")
        next_state, threshold = _threshold_plan(current_state, current_pct)
        previous_pct = _to_float(metric.get("prev_percentile_60"))
        direction = _distance_direction(previous_pct, current_pct, threshold)
        lag_meta = lag_by_metric.get(metric_key, {})
        observed_at = str(
            lag_meta.get("observed_at")
            or metric.get("observed_at")
            or ""
        ).strip() or None
        prior_observed_at = str(previous_observed_by_metric.get(metric_key) or "").strip() or None
        new_observation, change_type = _observation_change_type(
            observed_at=observed_at,
            prior_observed_at=prior_observed_at,
            cache_fallback_used=cache_fallback_used,
        )
        citations = list(metric.get("citations", []) or [])
        candidates.append(
            {
                "metric_key": metric_key,
                "metric_name": str(metric.get("metric_name") or friendly_metric_name(metric_key)),
                "current_state": current_state,
                "next_state": next_state,
                "distance_to_threshold": round(abs(current_pct - threshold), 2),
                "distance_unit": "pct pts",
                "direction": direction,
                "days_in_state": int(metric.get("days_in_state_short") or 1),
                "observed_at": observed_at,
                "retrieved_at": lag_meta.get("retrieved_at"),
                "lag_days": lag_meta.get("lag_days"),
                "lag_class": lag_meta.get("lag_class"),
                "lag_cause": lag_meta.get("lag_cause", "unknown"),
                "new_observation_since_prior_run": new_observation,
                "change_type": change_type,
                "observation_status_text": _observation_status_text(observed_at, new_observation=new_observation),
                "citations": citations,
            }
        )
    candidates.sort(
        key=lambda item: (
            float(item.get("distance_to_threshold") or 9999.0),
            0 if str(item.get("direction")) == "toward threshold" else 1,
        )
    )
    return candidates[: max(1, int(top_n))]


def build_top_movers_summary(
    metrics: list[dict[str, Any]],
    lag_by_metric: dict[str, dict[str, Any]],
    previous_observed_by_metric: dict[str, str],
    *,
    cache_fallback_used: bool,
    top_n: int = 3,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for metric in metrics:
        metric_key = normalize_series_key(str(metric.get("metric_id") or metric.get("metric_key") or ""))
        if not metric_key:
            continue
        delta = _to_float(metric.get("delta_1d"))
        if delta is None:
            continue
        prev_pct = _to_float(metric.get("prev_percentile_60"))
        curr_pct = _to_float(metric.get("percentile_60"))
        crossed = _threshold_crossed(prev_pct, curr_pct)
        lag_meta = lag_by_metric.get(metric_key, {})
        observed_at = str(
            lag_meta.get("observed_at")
            or metric.get("observed_at")
            or ""
        ).strip() or None
        prior_observed_at = str(previous_observed_by_metric.get(metric_key) or "").strip() or None
        new_observation, change_type = _observation_change_type(
            observed_at=observed_at,
            prior_observed_at=prior_observed_at,
            cache_fallback_used=cache_fallback_used,
        )
        sleeves = list(_metric_sleeves(metric_key))
        interpretation_tag = "threshold_cross" if crossed else ("risk_repricing" if metric_key in {"VIXCLS", "BAMLH0A0HYM2"} else "monitor")
        rows.append(
            {
                "metric_key": metric_key,
                "metric_name": str(metric.get("metric_name") or friendly_metric_name(metric_key)),
                "delta_1d": round(float(delta), 4),
                "delta_window": "run",
                "state_transition": f"{metric.get('prev_state', 'stable')} -> {metric.get('curr_state', 'stable')}",
                "days_in_state": int(metric.get("days_in_state_short") or 1),
                "percentile_60": curr_pct,
                "threshold_crossed": crossed,
                "interpretation_tag": interpretation_tag,
                "sleeves": sleeves,
                "why_it_matters": (
                    f"Most relevant to {', '.join(sleeves)} sleeves under the current regime."
                ),
                "observed_at": observed_at,
                "retrieved_at": lag_meta.get("retrieved_at"),
                "lag_days": lag_meta.get("lag_days"),
                "lag_class": lag_meta.get("lag_class"),
                "lag_cause": lag_meta.get("lag_cause", "unknown"),
                "new_observation_since_prior_run": new_observation,
                "change_type": change_type,
                "observation_status_text": _observation_status_text(observed_at, new_observation=new_observation),
                "citations": list(metric.get("citations", []) or []),
                "_score": abs(float(delta)),
            }
        )
    rows.sort(key=lambda item: float(item.get("_score") or 0.0), reverse=True)
    for row in rows:
        row.pop("_score", None)
    return rows[: max(1, int(top_n))]


def build_why_today_matters(
    *,
    alerts_v2: list[dict[str, Any]],
    watchlist: list[dict[str, Any]],
    top_movers: list[dict[str, Any]],
) -> str:
    lifecycle_counts = {
        "new": sum(1 for item in alerts_v2 if str(item.get("lifecycle")) == "new"),
        "escalated": sum(1 for item in alerts_v2 if str(item.get("lifecycle")) == "escalated"),
        "de_escalated": sum(1 for item in alerts_v2 if str(item.get("lifecycle")) == "de_escalated"),
    }
    risk_like = any(item.get("metric_key") in {"VIXCLS", "BAMLH0A0HYM2"} for item in top_movers)
    near_risk = any(
        float(item.get("distance_to_threshold") or 999.0) <= 5.0 and str(item.get("direction")) == "toward threshold"
        for item in watchlist
    )

    if lifecycle_counts["new"] == 0 and lifecycle_counts["escalated"] == 0:
        if near_risk:
            return "No material regime transitions. Watchlist focuses on metrics closest to thresholds and persistence duration."
        return "Stress indicators are easing, with fewer near-threshold risks than prior run."
    if risk_like or near_risk:
        return "Volatility and credit stress moved closer to risk bands, increasing monitoring priority for drawdown-sensitive sleeves."
    return "No material regime transitions. Watchlist focuses on metrics closest to thresholds and persistence duration."


def build_portfolio_monitoring_mode(
    conn: sqlite3.Connection,
    *,
    holdings: list[Any],
    valuations: list[dict[str, Any]],
    diagnostic: dict[str, Any],
    policy_weights: dict[str, float],
    watchlist: list[dict[str, Any]],
    alerts_v2: list[dict[str, Any]],
) -> dict[str, Any]:
    if not holdings:
        sleeve_scores: dict[str, float] = {}
        for item in watchlist:
            metric_key = normalize_series_key(str(item.get("metric_key") or ""))
            distance = float(item.get("distance_to_threshold") or 999.0)
            weight = 1.0 / max(1.0, distance)
            for sleeve in _metric_sleeves(metric_key):
                sleeve_scores[sleeve] = sleeve_scores.get(sleeve, 0.0) + weight * float(policy_weights.get(sleeve, 0.0))
        top_exposures = sorted(sleeve_scores.items(), key=lambda item: item[1], reverse=True)[:3]
        return {
            "mode": "holdings_missing",
            "headline": "Holdings missing. Portfolio impact is shown as policy-assumed exposure.",
            "cta": {"label": "Add holdings", "path": "/portfolio"},
            "policy_assumed_exposure": [
                {
                    "sleeve": sleeve,
                    "policy_weight": float(policy_weights.get(sleeve, 0.0)),
                    "exposure_score": round(score, 4),
                }
                for sleeve, score in top_exposures
            ],
        }

    incomplete = [
        row
        for row in valuations
        if str(row.get("price_source") or "") == "fallback" or str(row.get("fx_source") or "") == "fallback"
    ]
    if incomplete:
        missing = [str(item.get("symbol") or "") for item in incomplete if str(item.get("symbol") or "")]
        last_success = conn.execute(
            """
            SELECT MAX(as_of) AS last_as_of
            FROM portfolio_price_cache
            WHERE source IN ('yahoo_quote', 'manual')
            """
        ).fetchone()
        return {
            "mode": "pricing_incomplete",
            "headline": "Holdings loaded, pricing incomplete. Some diagnostics are partial.",
            "missing_symbols": sorted(set(missing)),
            "missing_count": len(incomplete),
            "last_successful_pricing_at": str(last_success["last_as_of"]) if last_success and last_success["last_as_of"] else None,
        }

    drift = dict(diagnostic.get("sleeve_drift_weights") or {})
    drift_sgd = dict(diagnostic.get("sleeve_drift_sgd") or {})
    top_drift = sorted(drift.items(), key=lambda item: abs(float(item[1])), reverse=True)[:3]
    concentration = list((diagnostic.get("concentration_metrics") or {}).get("top_positions", []))[:3]
    impact_movers = sorted(
        [item for item in alerts_v2 if not bool(item.get("blocked"))],
        key=lambda item: float(item.get("impact_score") or 0.0),
        reverse=True,
    )[:3]
    stress_rows = list(diagnostic.get("stress_scenarios") or [])
    stress_headline = ""
    if stress_rows:
        worst = sorted(stress_rows, key=lambda item: float(item.get("estimated_impact_pct") or 0.0))[0]
        stress_headline = f"{worst.get('name')}: {float(worst.get('estimated_impact_pct') or 0.0) * 100.0:.1f}% estimated impact."

    return {
        "mode": "full",
        "headline": "Portfolio monitoring is live with holdings and pricing coverage.",
        "total_value": float(diagnostic.get("total_value") or 0.0),
        "top_sleeve_drifts": [
            {
                "sleeve": sleeve,
                "drift_pct": round(float(weight) * 100.0, 2),
                "drift_sgd": float(drift_sgd.get(sleeve, 0.0)),
            }
            for sleeve, weight in top_drift
        ],
        "top_position_concentration": concentration,
        "portfolio_impact_movers": [
            {
                "metric_key": str(item.get("metric_id") or ""),
                "metric_name": str(item.get("metric_name") or friendly_metric_name(str(item.get("metric_id") or ""))),
                "impact_score": float(item.get("impact_score") or 0.0),
                "transmission": str((item.get("impact_map") or {}).get("portfolio_transmission_line") or ""),
            }
            for item in impact_movers
        ],
        "stress_scenario_headline": stress_headline,
    }


def build_portfolio_first_bullets(
    *,
    portfolio_mode: dict[str, Any],
    top_movers: list[dict[str, Any]],
    watchlist: list[dict[str, Any]],
) -> list[str]:
    mode = str(portfolio_mode.get("mode") or "")
    bullets: list[str] = []
    if mode == "full":
        drifts = list(portfolio_mode.get("top_sleeve_drifts") or [])
        if drifts:
            first = drifts[0]
            bullets.append(
                f"Portfolio drift highlight: {first.get('sleeve')} at {float(first.get('drift_pct') or 0.0):+.1f} pct ({float(first.get('drift_sgd') or 0.0):+.0f} SGD)."
            )
        if top_movers:
            mover = top_movers[0]
            bullets.append(
                f"Portfolio sensitivity: {mover.get('metric_name')} moved {float(mover.get('delta_1d') or 0.0):+.2f}, most relevant to {', '.join(mover.get('sleeves') or [])}."
            )
        if watchlist:
            item = watchlist[0]
            bullets.append(
                f"Watchlist closest to importance: {item.get('metric_name')} is {float(item.get('distance_to_threshold') or 0.0):.1f} {item.get('distance_unit', 'pct pts')} from {item.get('next_state')}."
            )
    elif mode == "pricing_incomplete":
        bullets.append("Portfolio drift highlight: holdings loaded but pricing is incomplete.")
        bullets.append(
            f"Portfolio sensitivity: {int(portfolio_mode.get('missing_count') or 0)} positions are on fallback pricing."
        )
        bullets.append("Watchlist items still reflect macro-to-portfolio transmission, but confidence is reduced.")
    else:
        bullets.append("Portfolio drift highlight unavailable until holdings are added.")
        bullets.append("Portfolio sensitivity currently uses policy-assumed sleeve exposures.")
        bullets.append("Watchlist items show potential relevance by policy sleeve, not personal holdings.")
    while len(bullets) < 3:
        bullets.append("No additional portfolio-specific change note.")
    return bullets[:3]


def build_dashboard_monitoring(
    conn: sqlite3.Connection,
    *,
    delta_payload: dict[str, Any],
    today_upload: dict[str, Any],
    holdings: list[Any],
    valuations: list[dict[str, Any]],
    diagnostic: dict[str, Any],
    policy_weights: dict[str, float],
) -> dict[str, Any]:
    alerts_v2 = list(delta_payload.get("alerts_v2") or [])
    metrics = list(delta_payload.get("metrics") or [])
    metric_keys = [normalize_series_key(str(item.get("metric_id") or item.get("metric_key") or "")) for item in metrics]

    lag_rows = load_metric_lag_details(
        conn,
        metric_keys=metric_keys,
        cache_fallback_used=bool(today_upload.get("cached_used")),
    )
    lag_by_metric = {normalize_series_key(str(item.get("metric_key") or "")): item for item in lag_rows}
    previous_observed_by_metric = _load_prior_observed_by_metric(conn, metric_keys=metric_keys)
    trust_badge = build_data_trust_badge(lag_rows)
    watchlist = build_near_threshold_watchlist(
        metrics,
        lag_by_metric,
        previous_observed_by_metric,
        cache_fallback_used=bool(today_upload.get("cached_used")),
        top_n=5,
    )
    top_movers = build_top_movers_summary(
        metrics,
        lag_by_metric,
        previous_observed_by_metric,
        cache_fallback_used=bool(today_upload.get("cached_used")),
        top_n=3,
    )
    data_recency = _build_data_recency_badge(lag_rows)
    why_today = build_why_today_matters(alerts_v2=alerts_v2, watchlist=watchlist, top_movers=top_movers)
    portfolio_mode = build_portfolio_monitoring_mode(
        conn,
        holdings=holdings,
        valuations=valuations,
        diagnostic=diagnostic,
        policy_weights=policy_weights,
        watchlist=watchlist,
        alerts_v2=alerts_v2,
    )
    portfolio_bullets = build_portfolio_first_bullets(
        portfolio_mode=portfolio_mode,
        top_movers=top_movers,
        watchlist=watchlist,
    )
    return {
        "why_today_matters": why_today,
        "near_threshold_watchlist": watchlist,
        "top_movers_summary": top_movers,
        "lag_diagnostics": lag_rows,
        "data_trust_badge": {
            **trust_badge,
            "diagnostics_path": "/api/mcp-diagnostics",
        },
        "data_recency": {
            **data_recency,
            "diagnostics_path": "/api/mcp-diagnostics",
        },
        "portfolio_monitoring": portfolio_mode,
        "portfolio_first_bullets": portfolio_bullets,
    }
