from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.config import get_db_path


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_recommendation_reviews (
          review_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          decision_label TEXT NOT NULL,
          review_outcome TEXT NOT NULL,
          notes TEXT NOT NULL,
          what_changed_view TEXT NOT NULL,
          false_positive INTEGER NOT NULL DEFAULT 0,
          false_negative INTEGER NOT NULL DEFAULT 0,
          stale_evidence_miss INTEGER NOT NULL DEFAULT 0,
          overconfident_forecast_support INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_trigger_outcomes (
          trigger_outcome_id TEXT PRIMARY KEY,
          object_id TEXT NOT NULL,
          trigger_type TEXT NOT NULL,
          threshold_state TEXT NOT NULL,
          outcome_label TEXT NOT NULL,
          notes TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_blocker_outcomes (
          blocker_outcome_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          blocker_code TEXT NOT NULL,
          outcome_label TEXT NOT NULL,
          notes TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_evidence_miss_reviews (
          evidence_review_id TEXT PRIMARY KEY,
          candidate_id TEXT NOT NULL,
          miss_type TEXT NOT NULL,
          notes TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def create_recommendation_review(
    *,
    candidate_id: str,
    decision_label: str,
    review_outcome: str,
    notes: str,
    what_changed_view: str,
    false_positive: bool,
    false_negative: bool,
    stale_evidence_miss: bool,
    overconfident_forecast_support: bool,
) -> dict[str, Any]:
    payload = {
        "review_id": f"recommendation_review_{uuid.uuid4().hex}",
        "candidate_id": candidate_id,
        "decision_label": decision_label,
        "review_outcome": review_outcome,
        "notes": notes,
        "what_changed_view": what_changed_view,
        "false_positive": bool(false_positive),
        "false_negative": bool(false_negative),
        "stale_evidence_miss": bool(stale_evidence_miss),
        "overconfident_forecast_support": bool(overconfident_forecast_support),
        "created_at": _now_iso(),
    }
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO v2_recommendation_reviews (
              review_id, candidate_id, decision_label, review_outcome, notes, what_changed_view,
              false_positive, false_negative, stale_evidence_miss, overconfident_forecast_support, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["review_id"],
                payload["candidate_id"],
                payload["decision_label"],
                payload["review_outcome"],
                payload["notes"],
                payload["what_changed_view"],
                int(payload["false_positive"]),
                int(payload["false_negative"]),
                int(payload["stale_evidence_miss"]),
                int(payload["overconfident_forecast_support"]),
                payload["created_at"],
            ),
        )
        conn.commit()
    return payload


def list_recommendation_reviews(*, candidate_id: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    if candidate_id is not None:
        clauses.append("candidate_id = ?")
        params.append(candidate_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    capped_limit = max(1, min(int(limit or 100), 500))
    with _connection() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM v2_recommendation_reviews
            {where}
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (*params, capped_limit),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            for key in (
                "false_positive",
                "false_negative",
                "stale_evidence_miss",
                "overconfident_forecast_support",
            ):
                item[key] = bool(item.get(key))
        return items


def forecast_feedback_payload(*, provider: str | None = None, limit: int = 60) -> dict[str, Any]:
    from app.v2.forecasting.store import list_evaluations

    rows = list_evaluations(provider=provider, limit=limit)
    by_provider: dict[str, dict[str, Any]] = {}
    for row in rows:
        provider_name = str(row.get("provider") or "unknown")
        bucket = by_provider.setdefault(
            provider_name,
            {
                "provider": provider_name,
                "evaluation_count": 0,
                "metric_totals": {},
                "metric_counts": {},
                "latest_measured_at": None,
                "series_families": set(),
            },
        )
        bucket["evaluation_count"] += 1
        metric_name = str(row.get("metric_name") or "metric")
        metric_value = float(row.get("metric_value") or 0.0)
        bucket["metric_totals"][metric_name] = bucket["metric_totals"].get(metric_name, 0.0) + metric_value
        bucket["metric_counts"][metric_name] = bucket["metric_counts"].get(metric_name, 0) + 1
        bucket["latest_measured_at"] = max(
            str(bucket["latest_measured_at"] or ""),
            str(row.get("measured_at") or ""),
        )
        bucket["series_families"].add(str(row.get("series_family") or "unknown"))

    provider_rows = []
    for bucket in by_provider.values():
        averages = {
            metric_name: round(bucket["metric_totals"][metric_name] / max(bucket["metric_counts"][metric_name], 1), 6)
            for metric_name in bucket["metric_totals"]
        }
        provider_rows.append(
            {
                "provider": bucket["provider"],
                "evaluation_count": bucket["evaluation_count"],
                "average_metrics": averages,
                "latest_measured_at": bucket["latest_measured_at"],
                "series_families": sorted(bucket["series_families"]),
            }
        )
    provider_rows.sort(key=lambda item: (-int(item["evaluation_count"]), str(item["provider"])))
    return {
        "generated_at": _now_iso(),
        "provider_count": len(provider_rows),
        "providers": provider_rows,
        "recent_rows": rows,
    }


def trigger_feedback_payload(limit: int = 80) -> dict[str, Any]:
    from app.v2.forecasting.store import _connection as forecast_connection

    capped_limit = max(1, min(int(limit or 80), 500))
    with forecast_connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM trigger_support_records
            ORDER BY generated_at DESC
            LIMIT ?
            """,
            (capped_limit,),
        ).fetchall()
        items = [dict(row) for row in rows]
    by_state: dict[str, int] = {}
    by_provider: dict[str, int] = {}
    for item in items:
        threshold_state = str(item.get("threshold_state") or "unknown")
        provider_name = str(item.get("provider") or "unknown")
        by_state[threshold_state] = by_state.get(threshold_state, 0) + 1
        by_provider[provider_name] = by_provider.get(provider_name, 0) + 1
    return {
        "generated_at": _now_iso(),
        "trigger_count": len(items),
        "threshold_state_counts": by_state,
        "provider_counts": by_provider,
        "recent_rows": items,
    }


def recommendation_feedback_payload(*, candidate_id: str | None = None, limit: int = 100) -> dict[str, Any]:
    reviews = list_recommendation_reviews(candidate_id=candidate_id, limit=limit)
    summary = {
        "review_count": len(reviews),
        "false_positive_count": sum(1 for row in reviews if row["false_positive"]),
        "false_negative_count": sum(1 for row in reviews if row["false_negative"]),
        "stale_evidence_miss_count": sum(1 for row in reviews if row["stale_evidence_miss"]),
        "overconfident_forecast_support_count": sum(1 for row in reviews if row["overconfident_forecast_support"]),
    }
    return {
        "generated_at": _now_iso(),
        "summary": summary,
        "reviews": reviews,
    }
