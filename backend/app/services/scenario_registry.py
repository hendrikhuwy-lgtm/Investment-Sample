from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.services.stress_engine import SCENARIO_LABELS, SCENARIO_PROBABILITY_WEIGHTS, SCENARIO_SLEEVE_SHOCKS


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_scenario_registry_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_registry (
          scenario_id TEXT PRIMARY KEY,
          scenario_name TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          source_rationale TEXT,
          policy_notes TEXT,
          created_at TEXT NOT NULL,
          approved_at TEXT,
          retired_at TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_versions (
          scenario_version_id TEXT PRIMARY KEY,
          scenario_id TEXT NOT NULL,
          version_label TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 1,
          probability_weight REAL,
          confidence_rating TEXT NOT NULL DEFAULT 'medium',
          review_cadence_days INTEGER,
          last_reviewed_at TEXT,
          reviewed_by TEXT,
          shocks_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_review_events (
          scenario_review_event_id TEXT PRIMARY KEY,
          scenario_id TEXT NOT NULL,
          scenario_version_id TEXT,
          actor TEXT NOT NULL,
          event_type TEXT NOT NULL,
          note TEXT,
          occurred_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_comparison_snapshots (
          comparison_id TEXT PRIMARY KEY,
          scenario_id TEXT NOT NULL,
          scenario_version_id TEXT,
          current_run_id TEXT,
          prior_run_id TEXT,
          current_impact_pct REAL,
          prior_impact_pct REAL,
          impact_delta_pct REAL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.commit()


def seed_default_scenarios(conn: sqlite3.Connection) -> None:
    ensure_scenario_registry_tables(conn)
    now = _now_iso()
    for scenario_id, shocks in SCENARIO_SLEEVE_SHOCKS.items():
        registry_row = conn.execute(
            "SELECT scenario_id FROM scenario_registry WHERE scenario_id = ? LIMIT 1",
            (scenario_id,),
        ).fetchone()
        if registry_row is None:
            conn.execute(
                """
                INSERT INTO scenario_registry (
                  scenario_id, scenario_name, status, source_rationale, policy_notes, created_at, approved_at, retired_at
                ) VALUES (?, ?, 'active', ?, ?, ?, ?, NULL)
                """,
                (
                    scenario_id,
                    SCENARIO_LABELS.get(scenario_id, scenario_id.replace("_", " ")),
                    "Seeded from current stress analog library.",
                    "Use for resilience diagnostics, not directional forecasting.",
                    now,
                    now,
                ),
            )
        version_row = conn.execute(
            """
            SELECT scenario_version_id
            FROM scenario_versions
            WHERE scenario_id = ? AND version_label = '1.0'
            LIMIT 1
            """,
            (scenario_id,),
        ).fetchone()
        if version_row is None:
            conn.execute(
                """
                INSERT INTO scenario_versions (
                  scenario_version_id, scenario_id, version_label, is_active, probability_weight,
                  confidence_rating, review_cadence_days, last_reviewed_at, reviewed_by, shocks_json,
                  created_at, updated_at
                ) VALUES (?, ?, '1.0', 1, ?, 'medium', 30, ?, 'system_seed', ?, ?, ?)
                """,
                (
                    f"scenario_version_{uuid.uuid4().hex[:12]}",
                    scenario_id,
                    float(SCENARIO_PROBABILITY_WEIGHTS.get(scenario_id, 0.0)),
                    now,
                    json.dumps(shocks),
                    now,
                    now,
                ),
            )
    conn.commit()


def _active_version_row(conn: sqlite3.Connection, scenario_id: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT *
        FROM scenario_versions
        WHERE scenario_id = ? AND is_active = 1
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (scenario_id,),
    ).fetchone()


def list_scenarios(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_scenario_registry_tables(conn)
    seed_default_scenarios(conn)
    rows = conn.execute(
        """
        SELECT scenario_id, scenario_name, status, source_rationale, policy_notes, created_at, approved_at, retired_at
        FROM scenario_registry
        ORDER BY scenario_name ASC
        """
    ).fetchall()
    items: list[dict[str, Any]] = []
    now = datetime.now(UTC)
    for row in rows:
        item = dict(row)
        version = _active_version_row(conn, str(row["scenario_id"]))
        item["active_version"] = dict(version) if version is not None else None
        if item["active_version"] is not None:
            item["active_version"]["shocks_json"] = json.loads(str(item["active_version"].get("shocks_json") or "{}"))
            last_reviewed_at = item["active_version"].get("last_reviewed_at")
            cadence = int(item["active_version"].get("review_cadence_days") or 0)
            days_since_review = None
            review_due = False
            if last_reviewed_at:
                try:
                    reviewed_dt = datetime.fromisoformat(str(last_reviewed_at).replace("Z", "+00:00"))
                    days_since_review = max(0, (now - reviewed_dt).days)
                    review_due = cadence > 0 and days_since_review >= cadence
                except ValueError:
                    days_since_review = None
            item["active_version"]["days_since_review"] = days_since_review
            item["active_version"]["review_due"] = review_due
            item["active_version"]["review_due_level"] = (
                "overdue" if review_due else "due_soon" if days_since_review is not None and cadence > 0 and days_since_review >= max(cadence - 7, 0) else "healthy"
            )
        items.append(item)
    return items


def get_active_scenario_definitions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    scenarios = list_scenarios(conn)
    definitions: list[dict[str, Any]] = []
    for item in scenarios:
        version = item.get("active_version") or {}
        # Draft scenarios should still participate in governed comparison views so
        # PMs can review proposed shocks before formal approval. Only retired
        # scenarios are suppressed from active comparison output.
        if str(item.get("status") or "active") == "retired":
            continue
        definitions.append(
            {
                "scenario_id": str(item.get("scenario_id")),
                "name": str(item.get("scenario_name")),
                "scenario_probability_weight": float(version.get("probability_weight") or 0.0),
                "scenario_version": str(version.get("version_label") or "1.0"),
                "confidence_rating": str(version.get("confidence_rating") or "medium"),
                "review_cadence_days": int(version.get("review_cadence_days") or 0),
                "last_reviewed_at": version.get("last_reviewed_at"),
                "reviewed_by": version.get("reviewed_by"),
                "shocks": dict(version.get("shocks_json") or {}),
            }
        )
    return definitions


def _next_version_label(previous: str | None) -> str:
    if not previous:
        return "1.0"
    try:
        major, minor = previous.split(".", 1)
        return f"{major}.{int(minor) + 1}"
    except Exception:  # noqa: BLE001
        return f"{previous}.1"


def create_scenario(
    conn: sqlite3.Connection,
    *,
    scenario_name: str,
    source_rationale: str,
    policy_notes: str | None,
    shocks: dict[str, float],
    probability_weight: float | None = None,
    confidence_rating: str = "medium",
    reviewed_by: str | None = None,
) -> dict[str, Any]:
    ensure_scenario_registry_tables(conn)
    scenario_id = f"scenario_{uuid.uuid4().hex[:12]}"
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO scenario_registry (
          scenario_id, scenario_name, status, source_rationale, policy_notes, created_at, approved_at, retired_at
        ) VALUES (?, ?, 'draft', ?, ?, ?, NULL, NULL)
        """,
        (scenario_id, scenario_name, source_rationale, policy_notes, now),
    )
    conn.execute(
        """
        INSERT INTO scenario_versions (
          scenario_version_id, scenario_id, version_label, is_active, probability_weight, confidence_rating,
          review_cadence_days, last_reviewed_at, reviewed_by, shocks_json, created_at, updated_at
        ) VALUES (?, ?, '1.0', 1, ?, ?, 30, ?, ?, ?, ?, ?)
        """,
        (
            f"scenario_version_{uuid.uuid4().hex[:12]}",
            scenario_id,
            probability_weight,
            confidence_rating,
            now,
            reviewed_by,
            json.dumps(shocks),
            now,
            now,
        ),
    )
    conn.execute(
        """
        INSERT INTO scenario_review_events (
          scenario_review_event_id, scenario_id, scenario_version_id, actor, event_type, note, occurred_at
        ) VALUES (?, ?, NULL, ?, 'created', ?, ?)
        """,
        (f"scenario_review_{uuid.uuid4().hex[:12]}", scenario_id, reviewed_by or "system", source_rationale, now),
    )
    conn.commit()
    return next(item for item in list_scenarios(conn) if str(item.get("scenario_id")) == scenario_id)


def update_scenario(
    conn: sqlite3.Connection,
    *,
    scenario_id: str,
    patch: dict[str, Any],
    actor: str,
) -> dict[str, Any]:
    ensure_scenario_registry_tables(conn)
    row = conn.execute("SELECT * FROM scenario_registry WHERE scenario_id = ? LIMIT 1", (scenario_id,)).fetchone()
    if row is None:
        raise ValueError("Scenario not found.")
    now = _now_iso()
    allowed_registry_fields = {"scenario_name", "status", "source_rationale", "policy_notes", "approved_at", "retired_at"}
    assignments: list[str] = []
    values: list[Any] = []
    for key, value in patch.items():
        if key in allowed_registry_fields:
            assignments.append(f"{key} = ?")
            values.append(value)
    if assignments:
        values.append(scenario_id)
        conn.execute(f"UPDATE scenario_registry SET {', '.join(assignments)} WHERE scenario_id = ?", tuple(values))

    version_patch = {key: value for key, value in patch.items() if key in {"shocks", "probability_weight", "confidence_rating", "review_cadence_days", "last_reviewed_at", "reviewed_by"}}
    if version_patch:
        current_version = _active_version_row(conn, scenario_id)
        previous_label = str(current_version["version_label"]) if current_version is not None else None
        next_label = _next_version_label(previous_label)
        if current_version is not None:
            conn.execute("UPDATE scenario_versions SET is_active = 0, updated_at = ? WHERE scenario_version_id = ?", (now, str(current_version["scenario_version_id"])))
        conn.execute(
            """
            INSERT INTO scenario_versions (
              scenario_version_id, scenario_id, version_label, is_active, probability_weight, confidence_rating,
              review_cadence_days, last_reviewed_at, reviewed_by, shocks_json, created_at, updated_at
            ) VALUES (?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"scenario_version_{uuid.uuid4().hex[:12]}",
                scenario_id,
                next_label,
                version_patch.get("probability_weight"),
                str(version_patch.get("confidence_rating") or (current_version["confidence_rating"] if current_version is not None else "medium")),
                int(version_patch.get("review_cadence_days") or (current_version["review_cadence_days"] if current_version is not None and current_version["review_cadence_days"] is not None else 30)),
                version_patch.get("last_reviewed_at") or now,
                version_patch.get("reviewed_by") or actor,
                json.dumps(version_patch.get("shocks") or json.loads(str(current_version["shocks_json"]) if current_version is not None else "{}")),
                now,
                now,
            ),
        )
    conn.execute(
        """
        INSERT INTO scenario_review_events (
          scenario_review_event_id, scenario_id, scenario_version_id, actor, event_type, note, occurred_at
        ) VALUES (?, ?, NULL, ?, 'updated', ?, ?)
        """,
        (f"scenario_review_{uuid.uuid4().hex[:12]}", scenario_id, actor, json.dumps(patch), now),
    )
    conn.commit()
    return next(item for item in list_scenarios(conn) if str(item.get("scenario_id")) == scenario_id)


def _impact(weights: dict[str, float], shocks: dict[str, float]) -> float:
    return round(sum(float(weights.get(sleeve, 0.0)) * float(shock) for sleeve, shock in shocks.items()) * 100.0, 2)


def compare_scenarios(
    conn: sqlite3.Connection,
    *,
    current_weights: dict[str, float],
    prior_weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    ensure_scenario_registry_tables(conn)
    definitions = get_active_scenario_definitions(conn)
    comparisons: list[dict[str, Any]] = []
    version_comparisons: list[dict[str, Any]] = []
    for definition in definitions:
        current_impact = _impact(current_weights, dict(definition.get("shocks") or {}))
        prior_impact = _impact(prior_weights or {}, dict(definition.get("shocks") or {})) if prior_weights else None
        if prior_impact is not None:
            comparisons.append(
                {
                    "scenario_id": definition["scenario_id"],
                    "name": definition["name"],
                    "current_impact_pct": current_impact,
                    "prior_portfolio_impact_pct": prior_impact,
                    "portfolio_drift_impact_pct": round(current_impact - prior_impact, 2),
                    "scenario_version": definition["scenario_version"],
                    "confidence_rating": definition["confidence_rating"],
                }
            )
        active_version = _active_version_row(conn, str(definition["scenario_id"]))
        previous_version = conn.execute(
            """
            SELECT *
            FROM scenario_versions
            WHERE scenario_id = ? AND is_active = 0
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (str(definition["scenario_id"]),),
        ).fetchone()
        if active_version is not None and previous_version is not None:
            active_impact = _impact(current_weights, json.loads(str(active_version["shocks_json"] or "{}")))
            previous_impact = _impact(current_weights, json.loads(str(previous_version["shocks_json"] or "{}")))
            version_comparisons.append(
                {
                    "scenario_id": str(definition["scenario_id"]),
                    "name": str(definition["name"]),
                    "active_version": str(active_version["version_label"]),
                    "prior_version": str(previous_version["version_label"]),
                    "active_version_impact_pct": active_impact,
                    "prior_version_impact_pct": previous_impact,
                    "version_drift_impact_pct": round(active_impact - previous_impact, 2),
                }
            )
    return {
        "current_vs_prior_portfolio": comparisons,
        "current_vs_prior_version": version_comparisons,
    }


def record_scenario_comparisons(
    conn: sqlite3.Connection,
    *,
    current_run_id: str,
    prior_run_id: str | None,
    comparisons: dict[str, Any],
) -> list[dict[str, Any]]:
    ensure_scenario_registry_tables(conn)
    now = _now_iso()
    inserted: list[dict[str, Any]] = []
    for item in list(comparisons.get("current_vs_prior_portfolio") or []):
        comparison_id = f"scenario_compare_{uuid.uuid4().hex[:12]}"
        conn.execute(
            """
            INSERT INTO scenario_comparison_snapshots (
              comparison_id, scenario_id, scenario_version_id, current_run_id, prior_run_id,
              current_impact_pct, prior_impact_pct, impact_delta_pct, created_at
            ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?)
            """,
            (
                comparison_id,
                str(item.get("scenario_id")),
                current_run_id,
                prior_run_id,
                float(item.get("current_impact_pct") or 0.0),
                float(item.get("prior_portfolio_impact_pct") or 0.0),
                float(item.get("portfolio_drift_impact_pct") or 0.0),
                now,
            ),
        )
        inserted.append(
            {
                "comparison_id": comparison_id,
                "scenario_id": str(item.get("scenario_id")),
                "current_run_id": current_run_id,
                "prior_run_id": prior_run_id,
                "impact_delta_pct": float(item.get("portfolio_drift_impact_pct") or 0.0),
                "created_at": now,
            }
        )
    conn.commit()
    return inserted


def list_scenario_comparison_history(
    conn: sqlite3.Connection,
    *,
    scenario_id: str | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    ensure_scenario_registry_tables(conn)
    if scenario_id:
        rows = conn.execute(
            """
            SELECT *
            FROM scenario_comparison_snapshots
            WHERE scenario_id = ?
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (scenario_id, max(1, limit)),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM scenario_comparison_snapshots
            ORDER BY created_at DESC
            LIMIT ?
            """,
            (max(1, limit),),
        ).fetchall()
    return [dict(row) for row in rows]
