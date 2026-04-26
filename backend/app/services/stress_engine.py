from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime

from app.models.types import StressScenarioResult
from app.services.policy_assumptions import (
    build_policy_citation,
    build_policy_source_record,
    build_policy_truth_state,
    ensure_policy_assumption_tables,
    policy_render_labels,
)


SCENARIO_SLEEVE_SHOCKS: dict[str, dict[str, float]] = {
    "rate_shock_analog": {
        "global_equity": -0.08,
        "ig_bond": -0.06,
        "real_asset": -0.03,
        "alt": -0.01,
        "convex": 0.02,
        "cash": 0.00,
    },
    "credit_shock_analog": {
        "global_equity": -0.11,
        "ig_bond": -0.07,
        "real_asset": -0.05,
        "alt": -0.04,
        "convex": 0.03,
        "cash": 0.00,
    },
    "volatility_spike_analog": {
        "global_equity": -0.09,
        "ig_bond": -0.02,
        "real_asset": -0.03,
        "alt": -0.02,
        "convex": 0.04,
        "cash": 0.00,
    },
    "drawdown_2008_style_analog": {
        "global_equity": -0.35,
        "ig_bond": -0.05,
        "real_asset": -0.18,
        "alt": -0.12,
        "convex": 0.10,
        "cash": 0.00,
    },
    "rate_regime_2022_style_analog": {
        "global_equity": -0.17,
        "ig_bond": -0.14,
        "real_asset": -0.07,
        "alt": -0.04,
        "convex": 0.05,
        "cash": 0.00,
    },
}


SCENARIO_LABELS: dict[str, str] = {
    "rate_shock_analog": "Rate shock analog",
    "credit_shock_analog": "Credit shock analog",
    "volatility_spike_analog": "Volatility spike analog",
    "drawdown_2008_style_analog": "2008-style equity drawdown analog",
    "rate_regime_2022_style_analog": "2022 rate regime analog",
}

SCENARIO_PROBABILITY_WEIGHTS: dict[str, float] = {
    "rate_shock_analog": 0.22,
    "credit_shock_analog": 0.18,
    "volatility_spike_analog": 0.20,
    "drawdown_2008_style_analog": 0.15,
    "rate_regime_2022_style_analog": 0.25,
}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_stress_history_tables(conn: sqlite3.Connection) -> None:
    ensure_policy_assumption_tables(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stress_scenario_history (
          scenario_record_id TEXT PRIMARY KEY,
          as_of_ts TEXT NOT NULL,
          scenario_id TEXT NOT NULL,
          scenario_name TEXT NOT NULL,
          scenario_probability_weight REAL,
          estimated_impact_pct REAL NOT NULL,
          convex_contribution_pct REAL,
          ex_convex_impact_pct REAL,
          scenario_version TEXT NOT NULL DEFAULT '1.0'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_stress_scenario_history_asof
        ON stress_scenario_history (as_of_ts DESC, scenario_id)
        """
    )
    conn.commit()


def seed_default_stress_methodologies(conn: sqlite3.Connection) -> None:
    ensure_stress_history_tables(conn)
    for scenario_key, shocks in SCENARIO_SLEEVE_SHOCKS.items():
        exists = conn.execute(
            """
            SELECT scenario_key
            FROM stress_methodology_registry
            WHERE scenario_key = ?
            LIMIT 1
            """,
            (scenario_key,),
        ).fetchone()
        if exists is not None:
            continue
        conn.execute(
            """
            INSERT INTO stress_methodology_registry (
              scenario_key, shock_definition_json, methodology_source_name, methodology_source_url,
              observed_at, provenance_level, confidence_label, notes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scenario_key,
                json.dumps(shocks),
                "Developer seed stress analog methodology",
                "https://policy.example/internal/stress/developer-seed",
                datetime.now(UTC).date().isoformat(),
                "developer_seed",
                "low",
                "Seeded sleeve-level analog shocks pending a cited scenario methodology or approved internal stress policy.",
            ),
        )
    conn.commit()


def _scenario_impact(weights: dict[str, float], shocks: dict[str, float]) -> float:
    return sum(float(weights.get(sleeve, 0.0)) * float(shock) for sleeve, shock in shocks.items())


def run_stress_scenarios(weights: dict[str, float]) -> list[StressScenarioResult]:
    results: list[StressScenarioResult] = []
    for scenario_id, shocks in SCENARIO_SLEEVE_SHOCKS.items():
        impact_pct = _scenario_impact(weights, shocks) * 100.0
        results.append(
            StressScenarioResult(
                scenario_id=scenario_id,
                name=SCENARIO_LABELS.get(scenario_id, scenario_id.replace("_", " ")),
                estimated_impact_pct=round(impact_pct, 2),
                diagnostic=(
                    "Scenario sensitivity estimate based on current sleeve exposures; "
                    "used for monitoring and resilience diagnostics."
                ),
            )
        )
    return results


def run_stress_suite(
    weights: dict[str, float],
    *,
    convex_carry_estimate_pct: float = 0.0,
    scenario_definitions: list[dict] | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict:
    detailed = []
    methodology_rows: dict[str, dict] = {}
    if conn is not None:
        seed_default_stress_methodologies(conn)
        rows = conn.execute("SELECT * FROM stress_methodology_registry").fetchall()
        methodology_rows = {str(row["scenario_key"]): dict(row) for row in rows}
    definitions = scenario_definitions or [
        {
            "scenario_id": scenario_id,
            "name": SCENARIO_LABELS.get(scenario_id, scenario_id.replace("_", " ")),
            "scenario_probability_weight": float(SCENARIO_PROBABILITY_WEIGHTS.get(scenario_id, 0.0)),
            "scenario_version": "1.0",
            "confidence_rating": "medium",
            "shocks": shocks,
        }
        for scenario_id, shocks in SCENARIO_SLEEVE_SHOCKS.items()
    ]
    source_records = []
    citations = []
    truth_states: list[str] = []
    for definition in definitions:
        scenario_id = str(definition.get("scenario_id"))
        shocks = dict(definition.get("shocks") or {})
        methodology_row = methodology_rows.get(scenario_id, {})
        truth_state = build_policy_truth_state(
            provenance_level=str(methodology_row.get("provenance_level") or "developer_seed"),
            observed_at=str(methodology_row.get("observed_at") or "") or None,
        )
        truth_states.append(truth_state)
        source_id = f"policy_stress_{scenario_id}"
        citation = build_policy_citation(
            source_id=source_id,
            url=str(methodology_row.get("methodology_source_url") or ""),
            importance=f"Stress methodology for {definition.get('name') or scenario_id}",
            observed_at=str(methodology_row.get("observed_at") or "") or None,
            provenance_level=str(methodology_row.get("provenance_level") or "developer_seed"),
            confidence_label=str(methodology_row.get("confidence_label") or definition.get("confidence_rating") or "low"),
            methodology_class="stress_methodology",
        )
        source_record = build_policy_source_record(
            source_id=source_id,
            url=str(methodology_row.get("methodology_source_url") or ""),
            publisher=str(methodology_row.get("methodology_source_name") or "Stress methodology source"),
            topic="stress_methodology",
            credibility_tier="secondary" if truth_state in {"developer_seed", "provisional", "stale"} else "primary",
        )
        if citation is not None:
            citations.append(citation)
        if source_record is not None:
            source_records.append(source_record)
        total = _scenario_impact(weights, shocks)
        convex_component = float(weights.get("convex", 0.0)) * float(shocks.get("convex", 0.0))
        ex_convex = total - convex_component
        detailed.append(
            {
                "scenario_id": scenario_id,
                "name": str(definition.get("name") or SCENARIO_LABELS.get(scenario_id, scenario_id.replace("_", " "))),
                "scenario_probability_weight": float(definition.get("scenario_probability_weight") or 0.0),
                "scenario_version": str(definition.get("scenario_version") or "1.0"),
                "confidence_rating": str(definition.get("confidence_rating") or "medium"),
                "methodology_source_name": methodology_row.get("methodology_source_name"),
                "methodology_source_url": methodology_row.get("methodology_source_url"),
                "methodology_note": methodology_row.get("notes"),
                "observed_at": methodology_row.get("observed_at"),
                "provenance_level": methodology_row.get("provenance_level"),
                "policy_truth_state": truth_state,
                "policy_labels": policy_render_labels(truth_state),
                "citations": [citation] if citation is not None else [],
                "estimated_impact_pct": round(total * 100.0, 2),
                "convex_contribution_pct": round(convex_component * 100.0, 2),
                "ex_convex_impact_pct": round(ex_convex * 100.0, 2),
                "diagnostic": (
                    "Scenario estimate derived from sleeve-level methodology inputs; "
                    "use for resilience monitoring, not directional prediction or allocation instruction."
                ),
            }
        )

    worst_case = min((item["estimated_impact_pct"] for item in detailed), default=0.0)
    mean_impact = sum(item["estimated_impact_pct"] for item in detailed) / max(len(detailed), 1)
    convex_support = sum(item["convex_contribution_pct"] for item in detailed if item["convex_contribution_pct"] > 0)
    resilience_score = max(
        0.0,
        min(100.0, 100.0 + worst_case + (convex_support * 0.4) - (convex_carry_estimate_pct * 10.0)),
    )
    weighted_impact = sum(
        float(item["estimated_impact_pct"]) * float(item.get("scenario_probability_weight") or 0.0)
        for item in detailed
    )
    suite_truth_state = (
        "blocked"
        if not detailed
        else "developer_seed"
        if any(state == "developer_seed" for state in truth_states)
        else "stale"
        if any(state == "stale" for state in truth_states)
        else "provisional"
        if any(state == "provisional" for state in truth_states)
        else "sourced"
    )
    return {
        "policy_truth_state": suite_truth_state,
        "policy_labels": policy_render_labels(suite_truth_state),
        "citations": citations,
        "source_records": source_records,
        "scenarios": detailed,
        "summary": {
            "worst_case_pct": round(worst_case, 2),
            "average_impact_pct": round(mean_impact, 2),
            "probability_weighted_impact_pct": round(weighted_impact, 2),
            "convex_support_score": round(convex_support, 2),
            "resilience_score": round(resilience_score, 2),
            "convex_carry_estimate_pct": round(convex_carry_estimate_pct, 4),
        },
    }


def record_stress_suite(conn: sqlite3.Connection, stress: dict) -> None:
    ensure_stress_history_tables(conn)
    as_of_ts = _now_iso()
    for item in list(stress.get("scenarios") or []):
        conn.execute(
            """
            INSERT INTO stress_scenario_history (
              scenario_record_id, as_of_ts, scenario_id, scenario_name, scenario_probability_weight,
              estimated_impact_pct, convex_contribution_pct, ex_convex_impact_pct, scenario_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"stress_record_{uuid.uuid4().hex[:12]}",
                as_of_ts,
                str(item.get("scenario_id")),
                str(item.get("name")),
                float(item.get("scenario_probability_weight") or 0.0),
                float(item.get("estimated_impact_pct") or 0.0),
                float(item.get("convex_contribution_pct") or 0.0),
                float(item.get("ex_convex_impact_pct") or 0.0),
                str(item.get("scenario_version") or "1.0"),
            ),
        )
    conn.commit()


def list_stress_history(conn: sqlite3.Connection, *, limit_runs: int = 5) -> list[dict]:
    ensure_stress_history_tables(conn)
    rows = conn.execute(
        """
        SELECT as_of_ts, scenario_id, scenario_name, scenario_probability_weight, estimated_impact_pct,
               convex_contribution_pct, ex_convex_impact_pct, scenario_version
        FROM stress_scenario_history
        ORDER BY as_of_ts DESC
        LIMIT ?
        """,
        (max(1, limit_runs * 10),),
    ).fetchall()
    return [dict(row) for row in rows]
