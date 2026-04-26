from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.services.policy_assumptions import (
    build_policy_citation,
    build_policy_source_record,
    build_policy_truth_state,
    ensure_policy_assumption_tables,
    policy_render_labels,
)


DEFAULT_BENCHMARK_VERSION = "2026.03"
DEFAULT_ASSUMPTION_DATE = "2026-03-01"
DEFAULT_CONTEXT_KEY = "daily_brief_reference_profile"

DEFAULT_COMPONENTS = [
    {
        "component_key": "global_equity",
        "component_name": "Global Equity",
        "weight": 0.50,
        "rationale": "Primary growth anchor for a diversified long-horizon policy benchmark.",
    },
    {
        "component_key": "ig_bond",
        "component_name": "Investment Grade Bonds",
        "weight": 0.20,
        "rationale": "Core ballast sleeve for duration and income context.",
    },
    {
        "component_key": "cash",
        "component_name": "Cash and Bills",
        "weight": 0.10,
        "rationale": "Liquidity reserve and deployment optionality.",
    },
    {
        "component_key": "real_asset",
        "component_name": "Real Assets",
        "weight": 0.10,
        "rationale": "Inflation-sensitive diversifier sleeve.",
    },
    {
        "component_key": "alt",
        "component_name": "Alternatives",
        "weight": 0.07,
        "rationale": "Diversifying return sources with wider dispersion.",
    },
    {
        "component_key": "convex",
        "component_name": "Convex Protection",
        "weight": 0.03,
        "rationale": "Resilience sleeve rather than a return-maximizing bucket.",
    },
]


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_benchmark_tables(conn: sqlite3.Connection) -> None:
    ensure_policy_assumption_tables(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_definitions (
          benchmark_definition_id TEXT PRIMARY KEY,
          context_key TEXT NOT NULL,
          benchmark_name TEXT NOT NULL,
          version_label TEXT NOT NULL,
          components_json TEXT NOT NULL DEFAULT '[]',
          rationale TEXT,
          assumption_date TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_benchmark_definitions_context_version
        ON benchmark_definitions (context_key, version_label)
        """
    )
    conn.commit()


def seed_default_benchmark_definitions(conn: sqlite3.Connection) -> None:
    ensure_benchmark_tables(conn)
    exists = conn.execute(
        """
        SELECT benchmark_definition_id
        FROM benchmark_definitions
        WHERE context_key = ? AND version_label = ?
        LIMIT 1
        """,
        (DEFAULT_CONTEXT_KEY, DEFAULT_BENCHMARK_VERSION),
    ).fetchone()
    now = _now_iso()
    if exists is None:
        conn.execute(
            """
            INSERT INTO benchmark_definitions (
              benchmark_definition_id, context_key, benchmark_name, version_label,
              components_json, rationale, assumption_date, status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
            """,
            (
                f"benchmark_{uuid.uuid4().hex[:12]}",
                DEFAULT_CONTEXT_KEY,
                "Reference Policy Composite Benchmark",
                DEFAULT_BENCHMARK_VERSION,
                json.dumps(DEFAULT_COMPONENTS),
                (
                    "Developer-seed composite benchmark for policy-level comparison inside the Daily Brief. "
                    "It is intended for review context rather than attribution or allocation instruction."
                ),
                DEFAULT_ASSUMPTION_DATE,
                now,
                now,
            ),
        )
    for item in DEFAULT_COMPONENTS:
        profile_exists = conn.execute(
            """
            SELECT profile_row_id
            FROM benchmark_policy_profiles
            WHERE profile_key = ? AND sleeve_key = ? AND is_current = 1
            LIMIT 1
            """,
            (DEFAULT_CONTEXT_KEY, str(item["component_key"])),
        ).fetchone()
        if profile_exists is not None:
            continue
        conn.execute(
            """
            INSERT INTO benchmark_policy_profiles (
              profile_row_id, profile_key, sleeve_key, target_weight, min_weight, max_weight,
              source_name, source_url, methodology_note, observed_at, provenance_level, confidence_label, is_current
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
            """,
            (
                f"benchmark_profile_{uuid.uuid4().hex[:12]}",
                DEFAULT_CONTEXT_KEY,
                str(item["component_key"]),
                float(item["weight"]),
                max(0.0, float(item["weight"]) - 0.05),
                min(1.0, float(item["weight"]) + 0.05),
                "Persisted policy profile developer seed",
                "https://policy.example/internal/benchmark/developer-seed",
                "Bootstrap policy benchmark profile used until a cited benchmark methodology or approved internal policy profile is loaded.",
                DEFAULT_ASSUMPTION_DATE,
                "developer_seed",
                "low",
            ),
        )
    conn.commit()


def get_current_benchmark(conn: sqlite3.Connection, context_key: str = DEFAULT_CONTEXT_KEY) -> dict[str, Any]:
    seed_default_benchmark_definitions(conn)
    row = conn.execute(
        """
        SELECT *
        FROM benchmark_definitions
        WHERE context_key = ? AND status = 'active'
        ORDER BY assumption_date DESC, updated_at DESC
        LIMIT 1
        """,
        (context_key,),
    ).fetchone()
    if row is None:
        return {
            "benchmark_definition_id": None,
            "benchmark_name": "Reference Policy Composite Benchmark",
            "version": DEFAULT_BENCHMARK_VERSION,
            "assumption_date": DEFAULT_ASSUMPTION_DATE,
            "components": list(DEFAULT_COMPONENTS),
            "rationale": "Default composite benchmark policy context.",
        }
    profile_rows = conn.execute(
        """
        SELECT *
        FROM benchmark_policy_profiles
        WHERE profile_key = ? AND is_current = 1
        ORDER BY sleeve_key ASC
        """,
        (context_key,),
    ).fetchall()
    component_map = {str(item.get("component_key")): dict(item) for item in json.loads(str(row["components_json"] or "[]"))}
    components = []
    source_records = []
    citations = []
    truth_states: list[str] = []
    for profile_row in profile_rows:
        profile = dict(profile_row)
        truth_state = build_policy_truth_state(
            provenance_level=str(profile.get("provenance_level") or "developer_seed"),
            observed_at=str(profile.get("observed_at") or "") or None,
        )
        truth_states.append(truth_state)
        sleeve_key = str(profile.get("sleeve_key") or "")
        base = component_map.get(sleeve_key, {})
        source_id = f"policy_benchmark_{context_key}_{sleeve_key}"
        citation = build_policy_citation(
            source_id=source_id,
            url=str(profile.get("source_url") or ""),
            importance=f"Benchmark policy profile for {sleeve_key}",
            observed_at=str(profile.get("observed_at") or "") or None,
            provenance_level=str(profile.get("provenance_level") or "developer_seed"),
            confidence_label=str(profile.get("confidence_label") or "low"),
            methodology_class="benchmark_policy_profile",
        )
        source_record = build_policy_source_record(
            source_id=source_id,
            url=str(profile.get("source_url") or ""),
            publisher=str(profile.get("source_name") or "Policy source"),
            topic="benchmark_policy",
            credibility_tier="secondary" if truth_state in {"developer_seed", "provisional", "stale"} else "primary",
        )
        if citation is not None:
            citations.append(citation)
        if source_record is not None:
            source_records.append(source_record)
        components.append(
            {
                "component_key": sleeve_key,
                "component_name": str(base.get("component_name") or sleeve_key.replace("_", " ").title()),
                "weight": float(profile.get("target_weight") or 0.0),
                "min_weight": float(profile.get("min_weight") or 0.0),
                "max_weight": float(profile.get("max_weight") or 0.0),
                "rationale": str(base.get("rationale") or profile.get("methodology_note") or ""),
                "source_name": profile.get("source_name"),
                "source_url": profile.get("source_url"),
                "observed_at": profile.get("observed_at"),
                "methodology_note": profile.get("methodology_note"),
                "confidence_label": profile.get("confidence_label"),
                "provenance_level": profile.get("provenance_level"),
                "policy_truth_state": truth_state,
                "policy_labels": policy_render_labels(truth_state),
                "citations": [citation] if citation is not None else [],
            }
        )
    policy_truth_state = (
        "blocked"
        if not components
        else "developer_seed"
        if any(state == "developer_seed" for state in truth_states)
        else "stale"
        if any(state == "stale" for state in truth_states)
        else "provisional"
        if any(state == "provisional" for state in truth_states)
        else "sourced"
    )
    return {
        "benchmark_definition_id": str(row["benchmark_definition_id"]),
        "context_key": str(row["context_key"]),
        "benchmark_name": str(row["benchmark_name"]),
        "version": str(row["version_label"]),
        "assumption_date": str(row["assumption_date"]),
        "policy_truth_state": policy_truth_state,
        "policy_labels": policy_render_labels(policy_truth_state),
        "components": components,
        "rationale": str(row["rationale"] or ""),
        "citations": citations,
        "source_records": source_records,
    }


def build_comparison_context(
    benchmark: dict[str, Any],
    expected_return_section: dict[str, Any] | None = None,
) -> dict[str, Any]:
    components = list(benchmark.get("components") or [])
    expected_map = {
        str(item.get("sleeve_key")): item
        for item in list((expected_return_section or {}).get("items") or [])
    }
    low = 0.0
    high = 0.0
    for component in components:
        weight = float(component.get("weight") or 0.0)
        assumption = expected_map.get(str(component.get("component_key") or ""))
        if assumption is None:
            continue
        low += weight * float(assumption.get("expected_return_min") or 0.0)
        high += weight * float(assumption.get("expected_return_max") or 0.0)
    return {
        "benchmark_definition_id": benchmark.get("benchmark_definition_id"),
        "context_key": benchmark.get("context_key"),
        "benchmark_name": str(benchmark.get("benchmark_name") or "Reference Policy Composite Benchmark"),
        "version": str(benchmark.get("version") or DEFAULT_BENCHMARK_VERSION),
        "assumption_date": str(benchmark.get("assumption_date") or DEFAULT_ASSUMPTION_DATE),
        "policy_truth_state": str(benchmark.get("policy_truth_state") or "developer_seed"),
        "policy_labels": list(benchmark.get("policy_labels") or []),
        "components": components,
        "rationale": str(benchmark.get("rationale") or ""),
        "citations": list(benchmark.get("citations") or []),
        "source_records": list(benchmark.get("source_records") or []),
        "expected_return_min": round(low, 4),
        "expected_return_max": round(high, 4),
        "caveat": (
            "Reference-only benchmark context. These weights should not be read as portfolio guidance until sourced or approved policy inputs replace the developer seed."
            if str(benchmark.get("policy_truth_state") or "developer_seed") in {"developer_seed", "provisional", "stale", "blocked"}
            else "Benchmark ranges inherit the same policy-level assumption discipline and should not be read as forecasts."
        ),
    }
