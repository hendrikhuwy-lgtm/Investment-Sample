from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.services.policy_assumptions import (
    build_policy_citation,
    build_policy_source_record,
    build_policy_truth_state,
    ensure_policy_assumption_tables,
    get_policy_current,
    policy_render_labels,
    upsert_policy_observation,
)


DEFAULT_CMA_VERSION = "2026.03"
DEFAULT_ASSUMPTION_DATE = "2026-03-01"

DEFAULT_CMA_ROWS = [
    {
        "sleeve_key": "global_equity",
        "sleeve_name": "Global Equity",
        "expected_return_min": 0.06,
        "expected_return_max": 0.09,
        "confidence_label": "medium",
        "worst_year_loss_min": -0.22,
        "worst_year_loss_max": -0.38,
        "scenario_notes": "Policy-level range anchored on diversified global equity assumptions, not a short-term forecast.",
    },
    {
        "sleeve_key": "ig_bond",
        "sleeve_name": "Investment Grade Bonds",
        "expected_return_min": 0.03,
        "expected_return_max": 0.05,
        "confidence_label": "medium",
        "worst_year_loss_min": -0.06,
        "worst_year_loss_max": -0.15,
        "scenario_notes": "Range reflects rate-path sensitivity and carry normalization under policy assumptions.",
    },
    {
        "sleeve_key": "cash",
        "sleeve_name": "Cash and Bills",
        "expected_return_min": 0.02,
        "expected_return_max": 0.04,
        "confidence_label": "high",
        "worst_year_loss_min": 0.00,
        "worst_year_loss_max": -0.01,
        "scenario_notes": "Cash assumptions are policy placeholders for reserve capital rather than return-seeking allocations.",
    },
    {
        "sleeve_key": "real_asset",
        "sleeve_name": "Real Assets",
        "expected_return_min": 0.04,
        "expected_return_max": 0.07,
        "confidence_label": "low",
        "worst_year_loss_min": -0.12,
        "worst_year_loss_max": -0.28,
        "scenario_notes": "Real-asset ranges are wider because structure, inflation beta, and wrapper choice vary materially.",
    },
    {
        "sleeve_key": "alt",
        "sleeve_name": "Alternatives",
        "expected_return_min": 0.03,
        "expected_return_max": 0.06,
        "confidence_label": "low",
        "worst_year_loss_min": -0.10,
        "worst_year_loss_max": -0.24,
        "scenario_notes": "Alternatives are framed as diversifiers with wider uncertainty and lower precision.",
    },
    {
        "sleeve_key": "convex",
        "sleeve_name": "Convex Protection",
        "expected_return_min": -0.02,
        "expected_return_max": 0.03,
        "confidence_label": "low",
        "worst_year_loss_min": -0.03,
        "worst_year_loss_max": 0.12,
        "scenario_notes": "Convex sleeve assumptions focus on resilience support, not standalone expected return.",
    },
]


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_cma_tables(conn: sqlite3.Connection) -> None:
    ensure_policy_assumption_tables(conn)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS cma_assumptions (
          cma_id TEXT PRIMARY KEY,
          sleeve_key TEXT NOT NULL,
          sleeve_name TEXT NOT NULL,
          expected_return_min REAL NOT NULL,
          expected_return_max REAL NOT NULL,
          confidence_label TEXT NOT NULL,
          worst_year_loss_min REAL,
          worst_year_loss_max REAL,
          scenario_notes TEXT,
          assumption_date TEXT NOT NULL,
          version_label TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_cma_assumptions_active
        ON cma_assumptions (sleeve_key, version_label)
        """
    )
    conn.commit()


def seed_default_cma_assumptions(conn: sqlite3.Connection) -> None:
    ensure_cma_tables(conn)
    now = _now_iso()
    for row in DEFAULT_CMA_ROWS:
        exists = conn.execute(
            """
            SELECT cma_id
            FROM cma_assumptions
            WHERE sleeve_key = ? AND version_label = ?
            LIMIT 1
            """,
            (row["sleeve_key"], DEFAULT_CMA_VERSION),
        ).fetchone()
        if exists is not None:
            policy_current = get_policy_current(conn, assumption_family="cma", assumption_key=f"cma::{row['sleeve_key']}")
            if policy_current is not None:
                continue
        else:
            conn.execute(
                """
                INSERT INTO cma_assumptions (
                  cma_id, sleeve_key, sleeve_name, expected_return_min, expected_return_max, confidence_label,
                  worst_year_loss_min, worst_year_loss_max, scenario_notes, assumption_date,
                  version_label, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
                """,
                (
                    f"cma_{uuid.uuid4().hex[:12]}",
                    row["sleeve_key"],
                    row["sleeve_name"],
                    row["expected_return_min"],
                    row["expected_return_max"],
                    row["confidence_label"],
                    row["worst_year_loss_min"],
                    row["worst_year_loss_max"],
                    row["scenario_notes"],
                    DEFAULT_ASSUMPTION_DATE,
                    DEFAULT_CMA_VERSION,
                    now,
                    now,
                ),
            )
        upsert_policy_observation(
            conn,
            assumption_key=f"cma::{row['sleeve_key']}",
            assumption_family="cma",
            value={
                "sleeve_key": row["sleeve_key"],
                "sleeve_name": row["sleeve_name"],
                "expected_return_min": row["expected_return_min"],
                "expected_return_max": row["expected_return_max"],
                "worst_year_loss_min": row["worst_year_loss_min"],
                "worst_year_loss_max": row["worst_year_loss_max"],
                "scenario_notes": row["scenario_notes"],
                "version": DEFAULT_CMA_VERSION,
            },
            source_name="Developer seed CMA assumptions",
            source_url="https://policy.example/internal/cma/developer-seed",
            observed_at=DEFAULT_ASSUMPTION_DATE,
            methodology_note=(
                "Developer bootstrap capital-market assumption ranges used only until a cited methodology-backed observation is loaded."
            ),
            provenance_level="developer_seed",
            confidence_label=row["confidence_label"],
            overwrite_priority=0,
            is_current=True,
        )
    conn.commit()


def current_cma_version(conn: sqlite3.Connection) -> str:
    seed_default_cma_assumptions(conn)
    rows = list_current_cma_assumptions(conn)
    return str(rows[0].get("version") or DEFAULT_CMA_VERSION) if rows else DEFAULT_CMA_VERSION


def list_current_cma_assumptions(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    seed_default_cma_assumptions(conn)
    legacy_version = conn.execute(
        """
        SELECT version_label
        FROM cma_assumptions
        WHERE status = 'active'
        ORDER BY assumption_date DESC, updated_at DESC
        LIMIT 1
        """
    ).fetchone()
    rows = conn.execute(
        """
        SELECT sleeve_key, sleeve_name
        FROM cma_assumptions
        WHERE version_label = ? AND status = 'active'
        ORDER BY sleeve_key ASC
        """,
        (str(legacy_version["version_label"]) if legacy_version is not None else DEFAULT_CMA_VERSION,),
    ).fetchall()
    assumptions: list[dict[str, Any]] = []
    for row in rows:
        current = get_policy_current(
            conn,
            assumption_family="cma",
            assumption_key=f"cma::{row['sleeve_key']}",
        )
        if current is None:
            continue
        resolved = dict(current.get("resolved_value") or {})
        assumptions.append(
            {
                "sleeve_key": str(resolved.get("sleeve_key") or row["sleeve_key"]),
                "sleeve_name": str(resolved.get("sleeve_name") or row["sleeve_name"]),
                "expected_return_min": float(resolved.get("expected_return_min") or 0.0),
                "expected_return_max": float(resolved.get("expected_return_max") or 0.0),
                "confidence_label": str(current.get("confidence_label") or "low"),
                "worst_year_loss_min": float(resolved.get("worst_year_loss_min") or 0.0),
                "worst_year_loss_max": float(resolved.get("worst_year_loss_max") or 0.0),
                "scenario_notes": str(resolved.get("scenario_notes") or ""),
                "version": str(resolved.get("version") or DEFAULT_CMA_VERSION),
                "source_name": current.get("source_name"),
                "source_url": current.get("source_url"),
                "observed_at": current.get("observed_at"),
                "methodology_note": current.get("methodology_note"),
                "provenance_level": current.get("provenance_level"),
            }
        )
    return assumptions


def build_expected_return_range_section(conn: sqlite3.Connection) -> dict[str, Any]:
    assumptions = list_current_cma_assumptions(conn)
    version = current_cma_version(conn)
    truth_states = [
        build_policy_truth_state(
            provenance_level=str(item.get("provenance_level") or "developer_seed"),
            observed_at=str(item.get("observed_at") or "") or None,
        )
        for item in assumptions
    ]
    policy_truth_state = (
        "blocked"
        if not assumptions
        else "developer_seed"
        if any(state == "developer_seed" for state in truth_states)
        else "stale"
        if any(state == "stale" for state in truth_states)
        else "provisional"
        if any(state == "provisional" for state in truth_states)
        else "sourced"
    )
    source_records = []
    section_citations = []
    items: list[dict[str, Any]] = []
    for item in assumptions:
        truth_state = build_policy_truth_state(
            provenance_level=str(item.get("provenance_level") or "developer_seed"),
            observed_at=str(item.get("observed_at") or "") or None,
        )
        source_id = f"policy_cma_{item.get('sleeve_key')}"
        citation = build_policy_citation(
            source_id=source_id,
            url=str(item.get("source_url") or ""),
            importance=f"Policy CMA assumption for {item.get('sleeve_name')}",
            observed_at=str(item.get("observed_at") or "") or None,
            provenance_level=str(item.get("provenance_level") or "developer_seed"),
            confidence_label=str(item.get("confidence_label") or "low"),
            methodology_class="capital_market_assumption",
        )
        source_record = build_policy_source_record(
            source_id=source_id,
            url=str(item.get("source_url") or ""),
            publisher=str(item.get("source_name") or "Policy source"),
            topic="policy_assumption",
            credibility_tier="secondary" if truth_state in {"developer_seed", "provisional", "stale"} else "primary",
        )
        if citation is not None:
            section_citations.append(citation)
        if source_record is not None:
            source_records.append(source_record)
        items.append(
            {
                "sleeve_key": str(item.get("sleeve_key")),
                "sleeve_name": str(item.get("sleeve_name")),
                "expected_return_min": float(item.get("expected_return_min") or 0.0),
                "expected_return_max": float(item.get("expected_return_max") or 0.0),
                "confidence_label": str(item.get("confidence_label") or "low"),
                "scenario_notes": str(item.get("scenario_notes") or ""),
                "worst_year_loss_min": float(item.get("worst_year_loss_min") or 0.0),
                "worst_year_loss_max": float(item.get("worst_year_loss_max") or 0.0),
                "source_name": item.get("source_name"),
                "source_url": item.get("source_url"),
                "observed_at": item.get("observed_at"),
                "methodology_note": item.get("methodology_note"),
                "provenance_level": item.get("provenance_level"),
                "policy_truth_state": truth_state,
                "policy_labels": policy_render_labels(truth_state),
                "citations": [citation] if citation is not None else [],
            }
        )
    return {
        "version": version,
        "assumption_date": max((str(item.get("observed_at") or "") for item in assumptions), default=DEFAULT_ASSUMPTION_DATE),
        "policy_truth_state": policy_truth_state,
        "policy_labels": policy_render_labels(policy_truth_state),
        "source_records": source_records,
        "citations": section_citations,
        "items": items,
        "caveat": (
            "Reference-only capital-market assumption ranges. They are policy inputs, not forecasts, timing signals, or allocation instructions."
            if policy_truth_state in {"developer_seed", "provisional", "stale", "blocked"}
            else "Ranges are cited policy assumptions for framing. They are not point forecasts or timing signals."
        ),
    }
