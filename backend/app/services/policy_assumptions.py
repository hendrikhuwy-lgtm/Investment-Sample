from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.models.types import Citation, SourceRecord


POLICY_TRUTH_STATES = {"sourced", "provisional", "developer_seed", "stale", "blocked"}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_policy_assumption_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS policy_assumption_observations (
          observation_id TEXT PRIMARY KEY,
          assumption_key TEXT NOT NULL,
          assumption_family TEXT NOT NULL,
          value_json TEXT NOT NULL,
          source_name TEXT,
          source_url TEXT,
          observed_at TEXT,
          ingested_at TEXT NOT NULL,
          methodology_note TEXT,
          provenance_level TEXT NOT NULL DEFAULT 'developer_seed',
          confidence_label TEXT NOT NULL DEFAULT 'low',
          overwrite_priority INTEGER NOT NULL DEFAULT 0,
          is_current INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_policy_assumption_observations_key
        ON policy_assumption_observations (assumption_family, assumption_key, is_current, ingested_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS policy_assumption_current (
          assumption_key TEXT PRIMARY KEY,
          assumption_family TEXT NOT NULL,
          resolved_value_json TEXT NOT NULL,
          source_name TEXT,
          source_url TEXT,
          observed_at TEXT,
          ingested_at TEXT NOT NULL,
          methodology_note TEXT,
          provenance_level TEXT NOT NULL DEFAULT 'developer_seed',
          confidence_label TEXT NOT NULL DEFAULT 'low',
          last_resolved_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS benchmark_policy_profiles (
          profile_row_id TEXT PRIMARY KEY,
          profile_key TEXT NOT NULL,
          sleeve_key TEXT NOT NULL,
          target_weight REAL NOT NULL,
          min_weight REAL,
          max_weight REAL,
          source_name TEXT,
          source_url TEXT,
          methodology_note TEXT,
          observed_at TEXT,
          provenance_level TEXT NOT NULL DEFAULT 'developer_seed',
          confidence_label TEXT NOT NULL DEFAULT 'low',
          is_current INTEGER NOT NULL DEFAULT 1
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_benchmark_policy_profiles_key
        ON benchmark_policy_profiles (profile_key, is_current, sleeve_key)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS stress_methodology_registry (
          scenario_key TEXT PRIMARY KEY,
          shock_definition_json TEXT NOT NULL,
          methodology_source_name TEXT,
          methodology_source_url TEXT,
          observed_at TEXT,
          provenance_level TEXT NOT NULL DEFAULT 'developer_seed',
          confidence_label TEXT NOT NULL DEFAULT 'low',
          notes TEXT
        )
        """
    )
    conn.commit()


def _priority_for(provenance_level: str, overwrite_priority: int) -> tuple[int, int]:
    tier_map = {
        "sourced": 4,
        "external": 4,
        "internal_policy": 3,
        "provisional": 2,
        "developer_seed": 1,
        "blocked": 0,
    }
    return (tier_map.get(str(provenance_level or "developer_seed"), 1), int(overwrite_priority or 0))


def upsert_policy_observation(
    conn: sqlite3.Connection,
    *,
    assumption_key: str,
    assumption_family: str,
    value: dict[str, Any],
    source_name: str | None,
    source_url: str | None,
    observed_at: str | None,
    methodology_note: str | None,
    provenance_level: str,
    confidence_label: str,
    overwrite_priority: int = 0,
    is_current: bool = True,
) -> dict[str, Any]:
    ensure_policy_assumption_tables(conn)
    ingested_at = _now_iso()
    observation_id = f"policy_obs_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO policy_assumption_observations (
          observation_id, assumption_key, assumption_family, value_json, source_name, source_url,
          observed_at, ingested_at, methodology_note, provenance_level, confidence_label,
          overwrite_priority, is_current
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            observation_id,
            assumption_key,
            assumption_family,
            json.dumps(value),
            source_name,
            source_url,
            observed_at,
            ingested_at,
            methodology_note,
            provenance_level,
            confidence_label,
            int(overwrite_priority),
            1 if is_current else 0,
        ),
    )
    resolve_policy_current(conn, assumption_family=assumption_family, assumption_key=assumption_key)
    conn.commit()
    return {
        "observation_id": observation_id,
        "assumption_key": assumption_key,
        "assumption_family": assumption_family,
        "value": value,
        "source_name": source_name,
        "source_url": source_url,
        "observed_at": observed_at,
        "ingested_at": ingested_at,
        "methodology_note": methodology_note,
        "provenance_level": provenance_level,
        "confidence_label": confidence_label,
    }


def resolve_policy_current(
    conn: sqlite3.Connection,
    *,
    assumption_family: str,
    assumption_key: str,
) -> dict[str, Any] | None:
    ensure_policy_assumption_tables(conn)
    rows = conn.execute(
        """
        SELECT *
        FROM policy_assumption_observations
        WHERE assumption_family = ? AND assumption_key = ? AND is_current = 1
        ORDER BY ingested_at DESC
        """,
        (assumption_family, assumption_key),
    ).fetchall()
    if not rows:
        return None

    best = max(
        (dict(row) for row in rows),
        key=lambda row: (
            _priority_for(str(row.get("provenance_level") or "developer_seed"), int(row.get("overwrite_priority") or 0)),
            str(row.get("observed_at") or ""),
            str(row.get("ingested_at") or ""),
        ),
    )
    resolved = {
        "assumption_key": assumption_key,
        "assumption_family": assumption_family,
        "resolved_value": json.loads(str(best.get("value_json") or "{}")),
        "source_name": best.get("source_name"),
        "source_url": best.get("source_url"),
        "observed_at": best.get("observed_at"),
        "ingested_at": best.get("ingested_at"),
        "methodology_note": best.get("methodology_note"),
        "provenance_level": best.get("provenance_level"),
        "confidence_label": best.get("confidence_label"),
        "last_resolved_at": _now_iso(),
    }
    conn.execute(
        """
        INSERT INTO policy_assumption_current (
          assumption_key, assumption_family, resolved_value_json, source_name, source_url,
          observed_at, ingested_at, methodology_note, provenance_level, confidence_label, last_resolved_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(assumption_key) DO UPDATE SET
          assumption_family=excluded.assumption_family,
          resolved_value_json=excluded.resolved_value_json,
          source_name=excluded.source_name,
          source_url=excluded.source_url,
          observed_at=excluded.observed_at,
          ingested_at=excluded.ingested_at,
          methodology_note=excluded.methodology_note,
          provenance_level=excluded.provenance_level,
          confidence_label=excluded.confidence_label,
          last_resolved_at=excluded.last_resolved_at
        """,
        (
            assumption_key,
            assumption_family,
            json.dumps(resolved["resolved_value"]),
            resolved["source_name"],
            resolved["source_url"],
            resolved["observed_at"],
            resolved["ingested_at"],
            resolved["methodology_note"],
            resolved["provenance_level"],
            resolved["confidence_label"],
            resolved["last_resolved_at"],
        ),
    )
    conn.commit()
    return resolved


def get_policy_current(
    conn: sqlite3.Connection,
    *,
    assumption_family: str,
    assumption_key: str,
) -> dict[str, Any] | None:
    ensure_policy_assumption_tables(conn)
    row = conn.execute(
        """
        SELECT *
        FROM policy_assumption_current
        WHERE assumption_family = ? AND assumption_key = ?
        LIMIT 1
        """,
        (assumption_family, assumption_key),
    ).fetchone()
    if row is None:
        resolved = resolve_policy_current(conn, assumption_family=assumption_family, assumption_key=assumption_key)
        return resolved
    payload = dict(row)
    payload["resolved_value"] = json.loads(str(payload.get("resolved_value_json") or "{}"))
    return payload


def list_policy_assumptions_current(
    conn: sqlite3.Connection,
    *,
    assumption_family: str | None = None,
) -> list[dict[str, Any]]:
    ensure_policy_assumption_tables(conn)
    if assumption_family:
        rows = conn.execute(
            """
            SELECT *
            FROM policy_assumption_current
            WHERE assumption_family = ?
            ORDER BY assumption_family ASC, assumption_key ASC
            """,
            (assumption_family,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT *
            FROM policy_assumption_current
            ORDER BY assumption_family ASC, assumption_key ASC
            """
        ).fetchall()
    payloads: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["resolved_value"] = json.loads(str(payload.get("resolved_value_json") or "{}"))
        payload["policy_truth_state"] = build_policy_truth_state(
            provenance_level=str(payload.get("provenance_level") or "developer_seed"),
            observed_at=str(payload.get("observed_at") or "") or None,
        )
        payload["policy_labels"] = policy_render_labels(str(payload["policy_truth_state"]))
        payloads.append(payload)
    return payloads


def list_benchmark_policy_profiles(
    conn: sqlite3.Connection,
    *,
    profile_key: str | None = None,
    include_inactive: bool = False,
) -> list[dict[str, Any]]:
    ensure_policy_assumption_tables(conn)
    where = []
    params: list[Any] = []
    if profile_key:
        where.append("profile_key = ?")
        params.append(profile_key)
    if not include_inactive:
        where.append("is_current = 1")
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    rows = conn.execute(
        f"""
        SELECT *
        FROM benchmark_policy_profiles
        {where_sql}
        ORDER BY profile_key ASC, sleeve_key ASC
        """,
        tuple(params),
    ).fetchall()
    payloads: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["policy_truth_state"] = build_policy_truth_state(
            provenance_level=str(payload.get("provenance_level") or "developer_seed"),
            observed_at=str(payload.get("observed_at") or "") or None,
        )
        payload["policy_labels"] = policy_render_labels(str(payload["policy_truth_state"]))
        payloads.append(payload)
    return payloads


def upsert_benchmark_policy_profile(
    conn: sqlite3.Connection,
    *,
    profile_key: str,
    sleeve_key: str,
    target_weight: float,
    min_weight: float | None = None,
    max_weight: float | None = None,
    source_name: str | None = None,
    source_url: str | None = None,
    methodology_note: str | None = None,
    observed_at: str | None = None,
    provenance_level: str = "provisional",
    confidence_label: str = "medium",
    is_current: bool = True,
) -> dict[str, Any]:
    ensure_policy_assumption_tables(conn)
    existing = conn.execute(
        """
        SELECT profile_row_id
        FROM benchmark_policy_profiles
        WHERE profile_key = ? AND sleeve_key = ?
        LIMIT 1
        """,
        (profile_key, sleeve_key),
    ).fetchone()
    profile_row_id = str(existing["profile_row_id"]) if existing is not None else f"benchmark_profile_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO benchmark_policy_profiles (
          profile_row_id, profile_key, sleeve_key, target_weight, min_weight, max_weight,
          source_name, source_url, methodology_note, observed_at, provenance_level, confidence_label, is_current
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(profile_row_id) DO UPDATE SET
          profile_key=excluded.profile_key,
          sleeve_key=excluded.sleeve_key,
          target_weight=excluded.target_weight,
          min_weight=excluded.min_weight,
          max_weight=excluded.max_weight,
          source_name=excluded.source_name,
          source_url=excluded.source_url,
          methodology_note=excluded.methodology_note,
          observed_at=excluded.observed_at,
          provenance_level=excluded.provenance_level,
          confidence_label=excluded.confidence_label,
          is_current=excluded.is_current
        """,
        (
            profile_row_id,
            profile_key,
            sleeve_key,
            target_weight,
            min_weight,
            max_weight,
            source_name,
            source_url,
            methodology_note,
            observed_at,
            provenance_level,
            confidence_label,
            1 if is_current else 0,
        ),
    )
    conn.commit()
    return next(
        item for item in list_benchmark_policy_profiles(conn, profile_key=profile_key, include_inactive=True)
        if str(item.get("profile_row_id")) == profile_row_id
    )


def list_stress_methodology_registry(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_policy_assumption_tables(conn)
    rows = conn.execute(
        """
        SELECT *
        FROM stress_methodology_registry
        ORDER BY scenario_key ASC
        """
    ).fetchall()
    payloads: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["shock_definition"] = json.loads(str(payload.get("shock_definition_json") or "{}"))
        payload["policy_truth_state"] = build_policy_truth_state(
            provenance_level=str(payload.get("provenance_level") or "developer_seed"),
            observed_at=str(payload.get("observed_at") or "") or None,
        )
        payload["policy_labels"] = policy_render_labels(str(payload["policy_truth_state"]))
        payloads.append(payload)
    return payloads


def upsert_stress_methodology(
    conn: sqlite3.Connection,
    *,
    scenario_key: str,
    shock_definition: dict[str, Any],
    methodology_source_name: str | None = None,
    methodology_source_url: str | None = None,
    observed_at: str | None = None,
    provenance_level: str = "provisional",
    confidence_label: str = "medium",
    notes: str | None = None,
) -> dict[str, Any]:
    ensure_policy_assumption_tables(conn)
    conn.execute(
        """
        INSERT INTO stress_methodology_registry (
          scenario_key, shock_definition_json, methodology_source_name, methodology_source_url,
          observed_at, provenance_level, confidence_label, notes
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(scenario_key) DO UPDATE SET
          shock_definition_json=excluded.shock_definition_json,
          methodology_source_name=excluded.methodology_source_name,
          methodology_source_url=excluded.methodology_source_url,
          observed_at=excluded.observed_at,
          provenance_level=excluded.provenance_level,
          confidence_label=excluded.confidence_label,
          notes=excluded.notes
        """,
        (
            scenario_key,
            json.dumps(shock_definition),
            methodology_source_name,
            methodology_source_url,
            observed_at,
            provenance_level,
            confidence_label,
            notes,
        ),
    )
    conn.commit()
    return next(item for item in list_stress_methodology_registry(conn) if str(item.get("scenario_key")) == scenario_key)


def bootstrap_policy_reference_pack(conn: sqlite3.Connection) -> dict[str, Any]:
    ensure_policy_assumption_tables(conn)
    from app.services.brief_benchmark import DEFAULT_CONTEXT_KEY, get_current_benchmark, seed_default_benchmark_definitions
    from app.services.cma_engine import list_current_cma_assumptions, seed_default_cma_assumptions
    from app.services.stress_engine import seed_default_stress_methodologies

    seed_default_cma_assumptions(conn)
    seed_default_benchmark_definitions(conn)
    seed_default_stress_methodologies(conn)

    cma_seeded = 0
    for item in list_current_cma_assumptions(conn):
        assumption_key = f"cma::{str(item.get('sleeve_key') or '')}"
        current = get_policy_current(conn, assumption_family="cma", assumption_key=assumption_key)
        if current and str(current.get("provenance_level") or "") != "developer_seed":
            continue
        upsert_policy_observation(
            conn,
            assumption_key=assumption_key,
            assumption_family="cma",
            value={
                "expected_return_min": item.get("expected_return_min"),
                "expected_return_max": item.get("expected_return_max"),
                "worst_year_loss_min": item.get("worst_year_loss_min"),
                "worst_year_loss_max": item.get("worst_year_loss_max"),
                "scenario_notes": item.get("scenario_notes"),
            },
            source_name="Internal policy reference pack",
            source_url=f"https://policy.example/internal/cma/{assumption_key}",
            observed_at=str(item.get("assumption_date") or datetime.now(UTC).date().isoformat()),
            methodology_note="Bootstrap internal policy reference derived from the governed Daily Brief sleeve assumption template. Replace with cited external methodology or approved CIO policy source before enabling guidance mode.",
            provenance_level="provisional",
            confidence_label=str(item.get("confidence_label") or "medium"),
            overwrite_priority=5,
            is_current=True,
        )
        cma_seeded += 1

    benchmark_seeded = 0
    benchmark = get_current_benchmark(conn, context_key=DEFAULT_CONTEXT_KEY)
    for component in list_benchmark_policy_profiles(conn, profile_key=DEFAULT_CONTEXT_KEY):
        if str(component.get("provenance_level") or "") != "developer_seed":
            continue
        upsert_benchmark_policy_profile(
            conn,
            profile_key=DEFAULT_CONTEXT_KEY,
            sleeve_key=str(component.get("sleeve_key") or ""),
            target_weight=float(component.get("target_weight") or 0.0),
            min_weight=float(component.get("min_weight") or 0.0) if component.get("min_weight") is not None else None,
            max_weight=float(component.get("max_weight") or 0.0) if component.get("max_weight") is not None else None,
            source_name="Internal policy reference pack",
            source_url=f"https://policy.example/internal/benchmark/{DEFAULT_CONTEXT_KEY}",
            methodology_note="Bootstrap internal benchmark reference synced from the governed Daily Brief benchmark profile. Replace with approved policy benchmark methodology before guidance mode.",
            observed_at=str(benchmark.get("assumption_date") or datetime.now(UTC).date().isoformat()),
            provenance_level="provisional",
            confidence_label="medium",
            is_current=True,
        )
        benchmark_seeded += 1

    stress_seeded = 0
    for item in list_stress_methodology_registry(conn):
        if str(item.get("provenance_level") or "") != "developer_seed":
            continue
        upsert_stress_methodology(
            conn,
            scenario_key=str(item.get("scenario_key") or ""),
            shock_definition=dict(item.get("shock_definition") or {}),
            methodology_source_name="Internal policy reference pack",
            methodology_source_url=f"https://policy.example/internal/stress/{item.get('scenario_key')}",
            observed_at=str(item.get("observed_at") or datetime.now(UTC).date().isoformat()),
            provenance_level="provisional",
            confidence_label="medium",
            notes="Bootstrap internal stress methodology reference. Replace with approved stress policy note or external methodology source before guidance mode.",
        )
        stress_seeded += 1

    return {
        "status": "ok",
        "seeded": {
            "cma": cma_seeded,
            "benchmark_profiles": benchmark_seeded,
            "stress_methodologies": stress_seeded,
        },
        "guidance_ready": False,
        "note": "Bootstrap created provisional internal policy references. Guidance mode remains blocked until policy sources are explicitly approved or replaced with cited sources.",
    }


def build_policy_truth_state(
    *,
    provenance_level: str | None,
    observed_at: str | None,
    stale_after_days: int = 365,
) -> str:
    provenance = str(provenance_level or "developer_seed")
    if provenance in {"blocked"}:
        return "blocked"
    if provenance in {"developer_seed"}:
        return "developer_seed"
    if provenance in {"provisional"}:
        return "provisional"
    if observed_at:
        try:
            observed = datetime.fromisoformat(str(observed_at).replace("Z", "+00:00"))
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=UTC)
            age_days = max(0, (datetime.now(UTC) - observed).days)
            if age_days > stale_after_days:
                return "stale"
        except (ValueError, TypeError):
            return "stale"
    return "sourced"


def policy_render_labels(truth_state: str) -> list[str]:
    mapping = {
        "sourced": ["review-grade policy support"],
        "provisional": ["reference only", "provisional methodology"],
        "developer_seed": ["developer seed", "not for allocation decisions"],
        "stale": ["stale", "reference only"],
        "blocked": ["blocked", "not for allocation decisions"],
    }
    return mapping.get(str(truth_state or "blocked"), ["blocked"])


def build_policy_source_record(
    *,
    source_id: str,
    url: str | None,
    publisher: str,
    topic: str,
    credibility_tier: str,
    source_type: str = "web",
) -> SourceRecord | None:
    if not url:
        return None
    return SourceRecord(
        source_id=source_id,
        url=url,
        publisher=publisher,
        retrieved_at=datetime.now(UTC),
        topic=topic,
        credibility_tier=credibility_tier,  # type: ignore[arg-type]
        raw_hash=uuid.uuid5(uuid.NAMESPACE_URL, f"{source_id}|{url}").hex,
        source_type=source_type,  # type: ignore[arg-type]
    )


def build_policy_citation(
    *,
    source_id: str,
    url: str | None,
    importance: str,
    observed_at: str | None,
    provenance_level: str,
    confidence_label: str,
    methodology_class: str | None = None,
) -> Citation | None:
    if not url:
        return None
    suffix = (
        f"; provenance={provenance_level}; confidence={confidence_label}"
        + (f"; methodology={methodology_class}" if methodology_class else "")
    )
    return Citation(
        url=url,
        source_id=source_id,
        retrieved_at=datetime.now(UTC),
        importance=f"{importance}{suffix}",
        observed_at=observed_at,
        lag_days=None,
        lag_class=None,
        lag_cause=None,
    )


def classify_policy_trust_banner(policy_states: list[str]) -> dict[str, Any]:
    states = [str(item or "blocked") for item in policy_states]
    if not states:
        return {
            "trust_level": "market_monitoring_only",
            "label": "Market monitoring only",
            "guidance_ready": False,
        }
    if any(state in {"developer_seed", "blocked"} for state in states):
        return {
            "trust_level": "market_monitoring_only",
            "label": "Market monitoring only",
            "guidance_ready": False,
        }
    if any(state in {"provisional", "stale"} for state in states):
        return {
            "trust_level": "review_grade_policy_support",
            "label": "Review-grade policy support",
            "guidance_ready": False,
        }
    return {
        "trust_level": "portfolio_guidance_ready",
        "label": "Portfolio-guidance ready",
        "guidance_ready": True,
    }


def import_policy_pack(conn: sqlite3.Connection, payload: dict[str, Any]) -> dict[str, Any]:
    ensure_policy_assumption_tables(conn)
    from app.services.regime_methodology import upsert_regime_methodology

    assumptions = list(payload.get("assumptions") or [])
    benchmark_profiles = list(payload.get("benchmark_profiles") or [])
    stress_methodologies = list(payload.get("stress_methodologies") or [])
    regime_methodology = list(payload.get("regime_methodology") or [])

    counts = {"assumptions": 0, "benchmark_profiles": 0, "stress_methodologies": 0, "regime_methodology": 0}

    for raw in assumptions:
        item = dict(raw or {})
        upsert_policy_observation(
            conn,
            assumption_key=str(item.get("assumption_key") or ""),
            assumption_family=str(item.get("assumption_family") or ""),
            value=dict(item.get("value_json") or {}),
            source_name=item.get("source_name"),
            source_url=item.get("source_url"),
            observed_at=item.get("observed_at"),
            methodology_note=item.get("methodology_note"),
            provenance_level=str(item.get("provenance_level") or "provisional"),
            confidence_label=str(item.get("confidence_label") or "medium"),
            overwrite_priority=int(item.get("overwrite_priority") or 0),
            is_current=bool(item.get("is_current", True)),
        )
        counts["assumptions"] += 1

    for raw in benchmark_profiles:
        item = dict(raw or {})
        upsert_benchmark_policy_profile(
            conn,
            profile_key=str(item.get("profile_key") or ""),
            sleeve_key=str(item.get("sleeve_key") or ""),
            target_weight=float(item.get("target_weight") or 0.0),
            min_weight=float(item["min_weight"]) if item.get("min_weight") is not None else None,
            max_weight=float(item["max_weight"]) if item.get("max_weight") is not None else None,
            source_name=item.get("source_name"),
            source_url=item.get("source_url"),
            methodology_note=item.get("methodology_note"),
            observed_at=item.get("observed_at"),
            provenance_level=str(item.get("provenance_level") or "provisional"),
            confidence_label=str(item.get("confidence_label") or "medium"),
            is_current=bool(item.get("is_current", True)),
        )
        counts["benchmark_profiles"] += 1

    for raw in stress_methodologies:
        item = dict(raw or {})
        upsert_stress_methodology(
            conn,
            scenario_key=str(item.get("scenario_key") or ""),
            shock_definition=dict(item.get("shock_definition_json") or item.get("shock_definition") or {}),
            methodology_source_name=item.get("methodology_source_name"),
            methodology_source_url=item.get("methodology_source_url"),
            observed_at=item.get("observed_at"),
            provenance_level=str(item.get("provenance_level") or "provisional"),
            confidence_label=str(item.get("confidence_label") or "medium"),
            notes=item.get("notes"),
        )
        counts["stress_methodologies"] += 1

    for raw in regime_methodology:
        item = dict(raw or {})
        upsert_regime_methodology(
            conn,
            metric_key=str(item.get("metric_key") or ""),
            watch_threshold=float(item["watch_threshold"]) if item.get("watch_threshold") is not None else None,
            alert_threshold=float(item["alert_threshold"]) if item.get("alert_threshold") is not None else None,
            methodology_note=str(item.get("methodology_note") or "Imported regime methodology."),
            threshold_kind=str(item.get("threshold_kind") or "observational"),
            source_name=item.get("source_name"),
            source_url=item.get("source_url"),
            observed_at=item.get("observed_at"),
            provenance_level=str(item.get("provenance_level") or "provisional"),
            confidence_label=str(item.get("confidence_label") or "medium"),
            methodology_version=str(item.get("methodology_version") or ""),
        )
        counts["regime_methodology"] += 1

    return {"status": "ok", "counts": counts}
