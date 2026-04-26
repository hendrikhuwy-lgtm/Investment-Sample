from __future__ import annotations

import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
import hashlib
import json
from typing import Any

from app.services.ips import default_ips_profile
from app.services.blueprint_investment_quality import ensure_quality_tables, persist_quality_scores
from app.services.blueprint_recommendations import (
    build_recommendation_events,
    ensure_recommendation_tables,
    persist_recommendation_events,
    persist_sleeve_recommendations,
)


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def ensure_blueprint_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprints (
          blueprint_id TEXT PRIMARY KEY,
          name TEXT NOT NULL,
          version TEXT NOT NULL,
          base_currency TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'active',
          benchmark_reference TEXT,
          rebalance_frequency TEXT,
          rebalance_logic TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_sleeves (
          sleeve_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          sleeve_key TEXT NOT NULL,
          sleeve_name TEXT NOT NULL,
          target_weight REAL NOT NULL,
          min_band REAL NOT NULL,
          max_band REAL NOT NULL,
          core_satellite TEXT NOT NULL DEFAULT 'core',
          benchmark_reference TEXT,
          notes TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_sleeves_key
        ON blueprint_sleeves (blueprint_id, sleeve_key)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_benchmarks (
          benchmark_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          sleeve_key TEXT,
          benchmark_name TEXT NOT NULL,
          benchmark_symbol TEXT,
          notes TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_mapping_rules (
          rule_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          match_type TEXT NOT NULL,
          match_value TEXT NOT NULL,
          target_sleeve TEXT NOT NULL,
          confidence REAL NOT NULL DEFAULT 1.0,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_snapshots (
          snapshot_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          actor_id TEXT NOT NULL,
          note TEXT,
          blueprint_hash TEXT NOT NULL,
          portfolio_settings_hash TEXT NOT NULL,
          candidate_list_hash TEXT NOT NULL,
          sleeve_settings_hash TEXT NOT NULL,
          ips_version TEXT,
          governance_summary_json TEXT,
          market_state_snapshot_json TEXT,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_snapshots_blueprint_created
        ON blueprint_snapshots (blueprint_id, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_versions (
          version_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          version_label TEXT NOT NULL,
          is_active INTEGER NOT NULL DEFAULT 0,
          archived_at TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    for column, ddl in (
        ("ips_version", "TEXT"),
        ("governance_summary_json", "TEXT"),
        ("market_state_snapshot_json", "TEXT"),
    ):
        _ensure_column(conn, "blueprint_snapshots", column, ddl)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sub_sleeve_mappings (
          sub_sleeve_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          parent_sleeve_key TEXT NOT NULL,
          child_sleeve_key TEXT NOT NULL,
          child_sleeve_name TEXT NOT NULL,
          target_weight REAL NOT NULL,
          min_band REAL NOT NULL,
          max_band REAL NOT NULL,
          benchmark_reference TEXT,
          region TEXT,
          sector TEXT,
          factor_hint TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_decision_artifacts (
          artifact_id TEXT PRIMARY KEY,
          snapshot_id TEXT NOT NULL,
          sleeve_key TEXT,
          candidate_symbol TEXT,
          artifact_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_decision_artifacts_snapshot
        ON blueprint_decision_artifacts (snapshot_id, artifact_type, sleeve_key, candidate_symbol)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_runtime_cycles (
          cycle_id TEXT PRIMARY KEY,
          blueprint_id TEXT NOT NULL,
          refresh_run_id TEXT,
          evaluation_mode TEXT NOT NULL,
          payload_hash TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          generated_at TEXT,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_runtime_cycles_created
        ON blueprint_runtime_cycles (created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_runtime_cycles_identity
        ON blueprint_runtime_cycles (
          payload_hash,
          COALESCE(refresh_run_id, ''),
          evaluation_mode,
          COALESCE(generated_at, '')
        )
        """
    )
    _ensure_column(conn, "blueprint_runtime_cycles", "payload_json", "TEXT NOT NULL DEFAULT '{}'")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_runtime_cycle_artifacts (
          artifact_id TEXT PRIMARY KEY,
          cycle_id TEXT NOT NULL,
          sleeve_key TEXT,
          candidate_symbol TEXT,
          artifact_type TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_runtime_cycle_artifacts_cycle
        ON blueprint_runtime_cycle_artifacts (cycle_id, artifact_type, sleeve_key, candidate_symbol)
        """
    )
    conn.commit()


def persist_blueprint_decision_artifacts(
    conn: sqlite3.Connection,
    *,
    snapshot_id: str,
    blueprint_payload: dict[str, Any],
) -> None:
    ensure_blueprint_tables(conn)
    conn.execute("DELETE FROM blueprint_decision_artifacts WHERE snapshot_id = ?", (snapshot_id,))
    now = _now_iso()
    meta = dict(blueprint_payload.get("blueprint_meta") or {})
    payload_level_artifacts = {
        "deliverable_candidates": meta.get("deliverable_candidates") or {},
        "deliverable_candidates_diff": meta.get("deliverable_candidates_diff") or {},
        "candidate_universe": meta.get("candidate_universe") or {},
        "candidate_universe_diff": meta.get("candidate_universe_diff") or {},
        "portfolio_governance": meta.get("portfolio_governance") or {},
        "architecture": meta.get("architecture") or {},
    }
    for artifact_type, payload in payload_level_artifacts.items():
        conn.execute(
            """
            INSERT INTO blueprint_decision_artifacts (
              artifact_id, snapshot_id, sleeve_key, candidate_symbol, artifact_type, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"bda_{uuid.uuid4().hex[:12]}",
                snapshot_id,
                None,
                None,
                artifact_type,
                _stable_json(payload),
                now,
            ),
        )

    for sleeve in list(blueprint_payload.get("sleeves") or []):
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        recommendation = dict(sleeve.get("recommendation") or {})
        if recommendation:
            conn.execute(
                """
                INSERT INTO blueprint_decision_artifacts (
                  artifact_id, snapshot_id, sleeve_key, candidate_symbol, artifact_type, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"bda_{uuid.uuid4().hex[:12]}",
                    snapshot_id,
                    sleeve_key,
                    None,
                    "sleeve_recommendation_summary",
                    _stable_json(recommendation),
                    now,
                ),
            )
        for candidate in list(sleeve.get("candidates") or []):
            symbol = str(candidate.get("symbol") or "").upper()
            artifacts = {
                "decision_record": dict(candidate.get("decision_record") or {}),
                "approval_memo": dict(candidate.get("approval_memo") or {}),
                "rejection_memo": dict(candidate.get("rejection_memo") or {}),
                "recommendation_diff": dict(candidate.get("recommendation_diff") or {}),
                "candidate_record": dict(candidate.get("candidate_record") or {}),
                "evidence_pack": dict(candidate.get("evidence_pack") or {}),
                "source_integrity_result": dict(candidate.get("source_integrity_result") or {}),
                "gate_result": dict(candidate.get("gate_result") or {}),
                "review_intensity_decision": dict(candidate.get("review_intensity_decision") or {}),
                "universal_review_result": dict(candidate.get("universal_review_result") or {}),
                "deep_review_result": dict(candidate.get("deep_review_result") or {}),
                "scoring_result": dict(candidate.get("scoring_result") or {}),
                "current_holding_record": dict(candidate.get("current_holding_record") or {}),
                "recommendation_result": dict(candidate.get("recommendation_result") or {}),
                "decision_completeness_status": dict(candidate.get("decision_completeness_status") or {}),
                "portfolio_completeness_status": dict(candidate.get("portfolio_completeness_status") or {}),
                "investor_recommendation_status": dict(candidate.get("investor_recommendation_status") or {}),
                "benchmark_support_status": dict(candidate.get("benchmark_support_status") or {}),
                "gate_summary": dict(candidate.get("gate_summary") or {}),
                "base_promotion_state": candidate.get("base_promotion_state"),
                "lens_assessment": dict(candidate.get("lens_assessment") or {}),
                "lens_fusion_result": dict(candidate.get("lens_fusion_result") or {}),
                "decision_thesis": dict(candidate.get("decision_thesis") or {}),
                "forecast_visual_model": dict(candidate.get("forecast_visual_model") or {}),
                "forecast_defensibility_status": dict(candidate.get("forecast_defensibility_status") or {}),
                "tax_assumption_status": dict(candidate.get("tax_assumption_status") or {}),
                "cost_realism_summary": dict(candidate.get("cost_realism_summary") or {}),
                "portfolio_consequence_summary": dict(candidate.get("portfolio_consequence_summary") or {}),
                "decision_change_set": dict(candidate.get("decision_change_set") or {}),
                "supporting_metadata_summary": dict(candidate.get("supporting_metadata_summary") or {}),
                "memo_result": dict(candidate.get("memo_result") or {}),
                "baseline_reference": dict(candidate.get("baseline_reference") or {}),
                "audit_log_entries": list(candidate.get("audit_log_entries") or []),
            }
            for artifact_type, payload in artifacts.items():
                if not payload:
                    continue
                conn.execute(
                    """
                    INSERT INTO blueprint_decision_artifacts (
                      artifact_id, snapshot_id, sleeve_key, candidate_symbol, artifact_type, payload_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"bda_{uuid.uuid4().hex[:12]}",
                        snapshot_id,
                        sleeve_key,
                        symbol,
                        artifact_type,
                        _stable_json(payload),
                        now,
                    ),
                )
    conn.commit()


def persist_blueprint_runtime_cycle(
    conn: sqlite3.Connection,
    *,
    blueprint_payload: dict[str, Any],
) -> dict[str, Any]:
    ensure_blueprint_tables(conn)
    blueprint = get_current_blueprint(conn)
    blueprint_meta = dict(blueprint_payload.get("blueprint_meta") or {})
    payload_text = _stable_json(blueprint_payload)
    payload_hash = _hash_text(payload_text)
    existing = conn.execute(
        """
        SELECT cycle_id, blueprint_id, refresh_run_id, evaluation_mode, payload_hash, generated_at, created_at
        , payload_json
        FROM blueprint_runtime_cycles
        WHERE payload_hash = ?
          AND COALESCE(refresh_run_id, '') = COALESCE(?, '')
          AND evaluation_mode = ?
          AND COALESCE(generated_at, '') = COALESCE(?, '')
        LIMIT 1
        """,
        (
            payload_hash,
            str(dict(blueprint_meta.get("refresh_monitor") or {}).get("run_id") or "") or None,
            str(blueprint_meta.get("evaluation_mode") or "design_only"),
            str(blueprint_meta.get("generated_at") or "") or None,
        ),
    ).fetchone()
    if existing is not None:
        cycle_id = str(existing["cycle_id"])
        refreshed_created_at = _now_iso()
        conn.execute(
            """
            UPDATE blueprint_runtime_cycles
            SET payload_json = ?, generated_at = ?, created_at = ?
            WHERE cycle_id = ?
            """,
            (
                payload_text,
                str(blueprint_meta.get("generated_at") or "") or None,
                refreshed_created_at,
                cycle_id,
            ),
        )
        conn.execute("DELETE FROM blueprint_runtime_cycle_artifacts WHERE cycle_id = ?", (cycle_id,))
        conn.execute("DELETE FROM candidate_quality_scores WHERE snapshot_id = ?", (cycle_id,))
        conn.execute("DELETE FROM sleeve_recommendations WHERE snapshot_id = ?", (cycle_id,))
        conn.execute("DELETE FROM recommendation_events WHERE snapshot_id = ?", (cycle_id,))
        _persist_blueprint_runtime_cycle_artifacts(
            conn,
            cycle_id=cycle_id,
            blueprint_payload=blueprint_payload,
        )
        all_scores: list[dict[str, Any]] = []
        sleeve_recommendations: list[dict[str, Any]] = []
        for sleeve in list(blueprint_payload.get("sleeves") or []):
            recommendation = dict(sleeve.get("recommendation") or {})
            if recommendation:
                sleeve_recommendations.append(recommendation)
            for candidate in list(sleeve.get("candidates") or []):
                quality = dict(candidate.get("investment_quality") or {})
                if quality:
                    all_scores.append(quality)
        if all_scores:
            persist_quality_scores(conn, snapshot_id=cycle_id, scores=all_scores)
        if sleeve_recommendations:
            persist_sleeve_recommendations(conn, snapshot_id=cycle_id, summaries=sleeve_recommendations)
        conn.commit()
        refreshed = dict(existing)
        refreshed["payload_json"] = payload_text
        refreshed["generated_at"] = str(blueprint_meta.get("generated_at") or "") or None
        refreshed["created_at"] = refreshed_created_at
        return refreshed

    cycle = {
        "cycle_id": f"bpc_{uuid.uuid4().hex[:12]}",
        "blueprint_id": str(blueprint["blueprint_id"]),
        "refresh_run_id": str(dict(blueprint_meta.get("refresh_monitor") or {}).get("run_id") or "") or None,
        "evaluation_mode": str(blueprint_meta.get("evaluation_mode") or "design_only"),
        "payload_hash": payload_hash,
        "payload_json": payload_text,
        "generated_at": str(blueprint_meta.get("generated_at") or "") or None,
        "created_at": _now_iso(),
    }
    conn.execute(
        """
        INSERT INTO blueprint_runtime_cycles (
          cycle_id, blueprint_id, refresh_run_id, evaluation_mode, payload_hash, payload_json, generated_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            cycle["cycle_id"],
            cycle["blueprint_id"],
            cycle["refresh_run_id"],
            cycle["evaluation_mode"],
            cycle["payload_hash"],
            cycle["payload_json"],
            cycle["generated_at"],
            cycle["created_at"],
        ),
    )
    _persist_blueprint_runtime_cycle_artifacts(
        conn,
        cycle_id=str(cycle["cycle_id"]),
        blueprint_payload=blueprint_payload,
    )
    all_scores: list[dict[str, Any]] = []
    sleeve_recommendations: list[dict[str, Any]] = []
    for sleeve in list(blueprint_payload.get("sleeves") or []):
        recommendation = dict(sleeve.get("recommendation") or {})
        if recommendation:
            sleeve_recommendations.append(recommendation)
        for candidate in list(sleeve.get("candidates") or []):
            quality = dict(candidate.get("investment_quality") or {})
            if quality:
                all_scores.append(quality)
    if all_scores:
        persist_quality_scores(conn, snapshot_id=str(cycle["cycle_id"]), scores=all_scores)
    if sleeve_recommendations:
        persist_sleeve_recommendations(conn, snapshot_id=str(cycle["cycle_id"]), summaries=sleeve_recommendations)
    previous = conn.execute(
        """
        SELECT cycle_id
        FROM blueprint_runtime_cycles
        WHERE blueprint_id = ? AND cycle_id != ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (cycle["blueprint_id"], cycle["cycle_id"]),
    ).fetchone()
    if previous is not None:
        previous_cycle = get_blueprint_runtime_cycle(conn, str(previous["cycle_id"]))
        try:
            events = build_recommendation_events(
                previous_cycle.get("payload") if previous_cycle else {},
                blueprint_payload,
            )
            if events:
                persist_recommendation_events(conn, snapshot_id=str(cycle["cycle_id"]), events=events)
        except Exception:
            pass
    conn.commit()
    return cycle


def _persist_blueprint_runtime_cycle_artifacts(
    conn: sqlite3.Connection,
    *,
    cycle_id: str,
    blueprint_payload: dict[str, Any],
) -> None:
    now = _now_iso()
    meta = dict(blueprint_payload.get("blueprint_meta") or {})
    payload_level_artifacts = {
        "deliverable_candidates": meta.get("deliverable_candidates") or {},
        "deliverable_candidates_diff": meta.get("deliverable_candidates_diff") or {},
        "candidate_universe": meta.get("candidate_universe") or {},
        "candidate_universe_diff": meta.get("candidate_universe_diff") or {},
        "recommendation_summary": meta.get("recommendation_summary") or {},
        "portfolio_governance": meta.get("portfolio_governance") or {},
        "architecture": meta.get("architecture") or {},
    }
    for artifact_type, payload in payload_level_artifacts.items():
        if not payload:
            continue
        conn.execute(
            """
            INSERT INTO blueprint_runtime_cycle_artifacts (
              artifact_id, cycle_id, sleeve_key, candidate_symbol, artifact_type, payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"brca_{uuid.uuid4().hex[:12]}",
                cycle_id,
                None,
                None,
                artifact_type,
                _stable_json(payload),
                now,
            ),
        )
    for sleeve in list(blueprint_payload.get("sleeves") or []):
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        recommendation = dict(sleeve.get("recommendation") or {})
        if recommendation:
            conn.execute(
                """
                INSERT INTO blueprint_runtime_cycle_artifacts (
                  artifact_id, cycle_id, sleeve_key, candidate_symbol, artifact_type, payload_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"brca_{uuid.uuid4().hex[:12]}",
                    cycle_id,
                    sleeve_key,
                    None,
                    "sleeve_recommendation_summary",
                    _stable_json(recommendation),
                    now,
                ),
            )
        for candidate in list(sleeve.get("candidates") or []):
            symbol = str(candidate.get("symbol") or "").upper()
            artifacts = {
                "decision_record": dict(candidate.get("decision_record") or {}),
                "approval_memo": dict(candidate.get("approval_memo") or {}),
                "rejection_memo": dict(candidate.get("rejection_memo") or {}),
                "recommendation_diff": dict(candidate.get("recommendation_diff") or {}),
                "candidate_record": dict(candidate.get("candidate_record") or {}),
                "evidence_pack": dict(candidate.get("evidence_pack") or {}),
                "source_integrity_result": dict(candidate.get("source_integrity_result") or {}),
                "gate_result": dict(candidate.get("gate_result") or {}),
                "review_intensity_decision": dict(candidate.get("review_intensity_decision") or {}),
                "universal_review_result": dict(candidate.get("universal_review_result") or {}),
                "deep_review_result": dict(candidate.get("deep_review_result") or {}),
                "scoring_result": dict(candidate.get("scoring_result") or {}),
                "current_holding_record": dict(candidate.get("current_holding_record") or {}),
                "recommendation_result": dict(candidate.get("recommendation_result") or {}),
                "decision_completeness_status": dict(candidate.get("decision_completeness_status") or {}),
                "portfolio_completeness_status": dict(candidate.get("portfolio_completeness_status") or {}),
                "investor_recommendation_status": dict(candidate.get("investor_recommendation_status") or {}),
                "benchmark_support_status": dict(candidate.get("benchmark_support_status") or {}),
                "gate_summary": dict(candidate.get("gate_summary") or {}),
                "base_promotion_state": candidate.get("base_promotion_state"),
                "lens_assessment": dict(candidate.get("lens_assessment") or {}),
                "lens_fusion_result": dict(candidate.get("lens_fusion_result") or {}),
                "decision_thesis": dict(candidate.get("decision_thesis") or {}),
                "forecast_visual_model": dict(candidate.get("forecast_visual_model") or {}),
                "forecast_defensibility_status": dict(candidate.get("forecast_defensibility_status") or {}),
                "tax_assumption_status": dict(candidate.get("tax_assumption_status") or {}),
                "cost_realism_summary": dict(candidate.get("cost_realism_summary") or {}),
                "portfolio_consequence_summary": dict(candidate.get("portfolio_consequence_summary") or {}),
                "decision_change_set": dict(candidate.get("decision_change_set") or {}),
                "supporting_metadata_summary": dict(candidate.get("supporting_metadata_summary") or {}),
                "memo_result": dict(candidate.get("memo_result") or {}),
                "baseline_reference": dict(candidate.get("baseline_reference") or {}),
                "audit_log_entries": list(candidate.get("audit_log_entries") or []),
            }
            for artifact_type, payload in artifacts.items():
                if not payload:
                    continue
                conn.execute(
                    """
                    INSERT INTO blueprint_runtime_cycle_artifacts (
                      artifact_id, cycle_id, sleeve_key, candidate_symbol, artifact_type, payload_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        f"brca_{uuid.uuid4().hex[:12]}",
                        cycle_id,
                        sleeve_key,
                        symbol,
                        artifact_type,
                        _stable_json(payload),
                        now,
                    ),
                )


def list_blueprint_runtime_cycles(conn: sqlite3.Connection, *, limit: int = 25) -> list[dict[str, Any]]:
    ensure_blueprint_tables(conn)
    rows = conn.execute(
        """
        SELECT cycle_id, blueprint_id, refresh_run_id, evaluation_mode, payload_hash, generated_at, created_at
        , payload_json
        FROM blueprint_runtime_cycles
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (max(1, min(limit, 100)),),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        item.pop("payload_json", None)
        items.append(item)
    return items


def list_blueprint_runtime_cycle_artifacts(
    conn: sqlite3.Connection,
    *,
    cycle_id: str,
) -> list[dict[str, Any]]:
    ensure_blueprint_tables(conn)
    rows = conn.execute(
        """
        SELECT artifact_id, cycle_id, sleeve_key, candidate_symbol, artifact_type, payload_json, created_at
        FROM blueprint_runtime_cycle_artifacts
        WHERE cycle_id = ?
        ORDER BY artifact_type, sleeve_key, candidate_symbol, created_at
        """,
        (cycle_id,),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        try:
            item["payload"] = json.loads(str(item.pop("payload_json") or "{}"))
        except Exception:
            item["payload"] = {}
        items.append(item)
    return items


def get_blueprint_runtime_cycle(conn: sqlite3.Connection, cycle_id: str) -> dict[str, Any] | None:
    ensure_blueprint_tables(conn)
    row = conn.execute(
        """
        SELECT cycle_id, blueprint_id, refresh_run_id, evaluation_mode, payload_hash, generated_at, created_at
        , payload_json
        FROM blueprint_runtime_cycles
        WHERE cycle_id = ?
        LIMIT 1
        """,
        (cycle_id,),
    ).fetchone()
    if row is None:
        return None
    out = dict(row)
    try:
        out["payload"] = json.loads(str(out.pop("payload_json") or "{}"))
    except Exception:
        out["payload"] = {}
    out["artifacts"] = list_blueprint_runtime_cycle_artifacts(conn, cycle_id=cycle_id)
    return out


def prune_blueprint_runtime_cycles(
    conn: sqlite3.Connection,
    *,
    retention_days: int = 3,
    min_keep: int = 72,
) -> int:
    """Delete cycles older than retention_days, always keeping the newest min_keep cycles."""
    ensure_blueprint_tables(conn)
    cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
    # Find the cycle_id of the min_keep-th newest cycle (safety floor)
    floor_row = conn.execute(
        """
        SELECT created_at FROM blueprint_runtime_cycles
        ORDER BY created_at DESC
        LIMIT 1 OFFSET ?
        """,
        (min_keep - 1,),
    ).fetchone()
    floor_ts = floor_row[0] if floor_row else None

    # Build the set of cycle_ids eligible for deletion
    if floor_ts is not None:
        rows = conn.execute(
            """
            SELECT cycle_id FROM blueprint_runtime_cycles
            WHERE created_at < ? AND created_at < ?
            """,
            (cutoff, floor_ts),
        ).fetchall()
    else:
        # Fewer than min_keep cycles total — nothing to delete
        return 0

    cycle_ids = [r[0] for r in rows]
    if not cycle_ids:
        return 0

    placeholders = ",".join("?" * len(cycle_ids))
    conn.execute(
        f"DELETE FROM blueprint_runtime_cycle_artifacts WHERE cycle_id IN ({placeholders})",
        cycle_ids,
    )
    conn.execute(
        f"DELETE FROM blueprint_runtime_cycles WHERE cycle_id IN ({placeholders})",
        cycle_ids,
    )
    conn.commit()
    return len(cycle_ids)


def list_blueprint_decision_artifacts(
    conn: sqlite3.Connection,
    *,
    snapshot_id: str,
) -> list[dict[str, Any]]:
    ensure_blueprint_tables(conn)
    rows = conn.execute(
        """
        SELECT artifact_id, snapshot_id, sleeve_key, candidate_symbol, artifact_type, payload_json, created_at
        FROM blueprint_decision_artifacts
        WHERE snapshot_id = ?
        ORDER BY artifact_type, sleeve_key, candidate_symbol, created_at
        """,
        (snapshot_id,),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        try:
            item["payload"] = json.loads(str(item.pop("payload_json") or "{}"))
        except Exception:
            item["payload"] = {}
        items.append(item)
    return items


def ensure_default_blueprint(conn: sqlite3.Connection) -> str:
    ensure_blueprint_tables(conn)
    row = conn.execute(
        "SELECT blueprint_id FROM blueprints WHERE status = 'active' ORDER BY updated_at DESC LIMIT 1"
    ).fetchone()
    if row is not None:
        return str(row["blueprint_id"])

    profile = default_ips_profile()
    blueprint_id = "blueprint_primary"
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO blueprints (
          blueprint_id, name, version, base_currency, status, benchmark_reference,
          rebalance_frequency, rebalance_logic, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            blueprint_id,
            "Primary Blueprint",
            "1.0",
            profile.constraints.base_currency,
            "active",
            "policy_reference",
            profile.rebalance_frequency,
            "Rebalance when sleeve breaches band or cash flow permits.",
            now,
            now,
        ),
    )
    conn.execute(
        """
        INSERT INTO blueprint_versions (
          version_id, blueprint_id, version_label, is_active, archived_at, created_at, updated_at
        ) VALUES (?, ?, ?, 1, NULL, ?, ?)
        """,
        (
            f"blueprint_version_{uuid.uuid4().hex[:12]}",
            blueprint_id,
            "1.0",
            now,
            now,
        ),
    )
    for allocation in profile.allocations:
        conn.execute(
            """
            INSERT INTO blueprint_sleeves (
              sleeve_id, blueprint_id, sleeve_key, sleeve_name, target_weight, min_band,
              max_band, core_satellite, benchmark_reference, notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"sleeve_{uuid.uuid4().hex[:12]}",
                blueprint_id,
                allocation.sleeve,
                allocation.sleeve.replace("_", " ").title(),
                float(allocation.target_weight),
                float(allocation.min_band),
                float(allocation.max_band),
                "core" if allocation.sleeve in {"global_equity", "ig_bond", "cash"} else "satellite",
                f"{allocation.sleeve}_benchmark",
                "Seeded from IPS profile.",
                now,
                now,
            ),
        )
        conn.execute(
            """
            INSERT INTO blueprint_benchmarks (
              benchmark_id, blueprint_id, sleeve_key, benchmark_name, benchmark_symbol,
              notes, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"benchmark_{uuid.uuid4().hex[:12]}",
                blueprint_id,
                allocation.sleeve,
                allocation.sleeve.replace("_", " ").title(),
                None,
                "Seeded from IPS profile.",
                now,
                now,
            ),
        )
    conn.commit()
    return blueprint_id


def list_blueprints(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_blueprint_tables(conn)
    rows = conn.execute(
        """
        SELECT blueprint_id, name, version, base_currency, status, benchmark_reference,
               rebalance_frequency, rebalance_logic, created_at, updated_at
        FROM blueprints
        ORDER BY updated_at DESC
        """
    ).fetchall()
    return [dict(row) for row in rows]


def create_blueprint(
    conn: sqlite3.Connection,
    *,
    name: str,
    base_currency: str = "SGD",
    benchmark_reference: str | None = None,
) -> dict[str, Any]:
    ensure_blueprint_tables(conn)
    blueprint_id = f"blueprint_{uuid.uuid4().hex[:12]}"
    now = _now_iso()
    conn.execute(
        """
        INSERT INTO blueprints (
          blueprint_id, name, version, base_currency, status, benchmark_reference,
          rebalance_frequency, rebalance_logic, created_at, updated_at
        ) VALUES (?, ?, '1.0', ?, 'draft', ?, 'monthly', 'Manual review required', ?, ?)
        """,
        (blueprint_id, name, base_currency, benchmark_reference, now, now),
    )
    conn.execute(
        """
        INSERT INTO blueprint_versions (
          version_id, blueprint_id, version_label, is_active, archived_at, created_at, updated_at
        ) VALUES (?, ?, '1.0', 1, NULL, ?, ?)
        """,
        (f"blueprint_version_{uuid.uuid4().hex[:12]}", blueprint_id, now, now),
    )
    conn.commit()
    return get_blueprint(conn, blueprint_id)


def activate_blueprint(conn: sqlite3.Connection, blueprint_id: str) -> dict[str, Any]:
    ensure_blueprint_tables(conn)
    now = _now_iso()
    conn.execute("UPDATE blueprints SET status = CASE WHEN blueprint_id = ? THEN 'active' ELSE 'archived' END, updated_at = ?", (blueprint_id, now))
    conn.execute("UPDATE blueprint_versions SET is_active = CASE WHEN blueprint_id = ? THEN 1 ELSE 0 END, updated_at = ?", (blueprint_id, now))
    conn.commit()
    return get_blueprint(conn, blueprint_id)


def get_blueprint(conn: sqlite3.Connection, blueprint_id: str) -> dict[str, Any]:
    ensure_blueprint_tables(conn)
    row = conn.execute(
        """
        SELECT blueprint_id, name, version, base_currency, status, benchmark_reference,
               rebalance_frequency, rebalance_logic, created_at, updated_at
        FROM blueprints
        WHERE blueprint_id = ?
        LIMIT 1
        """,
        (blueprint_id,),
    ).fetchone()
    if row is None:
        raise ValueError("Blueprint not found.")
    payload = dict(row)
    payload["sub_sleeves"] = list_sub_sleeves(conn, blueprint_id)
    return payload


def list_sub_sleeves(conn: sqlite3.Connection, blueprint_id: str) -> list[dict[str, Any]]:
    ensure_blueprint_tables(conn)
    rows = conn.execute(
        """
        SELECT sub_sleeve_id, blueprint_id, parent_sleeve_key, child_sleeve_key, child_sleeve_name,
               target_weight, min_band, max_band, benchmark_reference, region, sector, factor_hint, created_at, updated_at
        FROM sub_sleeve_mappings
        WHERE blueprint_id = ?
        ORDER BY parent_sleeve_key ASC, child_sleeve_key ASC
        """,
        (blueprint_id,),
    ).fetchall()
    return [dict(row) for row in rows]


def get_current_blueprint(conn: sqlite3.Connection) -> dict[str, Any]:
    blueprint_id = ensure_default_blueprint(conn)
    header = conn.execute(
        """
        SELECT blueprint_id, name, version, base_currency, status, benchmark_reference,
               rebalance_frequency, rebalance_logic, created_at, updated_at
        FROM blueprints
        WHERE blueprint_id = ?
        """,
        (blueprint_id,),
    ).fetchone()
    sleeves = conn.execute(
        """
        SELECT sleeve_id, sleeve_key, sleeve_name, target_weight, min_band, max_band,
               core_satellite, benchmark_reference, notes, created_at, updated_at
        FROM blueprint_sleeves
        WHERE blueprint_id = ?
        ORDER BY target_weight DESC, sleeve_key ASC
        """,
        (blueprint_id,),
    ).fetchall()
    benchmarks = conn.execute(
        """
        SELECT benchmark_id, sleeve_key, benchmark_name, benchmark_symbol, notes, created_at, updated_at
        FROM blueprint_benchmarks
        WHERE blueprint_id = ?
        ORDER BY sleeve_key ASC
        """,
        (blueprint_id,),
    ).fetchall()
    return {
        "blueprint_id": blueprint_id,
        "name": str(header["name"]),
        "version": str(header["version"]),
        "base_currency": str(header["base_currency"]),
        "status": str(header["status"]),
        "benchmark_reference": header["benchmark_reference"],
        "rebalance_frequency": header["rebalance_frequency"],
        "rebalance_logic": header["rebalance_logic"],
        "created_at": header["created_at"],
        "updated_at": header["updated_at"],
        "sleeves": [
            {
                "sleeve_id": str(row["sleeve_id"]),
                "sleeve_key": str(row["sleeve_key"]),
                "sleeve_name": str(row["sleeve_name"]),
                "target_weight": float(row["target_weight"]),
                "min_band": float(row["min_band"]),
                "max_band": float(row["max_band"]),
                "core_satellite": str(row["core_satellite"]),
                "benchmark_reference": row["benchmark_reference"],
                "notes": row["notes"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in sleeves
        ],
        "benchmarks": [
            {
                "benchmark_id": str(row["benchmark_id"]),
                "sleeve_key": row["sleeve_key"],
                "benchmark_name": str(row["benchmark_name"]),
                "benchmark_symbol": row["benchmark_symbol"],
                "notes": row["notes"],
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
            for row in benchmarks
        ],
        "sub_sleeves": list_sub_sleeves(conn, blueprint_id),
    }


def create_blueprint_snapshot(
    conn: sqlite3.Connection,
    *,
    blueprint_payload: dict[str, Any],
    actor_id: str = "local_actor",
    note: str | None = None,
) -> dict[str, Any]:
    blueprint = get_current_blueprint(conn)
    blueprint_id = str(blueprint["blueprint_id"])
    ensure_blueprint_tables(conn)
    ensure_quality_tables(conn)
    ensure_recommendation_tables(conn)
    payload_text = _stable_json(blueprint_payload)
    blueprint_meta = dict(blueprint_payload.get("blueprint_meta") or {})
    ips_linkage = dict(blueprint_meta.get("ips_linkage") or {})
    governance_summary = {
        "truth_summary": blueprint_meta.get("truth_summary") or {},
        "data_quality": blueprint_meta.get("data_quality") or {},
        "benchmark_registry": blueprint_meta.get("benchmark_registry") or {},
        "recommendation_summary": blueprint_meta.get("recommendation_summary") or {},
    }
    market_state_snapshot = {
        "generated_at": blueprint_meta.get("generated_at"),
        "score_models": blueprint_meta.get("score_models") or [],
        "candidate_source_states": {
            str(candidate.get("symbol") or ""): {
                "source_state": candidate.get("source_state"),
                "freshness_state": candidate.get("freshness_state"),
                "score_mode": candidate.get("score_mode"),
                "benchmark_assignment": candidate.get("benchmark_assignment"),
                "performance_metrics": candidate.get("performance_metrics"),
            }
            for sleeve in list(blueprint_payload.get("sleeves") or [])
            for candidate in list(sleeve.get("candidates") or [])
        },
    }
    snapshot = {
        "snapshot_id": f"bps_{uuid.uuid4().hex[:12]}",
        "blueprint_id": blueprint_id,
        "actor_id": actor_id,
        "note": note.strip() if isinstance(note, str) and note.strip() else None,
        "blueprint_hash": _hash_text(payload_text),
        "portfolio_settings_hash": _hash_text(_stable_json(_portfolio_settings_payload(blueprint_payload))),
        "candidate_list_hash": _hash_text(_stable_json(_candidate_list_payload(blueprint_payload))),
        "sleeve_settings_hash": _hash_text(_stable_json(_sleeve_settings_payload(blueprint_payload))),
        "ips_version": str(ips_linkage.get("ips_version") or ""),
        "governance_summary": governance_summary,
        "market_state_snapshot": market_state_snapshot,
        "payload_json": payload_text,
        "created_at": _now_iso(),
    }
    conn.execute(
        """
        INSERT INTO blueprint_snapshots (
          snapshot_id, blueprint_id, actor_id, note, blueprint_hash, portfolio_settings_hash,
          candidate_list_hash, sleeve_settings_hash, ips_version, governance_summary_json,
          market_state_snapshot_json, payload_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot["snapshot_id"],
            snapshot["blueprint_id"],
            snapshot["actor_id"],
            snapshot["note"],
            snapshot["blueprint_hash"],
            snapshot["portfolio_settings_hash"],
            snapshot["candidate_list_hash"],
            snapshot["sleeve_settings_hash"],
            snapshot["ips_version"],
            _stable_json(snapshot["governance_summary"]),
            _stable_json(snapshot["market_state_snapshot"]),
            snapshot["payload_json"],
            snapshot["created_at"],
        ),
    )
    all_scores: list[dict[str, Any]] = []
    sleeve_recommendations: list[dict[str, Any]] = []
    for sleeve in list(blueprint_payload.get("sleeves") or []):
        recommendation = dict(sleeve.get("recommendation") or {})
        if recommendation:
            sleeve_recommendations.append(recommendation)
        for candidate in list(sleeve.get("candidates") or []):
            quality = dict(candidate.get("investment_quality") or {})
            if quality:
                all_scores.append(quality)
    if all_scores:
        persist_quality_scores(conn, snapshot_id=str(snapshot["snapshot_id"]), scores=all_scores)
    if sleeve_recommendations:
        persist_sleeve_recommendations(conn, snapshot_id=str(snapshot["snapshot_id"]), summaries=sleeve_recommendations)
    if previous := conn.execute(
        """
        SELECT payload_json
        FROM blueprint_snapshots
        WHERE blueprint_id = ? AND snapshot_id != ?
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (blueprint_id, snapshot["snapshot_id"]),
    ).fetchone():
        try:
            events = build_recommendation_events(json.loads(str(previous["payload_json"])), blueprint_payload)
            if events:
                persist_recommendation_events(conn, snapshot_id=str(snapshot["snapshot_id"]), events=events)
        except Exception:
            pass
    persist_blueprint_decision_artifacts(
        conn,
        snapshot_id=str(snapshot["snapshot_id"]),
        blueprint_payload=blueprint_payload,
    )
    conn.commit()
    return {key: value for key, value in snapshot.items() if key != "payload_json"}


def list_blueprint_snapshots(conn: sqlite3.Connection, *, limit: int = 25) -> list[dict[str, Any]]:
    blueprint = get_current_blueprint(conn)
    rows = conn.execute(
        """
        SELECT snapshot_id, blueprint_id, actor_id, note, blueprint_hash, portfolio_settings_hash,
               candidate_list_hash, sleeve_settings_hash, ips_version, governance_summary_json,
               market_state_snapshot_json, created_at
        FROM blueprint_snapshots
        WHERE blueprint_id = ?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (str(blueprint["blueprint_id"]), max(1, min(limit, 100))),
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        for key in ("governance_summary_json", "market_state_snapshot_json"):
            try:
                item[key[:-5]] = json.loads(str(item.pop(key) or "{}"))
            except Exception:
                item[key[:-5]] = {}
        items.append(item)
    return items


def get_blueprint_snapshot(conn: sqlite3.Connection, snapshot_id: str) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT snapshot_id, blueprint_id, actor_id, note, blueprint_hash, portfolio_settings_hash,
               candidate_list_hash, sleeve_settings_hash, ips_version, governance_summary_json,
               market_state_snapshot_json, payload_json, created_at
        FROM blueprint_snapshots
        WHERE snapshot_id = ?
        """,
        (snapshot_id,),
    ).fetchone()
    if row is None:
        return None
    out = dict(row)
    for key in ("governance_summary_json", "market_state_snapshot_json"):
        try:
            out[key[:-5]] = json.loads(str(out.pop(key) or "{}"))
        except Exception:
            out[key[:-5]] = {}
    out["payload"] = json.loads(str(out.pop("payload_json")))
    out["decision_artifacts"] = list_blueprint_decision_artifacts(conn, snapshot_id=snapshot_id)
    return out


def diff_blueprint_snapshots(conn: sqlite3.Connection, *, snapshot_a: str, snapshot_b: str) -> dict[str, Any]:
    left = get_blueprint_snapshot(conn, snapshot_a)
    right = get_blueprint_snapshot(conn, snapshot_b)
    if left is None or right is None:
        raise KeyError("Snapshot not found")

    left_payload = dict(left["payload"] or {})
    right_payload = dict(right["payload"] or {})

    left_sleeves = {str(item.get("sleeve_key")): item for item in list(left_payload.get("sleeves") or [])}
    right_sleeves = {str(item.get("sleeve_key")): item for item in list(right_payload.get("sleeves") or [])}

    left_candidates = _flatten_candidates(left_payload)
    right_candidates = _flatten_candidates(right_payload)

    added_candidates = sorted(set(right_candidates) - set(left_candidates))
    removed_candidates = sorted(set(left_candidates) - set(right_candidates))

    weight_range_changes: list[dict[str, Any]] = []
    policy_changes: list[str] = []
    verification_status_changes: list[dict[str, Any]] = []
    risk_control_changes: list[dict[str, Any]] = []
    recommendation_changes: list[dict[str, Any]] = []
    sleeve_pick_changes: list[dict[str, Any]] = []

    for sleeve_key in sorted(set(left_sleeves) | set(right_sleeves)):
        before = dict(left_sleeves.get(sleeve_key) or {})
        after = dict(right_sleeves.get(sleeve_key) or {})
        before_range = dict(before.get("policy_weight_range") or {})
        after_range = dict(after.get("policy_weight_range") or {})
        if before_range != after_range:
            weight_range_changes.append(
                {
                    "sleeve_key": sleeve_key,
                    "before": before_range,
                    "after": after_range,
                }
            )
        before_rebalance = dict(before.get("rebalance_policy") or {})
        after_rebalance = dict(after.get("rebalance_policy") or {})
        if before_rebalance != after_rebalance:
            policy_changes.append(f"{sleeve_key}: rebalance policy changed")
        before_pick = str(dict(before.get("recommendation") or {}).get("our_pick_symbol") or "")
        after_pick = str(dict(after.get("recommendation") or {}).get("our_pick_symbol") or "")
        if before_pick != after_pick:
            sleeve_pick_changes.append(
                {"sleeve_key": sleeve_key, "before": before_pick or None, "after": after_pick or None}
            )

    left_meta = dict(left_payload.get("blueprint_meta") or {})
    right_meta = dict(right_payload.get("blueprint_meta") or {})
    for field in ("version", "base_currency", "profile_type", "domicile_preference", "accumulation_preference"):
        if left_meta.get(field) != right_meta.get(field):
            policy_changes.append(f"{field}: {left_meta.get(field)} -> {right_meta.get(field)}")

    common_candidates = sorted(set(left_candidates) & set(right_candidates))
    for key in common_candidates:
        left_candidate = left_candidates[key]
        right_candidate = right_candidates[key]
        if left_candidate.get("verification_status") != right_candidate.get("verification_status"):
            verification_status_changes.append(
                {
                    "candidate": key,
                    "before": left_candidate.get("verification_status"),
                    "after": right_candidate.get("verification_status"),
                }
            )
        left_risk = str(dict(left_candidate.get("investment_lens") or {}).get("risk_control_summary", {}).get("status") or "unknown")
        right_risk = str(dict(right_candidate.get("investment_lens") or {}).get("risk_control_summary", {}).get("status") or "unknown")
        left_liq = str(dict(left_candidate.get("investment_lens") or {}).get("liquidity_profile", {}).get("liquidity_status") or "unknown")
        right_liq = str(dict(right_candidate.get("investment_lens") or {}).get("liquidity_profile", {}).get("liquidity_status") or "unknown")
        if left_risk != right_risk or left_liq != right_liq:
            risk_control_changes.append(
                {
                    "candidate": key,
                    "risk_control_status": {"before": left_risk, "after": right_risk},
                    "liquidity_status": {"before": left_liq, "after": right_liq},
                }
            )
        left_quality = dict(left_candidate.get("investment_quality") or {})
        right_quality = dict(right_candidate.get("investment_quality") or {})
        if (
            left_quality.get("rank_in_sleeve") != right_quality.get("rank_in_sleeve")
            or left_quality.get("badge") != right_quality.get("badge")
            or left_quality.get("recommendation_state") != right_quality.get("recommendation_state")
        ):
            recommendation_changes.append(
                {
                    "candidate": key,
                    "rank": {"before": left_quality.get("rank_in_sleeve"), "after": right_quality.get("rank_in_sleeve")},
                    "badge": {"before": left_quality.get("badge"), "after": right_quality.get("badge")},
                    "recommendation_state": {
                        "before": left_quality.get("recommendation_state"),
                        "after": right_quality.get("recommendation_state"),
                    },
                }
            )

    return {
        "snapshot_a": _strip_payload(left),
        "snapshot_b": _strip_payload(right),
        "diff": {
            "added_candidates": added_candidates,
            "removed_candidates": removed_candidates,
            "weight_range_changes": weight_range_changes,
            "policy_changes": policy_changes,
            "verification_status_changes": verification_status_changes,
            "risk_control_changes": risk_control_changes,
            "recommendation_changes": recommendation_changes,
            "sleeve_pick_changes": sleeve_pick_changes,
        },
    }


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _portfolio_settings_payload(payload: dict[str, Any]) -> dict[str, Any]:
    meta = dict(payload.get("blueprint_meta") or {})
    return {
        "base_currency": meta.get("base_currency"),
        "profile_type": meta.get("profile_type"),
        "domicile_preference": meta.get("domicile_preference"),
        "accumulation_preference": meta.get("accumulation_preference"),
        "default_investor_profile": meta.get("default_investor_profile"),
    }


def _candidate_list_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for sleeve in list(payload.get("sleeves") or []):
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        for candidate in list(sleeve.get("candidates") or []):
            out.append(
                {
                    "sleeve_key": sleeve_key,
                    "symbol": candidate.get("symbol"),
                    "name": candidate.get("name"),
                    "verification_status": candidate.get("verification_status"),
                }
            )
    return sorted(out, key=lambda item: (str(item.get("sleeve_key") or ""), str(item.get("symbol") or "")))


def _sleeve_settings_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for sleeve in list(payload.get("sleeves") or []):
        out.append(
            {
                "sleeve_key": sleeve.get("sleeve_key"),
                "policy_weight_range": sleeve.get("policy_weight_range"),
                "constraints": sleeve.get("constraints"),
                "rebalance_policy": sleeve.get("rebalance_policy"),
            }
        )
    return sorted(out, key=lambda item: str(item.get("sleeve_key") or ""))


def _flatten_candidates(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for sleeve in list(payload.get("sleeves") or []):
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        for candidate in list(sleeve.get("candidates") or []):
            key = f"{sleeve_key}::{str(candidate.get('symbol') or '')}"
            out[key] = dict(candidate)
    return out


def _strip_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in snapshot.items() if key != "payload"}
