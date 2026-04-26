from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any


REGENERATION_JOB_STATUSES = {
    "queued",
    "running",
    "succeeded",
    "failed",
    "succeeded_but_contract_invalid",
}


def ensure_daily_brief_regeneration_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_brief_regeneration_jobs (
          job_id TEXT PRIMARY KEY,
          requested_at TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          status TEXT NOT NULL,
          requested_by TEXT,
          brief_mode TEXT NOT NULL DEFAULT 'daily',
          audience_preset TEXT NOT NULL DEFAULT 'pm',
          force_cache_only INTEGER NOT NULL DEFAULT 0,
          contract_version_target TEXT,
          contract_version_persisted TEXT,
          brief_run_id TEXT,
          verifier_result_json TEXT NOT NULL DEFAULT '{}',
          proof_json TEXT NOT NULL DEFAULT '{}',
          stage_reports_json TEXT NOT NULL DEFAULT '[]',
          failure_reason TEXT,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_daily_brief_regeneration_jobs_requested
        ON daily_brief_regeneration_jobs (requested_at DESC)
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_daily_brief_regeneration_jobs_status
        ON daily_brief_regeneration_jobs (status, updated_at DESC)
        """
    )
    conn.commit()


def _row_to_job(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {
        "job_id": str(row["job_id"]),
        "requested_at": str(row["requested_at"]),
        "started_at": str(row["started_at"]) if row["started_at"] is not None else None,
        "finished_at": str(row["finished_at"]) if row["finished_at"] is not None else None,
        "status": str(row["status"]),
        "requested_by": str(row["requested_by"]) if row["requested_by"] is not None else None,
        "brief_mode": str(row["brief_mode"] or "daily"),
        "audience_preset": str(row["audience_preset"] or "pm"),
        "force_cache_only": bool(row["force_cache_only"]),
        "contract_version_target": str(row["contract_version_target"]) if row["contract_version_target"] is not None else None,
        "contract_version_persisted": str(row["contract_version_persisted"]) if row["contract_version_persisted"] is not None else None,
        "brief_run_id": str(row["brief_run_id"]) if row["brief_run_id"] is not None else None,
        "verifier_result": json.loads(str(row["verifier_result_json"] or "{}")),
        "proof": json.loads(str(row["proof_json"] or "{}")),
        "stage_reports": json.loads(str(row["stage_reports_json"] or "[]")),
        "failure_reason": str(row["failure_reason"]) if row["failure_reason"] is not None else None,
        "updated_at": str(row["updated_at"]),
    }


def create_regeneration_job(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    requested_by: str | None,
    brief_mode: str,
    audience_preset: str,
    force_cache_only: bool,
    contract_version_target: str,
) -> dict[str, Any]:
    ensure_daily_brief_regeneration_tables(conn)
    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO daily_brief_regeneration_jobs (
          job_id, requested_at, started_at, finished_at, status, requested_by,
          brief_mode, audience_preset, force_cache_only, contract_version_target,
          verifier_result_json, proof_json, stage_reports_json, failure_reason, updated_at
        ) VALUES (?, ?, NULL, NULL, 'queued', ?, ?, ?, ?, ?, '{}', '{}', '[]', NULL, ?)
        """,
        (
            job_id,
            now,
            requested_by,
            brief_mode,
            audience_preset,
            1 if force_cache_only else 0,
            contract_version_target,
            now,
        ),
    )
    conn.commit()
    return get_regeneration_job(conn, job_id) or {}


def update_regeneration_job(
    conn: sqlite3.Connection,
    job_id: str,
    *,
    status: str | None = None,
    started_at: str | None = None,
    finished_at: str | None = None,
    contract_version_persisted: str | None = None,
    brief_run_id: str | None = None,
    verifier_result: dict[str, Any] | None = None,
    proof: dict[str, Any] | None = None,
    stage_reports: list[dict[str, Any]] | None = None,
    failure_reason: str | None = None,
) -> dict[str, Any] | None:
    ensure_daily_brief_regeneration_tables(conn)
    fields: list[str] = []
    values: list[Any] = []
    if status is not None:
        fields.append("status = ?")
        values.append(status)
    if started_at is not None:
        fields.append("started_at = ?")
        values.append(started_at)
    if finished_at is not None:
        fields.append("finished_at = ?")
        values.append(finished_at)
    if contract_version_persisted is not None:
        fields.append("contract_version_persisted = ?")
        values.append(contract_version_persisted)
    if brief_run_id is not None:
        fields.append("brief_run_id = ?")
        values.append(brief_run_id)
    if verifier_result is not None:
        fields.append("verifier_result_json = ?")
        values.append(json.dumps(verifier_result))
    if proof is not None:
        fields.append("proof_json = ?")
        values.append(json.dumps(proof))
    if stage_reports is not None:
        fields.append("stage_reports_json = ?")
        values.append(json.dumps(stage_reports))
    if failure_reason is not None:
        fields.append("failure_reason = ?")
        values.append(failure_reason)
    fields.append("updated_at = ?")
    values.append(datetime.now(UTC).isoformat())
    values.append(job_id)
    conn.execute(
        f"""
        UPDATE daily_brief_regeneration_jobs
        SET {", ".join(fields)}
        WHERE job_id = ?
        """,
        tuple(values),
    )
    conn.commit()
    return get_regeneration_job(conn, job_id)


def get_regeneration_job(conn: sqlite3.Connection, job_id: str) -> dict[str, Any] | None:
    ensure_daily_brief_regeneration_tables(conn)
    row = conn.execute(
        "SELECT * FROM daily_brief_regeneration_jobs WHERE job_id = ?",
        (job_id,),
    ).fetchone()
    return _row_to_job(row)


def latest_regeneration_job(conn: sqlite3.Connection) -> dict[str, Any] | None:
    ensure_daily_brief_regeneration_tables(conn)
    row = conn.execute(
        """
        SELECT *
        FROM daily_brief_regeneration_jobs
        ORDER BY requested_at DESC
        LIMIT 1
        """
    ).fetchone()
    return _row_to_job(row)


def latest_verified_good_regeneration(conn: sqlite3.Connection) -> dict[str, Any] | None:
    ensure_daily_brief_regeneration_tables(conn)
    row = conn.execute(
        """
        SELECT *
        FROM daily_brief_regeneration_jobs
        WHERE status = 'succeeded'
          AND json_extract(verifier_result_json, '$.status') = 'ok'
        ORDER BY finished_at DESC, requested_at DESC
        LIMIT 1
        """
    ).fetchone()
    return _row_to_job(row)


def reconcile_incomplete_regeneration_jobs(conn: sqlite3.Connection, *, reason: str = "interrupted_by_restart") -> int:
    ensure_daily_brief_regeneration_tables(conn)
    cursor = conn.execute(
        """
        UPDATE daily_brief_regeneration_jobs
        SET status = 'failed',
            finished_at = COALESCE(finished_at, ?),
            failure_reason = COALESCE(failure_reason, ?),
            updated_at = ?
        WHERE status IN ('queued', 'running')
        """,
        (
            datetime.now(UTC).isoformat(),
            reason,
            datetime.now(UTC).isoformat(),
        ),
    )
    conn.commit()
    return int(cursor.rowcount or 0)
