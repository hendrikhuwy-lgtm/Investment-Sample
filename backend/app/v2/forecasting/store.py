from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.config import get_db_path
from app.v2.forecasting.capabilities import ForecastBundle, ForecastEvaluation, ForecastRequest, ScenarioSupport, TriggerSupport


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
        CREATE TABLE IF NOT EXISTS forecast_runs (
          forecast_run_id TEXT PRIMARY KEY,
          provider TEXT NOT NULL,
          model_name TEXT NOT NULL,
          surface_name TEXT NOT NULL,
          candidate_id TEXT,
          object_type TEXT NOT NULL,
          object_id TEXT NOT NULL,
          series_id TEXT NOT NULL,
          horizon INTEGER NOT NULL,
          status TEXT NOT NULL,
          generated_at TEXT NOT NULL,
          degraded_reason TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_forecast_runs_object
        ON forecast_runs (surface_name, object_type, object_id, generated_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_outputs (
          forecast_output_id TEXT PRIMARY KEY,
          forecast_run_id TEXT NOT NULL,
          point_json TEXT NOT NULL,
          quantiles_json TEXT NOT NULL,
          anomaly_score REAL,
          confidence_summary TEXT NOT NULL,
          direction_summary TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS scenario_support_records (
          scenario_support_id TEXT PRIMARY KEY,
          forecast_run_id TEXT,
          candidate_id TEXT,
          object_type TEXT NOT NULL,
          object_id TEXT NOT NULL,
          provider TEXT NOT NULL,
          bull_json TEXT NOT NULL,
          base_json TEXT NOT NULL,
          bear_json TEXT NOT NULL,
          support_strength TEXT NOT NULL,
          what_confirms TEXT NOT NULL,
          what_breaks TEXT NOT NULL,
          threshold_summary_json TEXT NOT NULL,
          generated_at TEXT NOT NULL,
          degraded_reason TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_scenario_support_object
        ON scenario_support_records (object_type, object_id, generated_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trigger_support_records (
          trigger_support_id TEXT PRIMARY KEY,
          forecast_run_id TEXT,
          candidate_id TEXT,
          object_type TEXT NOT NULL,
          object_id TEXT NOT NULL,
          trigger_type TEXT NOT NULL,
          threshold_value TEXT NOT NULL,
          threshold_state TEXT NOT NULL,
          provider TEXT NOT NULL,
          current_distance_to_trigger TEXT NOT NULL,
          next_action_if_hit TEXT NOT NULL,
          next_action_if_broken TEXT NOT NULL,
          support_strength TEXT NOT NULL,
          confidence_summary TEXT NOT NULL,
          generated_at TEXT NOT NULL,
          degraded_reason TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_trigger_support_object
        ON trigger_support_records (object_type, object_id, generated_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_evaluations (
          evaluation_id TEXT PRIMARY KEY,
          provider TEXT NOT NULL,
          model_name TEXT NOT NULL,
          series_family TEXT NOT NULL,
          horizon INTEGER NOT NULL,
          metric_name TEXT NOT NULL,
          metric_value REAL NOT NULL,
          measured_at TEXT NOT NULL,
          notes_json TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_provider_probes (
          probe_id TEXT PRIMARY KEY,
          provider TEXT NOT NULL,
          base_url TEXT NOT NULL,
          endpoint TEXT NOT NULL,
          probed_at TEXT NOT NULL,
          success INTEGER NOT NULL,
          http_status INTEGER,
          latency_ms INTEGER,
          json_ok INTEGER,
          shape_ok INTEGER,
          horizon_ok INTEGER,
          error_code TEXT,
          error_message TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_forecast_provider_probes_provider
        ON forecast_provider_probes (provider, probed_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS forecast_evidence_refs (
          evidence_ref_id TEXT PRIMARY KEY,
          forecast_run_id TEXT,
          candidate_id TEXT NOT NULL,
          object_type TEXT NOT NULL,
          object_id TEXT NOT NULL,
          object_label TEXT NOT NULL,
          provider TEXT NOT NULL,
          model_name TEXT NOT NULL,
          support_strength TEXT NOT NULL,
          freshness_state TEXT NOT NULL,
          degraded_state TEXT,
          support_class TEXT NOT NULL,
          evidence_label TEXT NOT NULL,
          summary TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_forecast_evidence_refs_candidate
        ON forecast_evidence_refs (candidate_id, object_type, object_id, created_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS v2_notebook_forecast_refs (
          note_forecast_ref_id TEXT PRIMARY KEY,
          entry_id TEXT NOT NULL,
          forecast_run_id TEXT NOT NULL,
          reference_label TEXT NOT NULL,
          threshold_summary TEXT,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_v2_notebook_forecast_refs_entry
        ON v2_notebook_forecast_refs (entry_id, created_at DESC)
        """
    )
    conn.commit()


def record_provider_probe(
    *,
    provider: str,
    base_url: str,
    endpoint: str,
    success: bool,
    http_status: int | None,
    latency_ms: int | None,
    json_ok: bool | None,
    shape_ok: bool | None,
    horizon_ok: bool | None,
    error_code: str | None,
    error_message: str | None,
    probed_at: str | None = None,
) -> dict[str, Any]:
    payload = {
        "probe_id": f"forecast_probe_{uuid.uuid4().hex}",
        "provider": provider,
        "base_url": base_url,
        "endpoint": endpoint,
        "probed_at": probed_at or _now_iso(),
        "success": 1 if success else 0,
        "http_status": http_status,
        "latency_ms": latency_ms,
        "json_ok": None if json_ok is None else (1 if json_ok else 0),
        "shape_ok": None if shape_ok is None else (1 if shape_ok else 0),
        "horizon_ok": None if horizon_ok is None else (1 if horizon_ok else 0),
        "error_code": error_code,
        "error_message": error_message,
    }
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO forecast_provider_probes (
              probe_id, provider, base_url, endpoint, probed_at, success, http_status, latency_ms,
              json_ok, shape_ok, horizon_ok, error_code, error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                payload["probe_id"],
                payload["provider"],
                payload["base_url"],
                payload["endpoint"],
                payload["probed_at"],
                payload["success"],
                payload["http_status"],
                payload["latency_ms"],
                payload["json_ok"],
                payload["shape_ok"],
                payload["horizon_ok"],
                payload["error_code"],
                payload["error_message"],
            ),
        )
        conn.commit()
    return {
        **payload,
        "success": bool(payload["success"]),
        "json_ok": None if payload["json_ok"] is None else bool(payload["json_ok"]),
        "shape_ok": None if payload["shape_ok"] is None else bool(payload["shape_ok"]),
        "horizon_ok": None if payload["horizon_ok"] is None else bool(payload["horizon_ok"]),
    }


def persist_forecast_bundle(
    *,
    bundle: ForecastBundle,
    surface_name: str,
    candidate_id: str | None,
    object_label: str,
    support_class: str,
) -> ForecastBundle:
    forecast_run_id = f"forecast_run_{uuid.uuid4().hex}"
    output_id = f"forecast_output_{uuid.uuid4().hex}"
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO forecast_runs (
              forecast_run_id, provider, model_name, surface_name, candidate_id,
              object_type, object_id, series_id, horizon, status, generated_at, degraded_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                forecast_run_id,
                bundle.result.provider,
                bundle.result.model_name,
                surface_name,
                candidate_id,
                bundle.request.object_type,
                bundle.request.object_id,
                bundle.request.series_id,
                bundle.request.horizon,
                "degraded" if bundle.result.degraded_state else "ready",
                bundle.result.generated_at,
                bundle.result.degraded_state,
            ),
        )
        conn.execute(
            """
            INSERT INTO forecast_outputs (
              forecast_output_id, forecast_run_id, point_json, quantiles_json,
              anomaly_score, confidence_summary, direction_summary
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                output_id,
                forecast_run_id,
                json.dumps(bundle.result.point_path, ensure_ascii=True),
                json.dumps(bundle.result.quantiles, ensure_ascii=True),
                bundle.result.anomaly_score,
                bundle.support.confidence_summary,
                bundle.result.direction,
            ),
        )
        scenario_support_id = f"scenario_support_{uuid.uuid4().hex}"
        conn.execute(
            """
            INSERT INTO scenario_support_records (
              scenario_support_id, forecast_run_id, candidate_id, object_type, object_id, provider,
              bull_json, base_json, bear_json, support_strength, what_confirms, what_breaks,
              threshold_summary_json, generated_at, degraded_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                scenario_support_id,
                forecast_run_id,
                candidate_id,
                bundle.request.object_type,
                bundle.request.object_id,
                bundle.support.provider,
                json.dumps(bundle.scenario_support.bull_case, ensure_ascii=True),
                json.dumps(bundle.scenario_support.base_case, ensure_ascii=True),
                json.dumps(bundle.scenario_support.bear_case, ensure_ascii=True),
                bundle.scenario_support.support_strength,
                bundle.scenario_support.what_confirms,
                bundle.scenario_support.what_breaks,
                json.dumps([item.to_dict() for item in bundle.trigger_support], ensure_ascii=True),
                bundle.scenario_support.generated_at,
                bundle.scenario_support.degraded_state,
            ),
        )
        for item in bundle.trigger_support:
            conn.execute(
                """
                INSERT INTO trigger_support_records (
                  trigger_support_id, forecast_run_id, candidate_id, object_type, object_id, trigger_type,
                  threshold_value, threshold_state, provider, current_distance_to_trigger, next_action_if_hit,
                  next_action_if_broken, support_strength, confidence_summary, generated_at, degraded_reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    f"trigger_support_{uuid.uuid4().hex}",
                    forecast_run_id,
                    candidate_id,
                    bundle.request.object_type,
                    bundle.request.object_id,
                    item.trigger_type,
                    item.threshold,
                    item.threshold_state,
                    item.provider,
                    item.current_distance_to_trigger,
                    item.next_action_if_hit,
                    item.next_action_if_broken,
                    item.support_strength,
                    item.confidence_summary,
                    item.generated_at,
                    item.degraded_state,
                ),
            )
        evaluation = bundle.evaluation
        conn.execute(
            """
            INSERT INTO forecast_evaluations (
              evaluation_id, provider, model_name, series_family, horizon,
              metric_name, metric_value, measured_at, notes_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"forecast_eval_{uuid.uuid4().hex}",
                evaluation.provider,
                evaluation.model_name,
                evaluation.series_family,
                evaluation.horizon,
                evaluation.metric_name,
                evaluation.metric_value,
                evaluation.measured_at,
                json.dumps(evaluation.notes, ensure_ascii=True),
            ),
        )
        conn.execute(
            """
            INSERT INTO forecast_evidence_refs (
              evidence_ref_id, forecast_run_id, candidate_id, object_type, object_id, object_label,
              provider, model_name, support_strength, freshness_state, degraded_state,
              support_class, evidence_label, summary, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"forecast_evidence_ref_{uuid.uuid4().hex}",
                forecast_run_id,
                candidate_id or bundle.request.object_id,
                bundle.request.object_type,
                bundle.request.object_id,
                object_label,
                bundle.support.provider,
                bundle.support.model_name,
                bundle.support.support_strength,
                bundle.result.freshness_state,
                bundle.support.degraded_state,
                support_class,
                f"Forecast support for {object_label}",
                str(bundle.scenario_support.base_case.get("summary") or ""),
                bundle.support.generated_at,
            ),
        )
        conn.commit()
    return ForecastBundle(
        request=bundle.request,
        result=bundle.result,
        support=bundle.support,
        scenario_support=bundle.scenario_support,
        trigger_support=bundle.trigger_support,
        evaluation=bundle.evaluation,
        forecast_run_id=forecast_run_id,
    )


def list_latest_runs(*, candidate_id: str | None = None, object_id: str | None = None, surface_name: str | None = None, limit: int = 24) -> list[dict[str, Any]]:
    capped_limit = max(1, min(int(limit or 24), 200))
    clauses: list[str] = []
    params: list[Any] = []
    if candidate_id is not None:
        clauses.append("candidate_id = ?")
        params.append(candidate_id)
    if object_id is not None:
        clauses.append("object_id = ?")
        params.append(object_id)
    if surface_name is not None:
        clauses.append("surface_name = ?")
        params.append(surface_name)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connection() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM forecast_runs
            {where}
            ORDER BY generated_at DESC
            LIMIT ?
            """,
            (*params, capped_limit),
        ).fetchall()
        return [dict(row) for row in rows]


def list_evaluations(*, provider: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    capped_limit = max(1, min(int(limit or 100), 500))
    clauses: list[str] = []
    params: list[Any] = []
    if provider is not None:
        clauses.append("provider = ?")
        params.append(provider)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connection() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM forecast_evaluations
            {where}
            ORDER BY measured_at DESC
            LIMIT ?
            """,
            (*params, capped_limit),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            try:
                item["notes"] = json.loads(str(item.get("notes_json") or "[]"))
            except Exception:
                item["notes"] = []
        return items


def list_forecast_evidence_refs(candidate_id: str) -> list[dict[str, Any]]:
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM forecast_evidence_refs
            WHERE candidate_id = ?
            ORDER BY created_at DESC
            """,
            (candidate_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def add_notebook_forecast_reference(entry_id: str, *, forecast_run_id: str, reference_label: str, threshold_summary: str | None) -> dict[str, Any]:
    payload = {
        "note_forecast_ref_id": f"note_forecast_ref_{uuid.uuid4().hex}",
        "entry_id": entry_id,
        "forecast_run_id": forecast_run_id,
        "reference_label": reference_label,
        "threshold_summary": threshold_summary,
        "created_at": _now_iso(),
    }
    with _connection() as conn:
        conn.execute(
            """
            INSERT INTO v2_notebook_forecast_refs (
              note_forecast_ref_id, entry_id, forecast_run_id, reference_label, threshold_summary, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                payload["note_forecast_ref_id"],
                payload["entry_id"],
                payload["forecast_run_id"],
                payload["reference_label"],
                payload["threshold_summary"],
                payload["created_at"],
            ),
        )
        conn.commit()
    return payload


def list_notebook_forecast_references(*, candidate_id: str | None = None, entry_id: str | None = None) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    joins = ""
    if candidate_id is not None:
        joins = "JOIN v2_notebook_entries entries ON entries.entry_id = refs.entry_id"
        clauses.append("entries.candidate_id = ?")
        params.append(candidate_id)
    if entry_id is not None:
        clauses.append("refs.entry_id = ?")
        params.append(entry_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connection() as conn:
        rows = conn.execute(
            f"""
            SELECT refs.*
            FROM v2_notebook_forecast_refs refs
            {joins}
            {where}
            ORDER BY refs.created_at DESC
            """,
            params,
        ).fetchall()
        return [dict(row) for row in rows]


def latest_scenario_support(object_id: str) -> dict[str, Any] | None:
    with _connection() as conn:
        row = conn.execute(
            """
            SELECT scenario_support_records.*, forecast_outputs.anomaly_score
            FROM scenario_support_records
            LEFT JOIN forecast_outputs
              ON forecast_outputs.forecast_run_id = scenario_support_records.forecast_run_id
            WHERE object_id = ?
            ORDER BY generated_at DESC
            LIMIT 1
            """,
            (object_id,),
        ).fetchone()
        if row is None:
            return None
        item = dict(row)
        for key in ("bull_json", "base_json", "bear_json", "threshold_summary_json"):
            try:
                item[key.removesuffix("_json")] = json.loads(str(item.get(key) or "{}"))
            except Exception:
                item[key.removesuffix("_json")] = {} if key != "threshold_summary_json" else []
        return item


def latest_trigger_states(object_id: str) -> list[dict[str, Any]]:
    with _connection() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM trigger_support_records
            WHERE object_id = ?
            ORDER BY generated_at DESC
            LIMIT 12
            """,
            (object_id,),
        ).fetchall()
        return [dict(row) for row in rows]


def list_provider_probes(*, provider: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
    capped_limit = max(1, min(int(limit or 100), 500))
    clauses: list[str] = []
    params: list[Any] = []
    if provider is not None:
        clauses.append("provider = ?")
        params.append(provider)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    with _connection() as conn:
        rows = conn.execute(
            f"""
            SELECT *
            FROM forecast_provider_probes
            {where}
            ORDER BY probed_at DESC
            LIMIT ?
            """,
            (*params, capped_limit),
        ).fetchall()
        items = [dict(row) for row in rows]
        for item in items:
            item["success"] = bool(item.get("success"))
            for key in ("json_ok", "shape_ok", "horizon_ok"):
                item[key] = None if item.get(key) is None else bool(item.get(key))
        return items


def latest_provider_probe(provider: str, *, endpoint: str | None = None) -> dict[str, Any] | None:
    clauses = ["provider = ?"]
    params: list[Any] = [provider]
    if endpoint is not None:
        clauses.append("endpoint = ?")
        params.append(endpoint)
    where = f"WHERE {' AND '.join(clauses)}"
    with _connection() as conn:
        row = conn.execute(
            f"""
            SELECT *
            FROM forecast_provider_probes
            {where}
            ORDER BY probed_at DESC
            LIMIT 1
            """,
            params,
        ).fetchone()
        if row is None:
            return None
        item = dict(row)
        item["success"] = bool(item.get("success"))
        for key in ("json_ok", "shape_ok", "horizon_ok"):
            item[key] = None if item.get(key) is None else bool(item.get(key))
        return item
