from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


FIELD_VALUE_TYPES = {
    "verified",
    "inferred",
    "proxy",
    "stale",
    "missing_fetchable",
    "missing_requires_source_expansion",
    "not_applicable",
}

MISSINGNESS_REASONS = {
    "populated",
    "fetchable_from_current_sources",
    "blocked_by_parser_gap",
    "blocked_by_source_gap",
    "blocked_by_candidate_type",
    "not_applicable",
}

PROVENANCE_PRIORITIES = {
    "verified_official": 500,
    "derived_from_validated_history": 450,
    "verified_nonissuer": 400,
    "verified_mapping": 350,
    "manual_reviewed_override": 300,
    "inferred": 200,
    "proxy": 100,
    "seeded_fallback": 50,
}

SOURCE_PRIORITY_POLICY: dict[str, tuple[str, ...]] = {
    "holdings_exposure": (
        "issuer_holdings_primary",
        "issuer_factsheet_secondary",
        "verified_third_party_fallback",
        "proxy_only_last_resort",
        "internal_fallback",
    ),
    "implementation_truth": (
        "issuer_holdings_primary",
        "issuer_factsheet_secondary",
        "verified_third_party_fallback",
        "internal_fallback",
        "proxy_only_last_resort",
    ),
    "benchmark_history": (
        "derived_from_validated_history",
        "issuer_factsheet_secondary",
        "verified_third_party_fallback",
        "proxy_only_last_resort",
    ),
}

CORE_PASSIVE_SLEEVES = {
    "global_equity_core",
    "developed_ex_us_optional",
    "emerging_markets",
    "china_satellite",
    "ig_bonds",
    "cash_bills",
}

SLEEVE_REQUIRED_FIELDS: dict[str, list[dict[str, Any]]] = {
    "global_equity_core": [
        {"field_name": "candidate_id", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "research"},
        {"field_name": "symbol", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "research"},
        {"field_name": "fund_name", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
        {"field_name": "issuer", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
        {"field_name": "isin", "critical_flag": 1, "applicability_rule": "etf_like", "readiness_tier": "review"},
        {"field_name": "domicile", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
        {"field_name": "wrapper_or_vehicle_type", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
        {"field_name": "distribution_type", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
        {"field_name": "share_class", "critical_flag": 0, "applicability_rule": "etf_like", "readiness_tier": "review"},
        {"field_name": "replication_method", "critical_flag": 0, "applicability_rule": "etf_like", "readiness_tier": "review"},
        {"field_name": "expense_ratio", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
        {"field_name": "benchmark_name", "critical_flag": 1, "applicability_rule": "benchmarked", "readiness_tier": "review"},
        {"field_name": "benchmark_key", "critical_flag": 1, "applicability_rule": "benchmarked", "readiness_tier": "review"},
        {"field_name": "benchmark_assignment_method", "critical_flag": 1, "applicability_rule": "benchmarked", "readiness_tier": "review"},
        {"field_name": "benchmark_confidence", "critical_flag": 1, "applicability_rule": "benchmarked", "readiness_tier": "review"},
        {"field_name": "factsheet_as_of", "critical_flag": 1, "applicability_rule": "etf_like", "readiness_tier": "review"},
        {"field_name": "market_data_as_of", "critical_flag": 1, "applicability_rule": "market_traded", "readiness_tier": "review"},
        {"field_name": "source_state", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "shortlist"},
        {"field_name": "freshness_state", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "shortlist"},
        {"field_name": "aum", "critical_flag": 1, "applicability_rule": "etf_like", "readiness_tier": "shortlist"},
        {"field_name": "primary_listing_exchange", "critical_flag": 0, "applicability_rule": "market_traded", "readiness_tier": "shortlist"},
        {"field_name": "primary_trading_currency", "critical_flag": 1, "applicability_rule": "market_traded", "readiness_tier": "shortlist"},
        {"field_name": "liquidity_proxy", "critical_flag": 1, "applicability_rule": "market_traded", "readiness_tier": "shortlist"},
        {"field_name": "bid_ask_spread_proxy", "critical_flag": 0, "applicability_rule": "market_traded", "readiness_tier": "shortlist"},
        {"field_name": "tracking_difference_1y", "critical_flag": 0, "applicability_rule": "benchmarked", "readiness_tier": "shortlist"},
        {"field_name": "tracking_difference_3y", "critical_flag": 0, "applicability_rule": "benchmarked", "readiness_tier": "shortlist"},
        {"field_name": "developed_market_exposure_summary", "critical_flag": 1, "applicability_rule": "equity", "readiness_tier": "shortlist"},
        {"field_name": "emerging_market_exposure_summary", "critical_flag": 1, "applicability_rule": "equity", "readiness_tier": "shortlist"},
        {"field_name": "us_weight", "critical_flag": 0, "applicability_rule": "equity", "readiness_tier": "shortlist"},
        {"field_name": "top_10_concentration", "critical_flag": 0, "applicability_rule": "equity", "readiness_tier": "shortlist"},
        {"field_name": "sector_concentration_proxy", "critical_flag": 0, "applicability_rule": "equity", "readiness_tier": "shortlist"},
        {"field_name": "holdings_count", "critical_flag": 0, "applicability_rule": "etf_like", "readiness_tier": "shortlist"},
        {"field_name": "withholding_tax_posture", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "recommendation"},
        {"field_name": "estate_risk_posture", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "recommendation"},
        {"field_name": "ucits_status", "critical_flag": 1, "applicability_rule": "etf_like", "readiness_tier": "recommendation"},
        {"field_name": "securities_lending_policy", "critical_flag": 0, "applicability_rule": "etf_like", "readiness_tier": "recommendation"},
        {"field_name": "last_successful_ingest_at", "critical_flag": 0, "applicability_rule": "market_traded", "readiness_tier": "recommendation"},
    ],
    "ig_bonds": [
        {"field_name": "effective_duration", "critical_flag": 1, "applicability_rule": "bond", "readiness_tier": "shortlist"},
        {
            "field_name": "average_maturity",
            "critical_flag": 0,
            "applicability_rule": "bond_average_maturity_disclosed",
            "readiness_tier": "shortlist",
        },
        {"field_name": "yield_proxy", "critical_flag": 1, "applicability_rule": "bond", "readiness_tier": "shortlist"},
        {"field_name": "credit_quality_mix", "critical_flag": 1, "applicability_rule": "bond", "readiness_tier": "recommendation"},
        {"field_name": "government_vs_corporate_split", "critical_flag": 0, "applicability_rule": "bond", "readiness_tier": "recommendation"},
        {"field_name": "issuer_concentration_proxy", "critical_flag": 0, "applicability_rule": "bond", "readiness_tier": "recommendation"},
        {"field_name": "interest_rate_sensitivity_proxy", "critical_flag": 1, "applicability_rule": "bond", "readiness_tier": "recommendation"},
    ],
    "cash_bills": [
        {"field_name": "weighted_average_maturity", "critical_flag": 0, "applicability_rule": "cash_like", "readiness_tier": "shortlist"},
        {"field_name": "portfolio_quality_summary", "critical_flag": 1, "applicability_rule": "cash_like", "readiness_tier": "shortlist"},
        {"field_name": "yield_proxy", "critical_flag": 1, "applicability_rule": "cash_like", "readiness_tier": "shortlist"},
        {"field_name": "redemption_settlement_notes", "critical_flag": 0, "applicability_rule": "cash_like", "readiness_tier": "recommendation"},
        {"field_name": "sg_suitability_note", "critical_flag": 1, "applicability_rule": "cash_like", "readiness_tier": "recommendation"},
    ],
    "real_assets": [
        {"field_name": "asset_type_classification", "critical_flag": 1, "applicability_rule": "real_assets", "readiness_tier": "review"},
        {"field_name": "inflation_linkage_rationale", "critical_flag": 1, "applicability_rule": "real_assets", "readiness_tier": "shortlist"},
        {"field_name": "underlying_exposure_profile", "critical_flag": 1, "applicability_rule": "real_assets", "readiness_tier": "shortlist"},
        {"field_name": "distribution_policy", "critical_flag": 0, "applicability_rule": "real_assets", "readiness_tier": "recommendation"},
        {"field_name": "tax_posture", "critical_flag": 1, "applicability_rule": "real_assets", "readiness_tier": "recommendation"},
    ],
    "alternatives": [
        {"field_name": "instrument_type", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
        {"field_name": "role_in_portfolio", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
        {"field_name": "implementation_method", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "shortlist"},
        {"field_name": "cost_model", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "shortlist"},
        {"field_name": "liquidity_and_execution_constraints", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "shortlist"},
        {"field_name": "scenario_role", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "recommendation"},
        {"field_name": "governance_conditions", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "recommendation"},
    ],
    "convex": [
        {"field_name": "instrument_type", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
        {"field_name": "role_in_portfolio", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
        {"field_name": "implementation_method", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "shortlist"},
        {"field_name": "cost_model", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "shortlist"},
        {"field_name": "liquidity_and_execution_constraints", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "shortlist"},
        {"field_name": "scenario_role", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "recommendation"},
        {"field_name": "governance_conditions", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "recommendation"},
        {"field_name": "max_loss_known", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "recommendation"},
        {"field_name": "margin_required", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "recommendation"},
        {"field_name": "short_options", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "recommendation"},
    ],
}

_COMMON_FIELDS = [
    {"field_name": "candidate_id", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "research"},
    {"field_name": "symbol", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "research"},
    {"field_name": "fund_name", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
    {"field_name": "issuer", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "review"},
    {"field_name": "source_state", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "shortlist"},
    {"field_name": "freshness_state", "critical_flag": 1, "applicability_rule": "always", "readiness_tier": "shortlist"},
]


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True)


def _parse_json(value: Any) -> Any:
    try:
        return json.loads(str(value))
    except Exception:
        return None


def ensure_candidate_truth_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_field_observations (
          observation_id TEXT PRIMARY KEY,
          candidate_symbol TEXT NOT NULL,
          sleeve_key TEXT NOT NULL,
          field_name TEXT NOT NULL,
          value_json TEXT,
          value_type TEXT NOT NULL,
          source_name TEXT,
          source_url TEXT,
          observed_at TEXT,
          ingested_at TEXT NOT NULL,
          provenance_level TEXT NOT NULL,
          confidence_label TEXT,
          parser_method TEXT,
          overwrite_priority INTEGER NOT NULL,
          missingness_reason TEXT NOT NULL,
          is_current INTEGER NOT NULL DEFAULT 0,
          override_annotation_json TEXT NOT NULL DEFAULT '{}'
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_candidate_field_observations_lookup
        ON candidate_field_observations (candidate_symbol, sleeve_key, field_name, ingested_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_field_current (
          candidate_symbol TEXT NOT NULL,
          sleeve_key TEXT NOT NULL,
          field_name TEXT NOT NULL,
          resolved_value_json TEXT,
          value_type TEXT NOT NULL,
          source_name TEXT,
          source_url TEXT,
          observed_at TEXT,
          ingested_at TEXT,
          provenance_level TEXT NOT NULL,
          confidence_label TEXT,
          parser_method TEXT,
          overwrite_priority INTEGER NOT NULL,
          missingness_reason TEXT NOT NULL,
          override_annotation_json TEXT NOT NULL DEFAULT '{}',
          last_resolved_at TEXT NOT NULL,
          PRIMARY KEY (candidate_symbol, sleeve_key, field_name)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_required_field_matrix (
          sleeve_key TEXT NOT NULL,
          field_name TEXT NOT NULL,
          critical_flag INTEGER NOT NULL DEFAULT 0,
          applicability_rule TEXT NOT NULL DEFAULT 'always',
          readiness_tier TEXT NOT NULL DEFAULT 'review',
          PRIMARY KEY (sleeve_key, field_name)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_completeness_snapshots (
          snapshot_id TEXT PRIMARY KEY,
          candidate_symbol TEXT NOT NULL,
          sleeve_key TEXT NOT NULL,
          required_fields_total INTEGER NOT NULL,
          required_fields_populated INTEGER NOT NULL,
          critical_required_fields_missing_json TEXT NOT NULL DEFAULT '[]',
          fetchable_missing_count INTEGER NOT NULL DEFAULT 0,
          source_gap_missing_count INTEGER NOT NULL DEFAULT 0,
          proxy_only_count INTEGER NOT NULL DEFAULT 0,
          stale_required_count INTEGER NOT NULL DEFAULT 0,
          readiness_level TEXT NOT NULL,
          computed_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_candidate_completeness_snapshots_lookup
        ON candidate_completeness_snapshots (candidate_symbol, sleeve_key, computed_at DESC)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sleeve_no_pick_reasons (
          snapshot_id TEXT NOT NULL,
          sleeve_key TEXT NOT NULL,
          reason_code TEXT NOT NULL,
          reason_text TEXT NOT NULL,
          nearest_passing_candidate TEXT,
          blocking_fields_json TEXT NOT NULL DEFAULT '[]',
          evidence_json TEXT NOT NULL DEFAULT '{}',
          PRIMARY KEY (snapshot_id, sleeve_key)
        )
        """
    )
    conn.commit()


def seed_required_field_matrix(conn: sqlite3.Connection, *, overwrite_existing: bool = False) -> None:
    ensure_candidate_truth_tables(conn)
    all_matrices: dict[str, list[dict[str, Any]]] = {}
    for sleeve_key, items in SLEEVE_REQUIRED_FIELDS.items():
        combined = list(_COMMON_FIELDS)
        seen = {item["field_name"] for item in combined}
        for item in items:
            if item["field_name"] not in seen:
                combined.append(item)
                seen.add(item["field_name"])
        all_matrices[sleeve_key] = combined
    for sleeve_key, rows in all_matrices.items():
        for row in rows:
            if overwrite_existing:
                conn.execute(
                    """
                    INSERT INTO candidate_required_field_matrix (
                      sleeve_key, field_name, critical_flag, applicability_rule, readiness_tier
                    ) VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(sleeve_key, field_name) DO UPDATE SET
                      critical_flag = excluded.critical_flag,
                      applicability_rule = excluded.applicability_rule,
                      readiness_tier = excluded.readiness_tier
                    """,
                    (
                        sleeve_key,
                        row["field_name"],
                        int(bool(row["critical_flag"])),
                        row["applicability_rule"],
                        row["readiness_tier"],
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO candidate_required_field_matrix (
                      sleeve_key, field_name, critical_flag, applicability_rule, readiness_tier
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        sleeve_key,
                        row["field_name"],
                        int(bool(row["critical_flag"])),
                        row["applicability_rule"],
                        row["readiness_tier"],
                    ),
                )
    conn.commit()


def list_required_fields(conn: sqlite3.Connection, sleeve_key: str) -> list[dict[str, Any]]:
    ensure_candidate_truth_tables(conn)
    count_row = conn.execute(
        "SELECT COUNT(*) AS count FROM candidate_required_field_matrix WHERE sleeve_key = ?",
        (sleeve_key,),
    ).fetchone()
    if not count_row or int(count_row["count"] or 0) == 0:
        seed_required_field_matrix(conn, overwrite_existing=False)
    rows = conn.execute(
        """
        SELECT sleeve_key, field_name, critical_flag, applicability_rule, readiness_tier
        FROM candidate_required_field_matrix
        WHERE sleeve_key = ?
        ORDER BY critical_flag DESC, readiness_tier, field_name
        """,
        (sleeve_key,),
    ).fetchall()
    return [dict(row) for row in rows]


def _validate_override_annotation(
    provenance_level: str,
    override_annotation: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = dict(override_annotation or {})
    if provenance_level != "manual_reviewed_override":
        return payload
    actor = str(payload.get("actor") or "").strip()
    reason = str(payload.get("reason") or "").strip()
    occurred_at = str(payload.get("timestamp") or "").strip()
    if not actor or not reason or not occurred_at:
        raise ValueError("manual_reviewed_override requires actor, reason, and timestamp")
    return payload


def _override_annotation_payload(row: dict[str, Any]) -> dict[str, Any]:
    return _parse_json(row.get("override_annotation_json")) or {}


def _reconciled_state(row: dict[str, Any]) -> str:
    payload = _override_annotation_payload(row)
    return str(payload.get("reconciled_state") or "").strip().lower()


def _observation_reconciled_out(row: dict[str, Any]) -> bool:
    return _reconciled_state(row) in {"quarantined", "demoted"}


def _default_value_type(value: Any, provenance_level: str, missingness_reason: str) -> str:
    if missingness_reason == "not_applicable":
        return "not_applicable"
    if missingness_reason != "populated" or value is None or (isinstance(value, str) and not value.strip()):
        if missingness_reason in {"fetchable_from_current_sources", "blocked_by_parser_gap"}:
            return "missing_fetchable"
        return "missing_requires_source_expansion"
    if provenance_level.startswith("verified"):
        return "verified"
    if provenance_level == "proxy":
        return "proxy"
    return "inferred"


def _field_stale_days(field_name: str) -> int:
    if field_name in {
        "factsheet_as_of",
        "market_data_as_of",
        "aum",
        "liquidity_proxy",
        "bid_ask_spread_proxy",
        "tracking_difference_1y",
        "tracking_difference_3y",
        "tracking_difference_5y",
        "tracking_error_1y",
        "yield_proxy",
        "effective_duration",
        "average_maturity",
    }:
        return 45
    return 180


def _observation_sort_key(row: dict[str, Any], *, now: datetime) -> tuple[Any, ...]:
    populated = str(row.get("missingness_reason") or "") == "populated"
    value_type = str(row.get("value_type") or "")
    observed_at = _safe_parse_dt(row.get("observed_at"))
    ingested_at = _safe_parse_dt(row.get("ingested_at"))
    field_name = str(row.get("field_name") or "")
    stale = False
    if populated and observed_at is not None:
        stale = (now - observed_at).days > _field_stale_days(field_name)
    source_rank = _effective_source_priority(field_name, str(row.get("source_name") or ""))
    overwrite_rank = int(row.get("overwrite_priority") or 0)
    if field_name == "aum":
        return (
            1 if _observation_reconciled_out(row) else 0,
            0 if populated else 1,
            1 if value_type == "not_applicable" else 0,
            0 if not stale else 1,
            -source_rank,
            -overwrite_rank,
            -(observed_at.timestamp() if observed_at is not None else -1.0),
            -(ingested_at.timestamp() if ingested_at is not None else -1.0),
        )
    return (
        1 if _observation_reconciled_out(row) else 0,
        0 if populated else 1,
        1 if value_type == "not_applicable" else 0,
        0 if not stale else 1,
        -overwrite_rank,
        -source_rank,
        -(observed_at.timestamp() if observed_at is not None else -1.0),
        -(ingested_at.timestamp() if ingested_at is not None else -1.0),
    )


def _source_priority(source_name: str) -> int:
    normalized = str(source_name or "").strip().lower()
    if normalized == "etf_holdings":
        return 40
    if normalized in {"issuer_doc_parser", "issuer_doc_registry", "etf_factsheet_metrics"}:
        return 30
    if normalized in {"validated_history_derivation", "performance_metrics"}:
        return 25
    if normalized == "market_route_runtime":
        return 24
    if normalized in {"eodhd", "financial modeling prep", "fmp"}:
        return 20
    if normalized in {"market_history_summary", "benchmark_registry"}:
        return 15
    # Tier 18–14: live-market providers (outrank seed deterministically)
    # Tier 8–6: seed sources (explicit demotion from legacy default of 10)
    if normalized in {"tiingo", "twelve_data", "alpha_vantage", "polygon"}:
        return 18
    if normalized == "etf_market_data":
        return 16
    if normalized == "finnhub":
        return 14
    if normalized == "candidate_payload":
        return 8
    if normalized == "candidate_registry":
        return 6
    return 10


def _effective_source_priority(field_name: str, source_name: str) -> int:
    field = str(field_name or "").strip().lower()
    normalized = str(source_name or "").strip().lower()
    priority = _source_priority(source_name)
    if field == "aum":
        if normalized == "candidate_registry":
            return 9
        if normalized == "candidate_payload":
            return 5
    return priority


def _safe_parse_dt(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def upsert_field_observation(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
    sleeve_key: str,
    field_name: str,
    value: Any,
    source_name: str,
    source_url: str | None = None,
    observed_at: str | None = None,
    ingested_at: str | None = None,
    provenance_level: str = "seeded_fallback",
    confidence_label: str | None = None,
    parser_method: str | None = None,
    missingness_reason: str = "populated",
    value_type: str | None = None,
    overwrite_priority: int | None = None,
    override_annotation: dict[str, Any] | None = None,
) -> str:
    ensure_candidate_truth_tables(conn)
    provenance = provenance_level if provenance_level in PROVENANCE_PRIORITIES else "seeded_fallback"
    missingness = missingness_reason if missingness_reason in MISSINGNESS_REASONS else "blocked_by_source_gap"
    annotation = _validate_override_annotation(provenance, override_annotation)
    payload_value = None if value is None else value
    resolved_type = value_type or _default_value_type(payload_value, provenance, missingness)
    if resolved_type not in FIELD_VALUE_TYPES:
        resolved_type = _default_value_type(payload_value, provenance, missingness)
    row_id = f"candidate_field_obs_{uuid.uuid4().hex[:12]}"
    ingested = ingested_at or _now_iso()
    priority = int(overwrite_priority if overwrite_priority is not None else PROVENANCE_PRIORITIES[provenance])
    conn.execute(
        """
        INSERT INTO candidate_field_observations (
          observation_id, candidate_symbol, sleeve_key, field_name, value_json, value_type,
          source_name, source_url, observed_at, ingested_at, provenance_level, confidence_label,
          parser_method, overwrite_priority, missingness_reason, is_current, override_annotation_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
        """,
        (
            row_id,
            candidate_symbol.upper(),
            sleeve_key,
            field_name,
            _json(payload_value) if payload_value is not None else None,
            resolved_type,
            source_name,
            source_url,
            observed_at,
            ingested,
            provenance,
            confidence_label,
            parser_method,
            priority,
            missingness,
            _json(annotation),
        ),
    )
    return row_id


def resolve_candidate_field_truth(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
    sleeve_key: str,
    now: datetime | None = None,
) -> dict[str, dict[str, Any]]:
    ensure_candidate_truth_tables(conn)
    current_time = now or datetime.now(UTC)
    rows = conn.execute(
        """
        SELECT observation_id, candidate_symbol, sleeve_key, field_name, value_json, value_type,
               source_name, source_url, observed_at, ingested_at, provenance_level, confidence_label,
               parser_method, overwrite_priority, missingness_reason, override_annotation_json
        FROM candidate_field_observations
        WHERE candidate_symbol = ? AND sleeve_key = ?
        ORDER BY ingested_at DESC
        """,
        (candidate_symbol.upper(), sleeve_key),
    ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        grouped.setdefault(str(item["field_name"]), []).append(item)

    resolved: dict[str, dict[str, Any]] = {}
    conn.execute(
        "UPDATE candidate_field_observations SET is_current = 0 WHERE candidate_symbol = ? AND sleeve_key = ?",
        (candidate_symbol.upper(), sleeve_key),
    )
    for field_name, items in grouped.items():
        best = sorted(items, key=lambda row: _observation_sort_key(row, now=current_time))[0]
        observed = _safe_parse_dt(best.get("observed_at"))
        value_type = str(best.get("value_type") or "")
        if str(best.get("missingness_reason") or "") == "populated" and observed is not None:
            if (current_time - observed).days > _field_stale_days(field_name):
                value_type = "stale"
        value = _parse_json(best.get("value_json")) if best.get("value_json") is not None else None
        resolved[field_name] = {
            "candidate_symbol": candidate_symbol.upper(),
            "sleeve_key": sleeve_key,
            "field_name": field_name,
            "resolved_value": value,
            "resolved_value_json": best.get("value_json"),
            "value_type": value_type,
            "source_name": best.get("source_name"),
            "source_url": best.get("source_url"),
            "observed_at": best.get("observed_at"),
            "ingested_at": best.get("ingested_at"),
            "provenance_level": best.get("provenance_level"),
            "confidence_label": best.get("confidence_label"),
            "parser_method": best.get("parser_method"),
            "overwrite_priority": int(best.get("overwrite_priority") or 0),
            "missingness_reason": best.get("missingness_reason"),
            "override_annotation": _parse_json(best.get("override_annotation_json")) or {},
            "last_resolved_at": current_time.isoformat(),
        }
        _annotate_truth_field(resolved[field_name])
        resolved[field_name].setdefault("usable_truth", value is not None)
        resolved[field_name].setdefault("sufficiency_state", "sufficient" if value is not None else "insufficient")
        conn.execute(
            """
            INSERT OR REPLACE INTO candidate_field_current (
              candidate_symbol, sleeve_key, field_name, resolved_value_json, value_type,
              source_name, source_url, observed_at, ingested_at, provenance_level, confidence_label,
              parser_method, overwrite_priority, missingness_reason, override_annotation_json, last_resolved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                candidate_symbol.upper(),
                sleeve_key,
                field_name,
                best.get("value_json"),
                value_type,
                best.get("source_name"),
                best.get("source_url"),
                best.get("observed_at"),
                best.get("ingested_at"),
                best.get("provenance_level"),
                best.get("confidence_label"),
                best.get("parser_method"),
                int(best.get("overwrite_priority") or 0),
                best.get("missingness_reason"),
                best.get("override_annotation_json") or "{}",
                current_time.isoformat(),
            ),
        )
        conn.execute(
            "UPDATE candidate_field_observations SET is_current = 1 WHERE observation_id = ?",
            (best["observation_id"],),
        )
    conn.commit()
    return resolved


def get_candidate_field_current(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
    sleeve_key: str,
) -> dict[str, dict[str, Any]]:
    ensure_candidate_truth_tables(conn)
    rows = conn.execute(
        """
        SELECT candidate_symbol, sleeve_key, field_name, resolved_value_json, value_type, source_name,
               source_url, observed_at, ingested_at, provenance_level, confidence_label, parser_method,
               overwrite_priority, missingness_reason, override_annotation_json, last_resolved_at
        FROM candidate_field_current
        WHERE candidate_symbol = ? AND sleeve_key = ?
        """,
        (candidate_symbol.upper(), sleeve_key),
    ).fetchall()
    out: dict[str, dict[str, Any]] = {}
    for row in rows:
        item = dict(row)
        item["resolved_value"] = _parse_json(item.get("resolved_value_json")) if item.get("resolved_value_json") is not None else None
        item["override_annotation"] = _parse_json(item.get("override_annotation_json")) or {}
        _annotate_truth_field(item)
        out[str(item["field_name"])] = item
    return out


def _annotate_truth_field(item: dict[str, Any]) -> dict[str, Any]:
    field_name = str(item.get("field_name") or "")
    source_name = str(item.get("source_name") or "")
    provenance_level = str(item.get("provenance_level") or "")
    value_type = str(item.get("value_type") or "")
    missingness_reason = str(item.get("missingness_reason") or "")
    resolved_value = item.get("resolved_value")

    if source_name == "etf_holdings":
        source_type = "issuer_holdings_primary"
    elif (
        field_name == "liquidity_proxy"
        and str(resolved_value or "").strip().lower() in {"provider_history_backed", "direct_history_backed", "proxy_history_backed"}
        and source_name.lower() in {"tiingo", "eod historical data", "market_route_runtime"}
    ):
        source_type = "derived_from_validated_history"
    elif source_name in {"issuer_doc_parser", "issuer_doc_registry", "etf_factsheet_metrics"}:
        source_type = "issuer_factsheet_secondary"
    elif provenance_level == "derived_from_validated_history":
        source_type = "derived_from_validated_history"
    elif provenance_level == "verified_mapping" or source_name == "benchmark_registry":
        source_type = "mapping_authority"
    elif provenance_level == "verified_nonissuer":
        source_type = "verified_third_party_fallback"
    elif provenance_level == "proxy" or value_type == "proxy":
        source_type = "proxy_only_last_resort"
    elif provenance_level in {"manual_reviewed_override", "seeded_fallback", "inferred"}:
        source_type = "internal_fallback"
    else:
        source_type = "unclassified"

    if missingness_reason == "not_applicable":
        completeness_state = "not_applicable"
    elif missingness_reason == "populated":
        if value_type in {"verified"} and source_type in {"issuer_holdings_primary", "issuer_factsheet_secondary", "mapping_authority"}:
            completeness_state = "complete"
        elif value_type in {"stale", "proxy", "inferred"} or source_type in {"verified_third_party_fallback", "proxy_only_last_resort", "internal_fallback"}:
            completeness_state = "weak_or_partial"
        else:
            completeness_state = "complete"
    elif missingness_reason in {"fetchable_from_current_sources", "blocked_by_parser_gap"}:
        completeness_state = "incomplete"
    else:
        completeness_state = "unavailable"

    item["source_type"] = source_type
    item["evidence_class"] = provenance_level or "unknown"
    item["as_of"] = item.get("observed_at")
    item["completeness_state"] = completeness_state
    item["source_rank"] = _source_priority(source_name)
    return item


def mark_observation_reconciled(
    conn: sqlite3.Connection,
    *,
    observation_id: str,
    reconciled_state: str,
    reason: str,
    trusted_value: Any | None = None,
    trusted_source: str | None = None,
    recovery_run_id: str | None = None,
) -> None:
    ensure_candidate_truth_tables(conn)
    row = conn.execute(
        """
        SELECT override_annotation_json
        FROM candidate_field_observations
        WHERE observation_id = ?
        LIMIT 1
        """,
        (observation_id,),
    ).fetchone()
    if row is None:
        return
    annotation = _parse_json(row["override_annotation_json"]) or {}
    annotation.update(
        {
            "reconciled_state": str(reconciled_state or "").strip().lower() or "demoted",
            "reconciled_reason": str(reason or "").strip() or "field_reconciliation",
            "reconciled_at": _now_iso(),
        }
    )
    if trusted_value is not None:
        annotation["trusted_value"] = trusted_value
    if trusted_source:
        annotation["trusted_source"] = str(trusted_source)
    if recovery_run_id:
        annotation["recovery_run_id"] = str(recovery_run_id)
    conn.execute(
        """
        UPDATE candidate_field_observations
        SET override_annotation_json = ?
        WHERE observation_id = ?
        """,
        (_json(annotation), observation_id),
    )


def reconcile_field_observations(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
    sleeve_key: str,
    field_name: str,
    trusted_value: Any,
    trusted_source_names: set[str],
    reason: str,
    recovery_run_id: str | None = None,
) -> dict[str, Any]:
    ensure_candidate_truth_tables(conn)
    trusted_value_json = _json(trusted_value) if trusted_value is not None else None
    if trusted_value_json is None:
        return {"updated": 0, "kept_active": 0}
    rows = conn.execute(
        """
        SELECT observation_id, field_name, value_json, source_name, observed_at, ingested_at,
               overwrite_priority, missingness_reason, override_annotation_json
        FROM candidate_field_observations
        WHERE candidate_symbol = ? AND sleeve_key = ? AND field_name = ?
        ORDER BY ingested_at DESC
        """,
        (candidate_symbol.upper(), sleeve_key, field_name),
    ).fetchall()
    updated = 0
    kept_active = 0
    now = datetime.now(UTC)
    for raw_row in rows:
        row = dict(raw_row)
        if str(row.get("missingness_reason") or "") != "populated":
            continue
        if _observation_reconciled_out(row):
            continue
        value_json = str(row.get("value_json") or "").strip()
        source_name = str(row.get("source_name") or "").strip()
        if not value_json:
            continue
        if value_json == trusted_value_json:
            kept_active += 1
            continue
        observed_at = _safe_parse_dt(row.get("observed_at"))
        stale = bool(observed_at and (now - observed_at).days > _field_stale_days(field_name))
        source_priority = _effective_source_priority(field_name, source_name)
        reconciled_state = "quarantined" if stale or source_priority <= 10 else "demoted"
        mark_observation_reconciled(
            conn,
            observation_id=str(row.get("observation_id") or ""),
            reconciled_state=reconciled_state,
            reason=reason,
            trusted_value=trusted_value,
            trusted_source=next(iter(sorted(trusted_source_names)), None),
            recovery_run_id=recovery_run_id,
        )
        updated += 1
    return {"updated": updated, "kept_active": kept_active}


def _field_applicable(candidate: dict[str, Any], field_name: str, rule: str) -> bool:
    instrument_type = str(candidate.get("instrument_type") or "").lower()
    sleeve_key = str(candidate.get("sleeve_key") or "")
    if rule == "always":
        return True
    if rule == "etf_like":
        return instrument_type in {"etf_ucits", "etf_us"}
    if rule == "market_traded":
        return instrument_type not in {"cash_account_sg", "long_put_overlay_strategy"}
    if rule == "benchmarked":
        return sleeve_key not in {"alternatives", "convex"}
    if rule == "equity":
        return sleeve_key in {"global_equity_core", "developed_ex_us_optional", "emerging_markets", "china_satellite"}
    if rule == "bond":
        return sleeve_key == "ig_bonds"
    if rule == "bond_average_maturity_disclosed":
        return sleeve_key == "ig_bonds" and not bool(candidate.get("average_maturity_not_disclosed"))
    if rule == "cash_like":
        return sleeve_key == "cash_bills"
    if rule == "real_assets":
        return sleeve_key == "real_assets"
    return True


def compute_candidate_completeness(
    conn: sqlite3.Connection,
    *,
    candidate: dict[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    ensure_candidate_truth_tables(conn)
    current_time = now or datetime.now(UTC)
    sleeve_key = str(candidate.get("sleeve_key") or "")
    symbol = str(candidate.get("symbol") or "").upper()
    current = get_candidate_field_current(conn, candidate_symbol=symbol, sleeve_key=sleeve_key)
    requirements = list_required_fields(conn, sleeve_key)
    entries: list[dict[str, Any]] = []
    required_total = 0
    populated_count = 0
    critical_missing: list[str] = []
    fetchable_missing_count = 0
    source_gap_missing_count = 0
    proxy_only_count = 0
    stale_required_count = 0
    critical_proxy = 0
    critical_stale = 0

    for requirement in requirements:
        field_name = str(requirement["field_name"])
        critical = bool(requirement["critical_flag"])
        applicable = _field_applicable(candidate, field_name, str(requirement["applicability_rule"]))
        field = current.get(field_name)
        if not applicable:
            entries.append(
                {
                    "field_name": field_name,
                    "critical": critical,
                    "status": "not_applicable",
                    "value": None,
                    "missingness_reason": "not_applicable",
                }
            )
            continue
        required_total += 1
        status = "missing_requires_source_expansion"
        missingness = "blocked_by_source_gap"
        value = None
        if field is not None:
            status = str(field.get("value_type") or "missing_requires_source_expansion")
            missingness = str(field.get("missingness_reason") or "blocked_by_source_gap")
            value = field.get("resolved_value")
        if missingness == "populated":
            populated_count += 1
            if status == "proxy":
                proxy_only_count += 1
                if critical:
                    critical_proxy += 1
            if status == "stale":
                stale_required_count += 1
                if critical:
                    critical_stale += 1
        else:
            if missingness in {"fetchable_from_current_sources", "blocked_by_parser_gap"}:
                fetchable_missing_count += 1
            elif missingness == "blocked_by_source_gap":
                source_gap_missing_count += 1
            if critical:
                critical_missing.append(field_name)
        entries.append(
            {
                "field_name": field_name,
                "critical": critical,
                "status": status,
                "value": value,
                "missingness_reason": missingness,
                "source_name": field.get("source_name") if field else None,
            }
        )

    eligibility_state = str(dict(candidate.get("eligibility") or {}).get("eligibility_state") or "")
    if critical_missing or critical_stale:
        readiness_level = "review_ready" if populated_count >= min(required_total, 6) else "research_visible"
    elif sleeve_key in CORE_PASSIVE_SLEEVES and critical_proxy:
        readiness_level = "shortlist_ready"
    elif eligibility_state in {"eligible", "eligible_with_caution"}:
        readiness_level = "recommendation_ready"
    elif populated_count >= max(1, required_total - max(2, source_gap_missing_count)):
        readiness_level = "shortlist_ready"
    elif populated_count >= min(required_total, 6):
        readiness_level = "review_ready"
    else:
        readiness_level = "research_visible"

    snapshot = {
        "snapshot_id": f"candidate_completeness_{uuid.uuid4().hex[:12]}",
        "candidate_symbol": symbol,
        "sleeve_key": sleeve_key,
        "required_fields_total": required_total,
        "required_fields_populated": populated_count,
        "critical_required_fields_missing": critical_missing,
        "fetchable_missing_count": fetchable_missing_count,
        "source_gap_missing_count": source_gap_missing_count,
        "proxy_only_count": proxy_only_count,
        "stale_required_count": stale_required_count,
        "readiness_level": readiness_level,
        "computed_at": current_time.isoformat(),
        "requirements": entries,
    }
    conn.execute(
        """
        INSERT INTO candidate_completeness_snapshots (
          snapshot_id, candidate_symbol, sleeve_key, required_fields_total, required_fields_populated,
          critical_required_fields_missing_json, fetchable_missing_count, source_gap_missing_count,
          proxy_only_count, stale_required_count, readiness_level, computed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            snapshot["snapshot_id"],
            snapshot["candidate_symbol"],
            snapshot["sleeve_key"],
            snapshot["required_fields_total"],
            snapshot["required_fields_populated"],
            _json(snapshot["critical_required_fields_missing"]),
            snapshot["fetchable_missing_count"],
            snapshot["source_gap_missing_count"],
            snapshot["proxy_only_count"],
            snapshot["stale_required_count"],
            snapshot["readiness_level"],
            snapshot["computed_at"],
        ),
    )
    conn.commit()
    return snapshot


def persist_sleeve_no_pick_reason(
    conn: sqlite3.Connection,
    *,
    snapshot_id: str,
    sleeve_key: str,
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    ensure_candidate_truth_tables(conn)
    operationally_usable = [
        candidate
        for candidate in candidates
        if str(candidate.get("action_readiness") or "") in {"usable_now", "usable_with_limits"}
    ]
    recommendation_ready = [
        candidate for candidate in candidates if str(dict(candidate.get("data_completeness") or {}).get("readiness_level") or "") == "recommendation_ready"
    ]
    if operationally_usable:
        best = operationally_usable[0]
        reason = {
            "snapshot_id": snapshot_id,
            "sleeve_key": sleeve_key,
            "reason_code": "pick_available",
            "reason_text": "At least one candidate is operationally usable now.",
            "nearest_passing_candidate": str(best.get("symbol") or ""),
            "blocking_fields": [],
            "evidence": {
                "operationally_usable_count": len(operationally_usable),
                "recommendation_ready_count": len(recommendation_ready),
            },
        }
    elif recommendation_ready:
        reason = {
            "snapshot_id": snapshot_id,
            "sleeve_key": sleeve_key,
            "reason_code": "pick_available",
            "reason_text": "At least one candidate is recommendation ready.",
            "nearest_passing_candidate": str(recommendation_ready[0].get("symbol") or ""),
            "blocking_fields": [],
            "evidence": {"recommendation_ready_count": len(recommendation_ready)},
        }
    else:
        ranked = sorted(
            candidates,
            key=lambda item: (
                {"recommendation_ready": 0, "shortlist_ready": 1, "review_ready": 2, "research_visible": 3}.get(
                    str(dict(item.get("data_completeness") or {}).get("readiness_level") or "research_visible"),
                    9,
                ),
                len(list(dict(item.get("data_completeness") or {}).get("critical_required_fields_missing") or [])),
                -float(dict(item.get("investment_quality") or {}).get("composite_score") or 0.0),
            ),
        )
        nearest = ranked[0] if ranked else None
        blockers = list(dict(dict(nearest or {}).get("decision_readiness") or {}).get("top_blockers") or [])
        blocker_labels = [str(item.get("label") or "") for item in blockers if str(item.get("label") or "").strip()]
        blocker_text = ", ".join(blocker_labels[:3])
        if any("benchmark" in item.lower() for item in blocker_labels):
            reason_code = "benchmark_readiness"
            reason_text = "No current pick because all candidates fail benchmark readiness."
        elif any("liquidity" in item.lower() or "spread" in item.lower() for item in blocker_labels):
            reason_code = "liquidity_incomplete"
            reason_text = "No current pick because liquidity profiles remain incomplete."
        elif any("tax" in item.lower() or "estate" in item.lower() for item in blocker_labels):
            reason_code = "tax_mechanics_unverified"
            reason_text = "No current pick because SG tax mechanics remain incomplete."
        elif nearest is not None and str(dict(nearest.get("data_completeness") or {}).get("readiness_level") or "") == "review_ready":
            reason_code = "review_ready_only"
            reason_text = "No current pick because all visible candidates are review ready but not recommendation ready."
        else:
            reason_code = "readiness_blocked"
            reason_text = "No current pick because all candidates fail recommendation readiness."
        if nearest is not None and blocker_text:
            reason_text += f" Nearest passing candidate is {nearest.get('symbol')}, blocked by {blocker_text}."
        reason = {
            "snapshot_id": snapshot_id,
            "sleeve_key": sleeve_key,
            "reason_code": reason_code,
            "reason_text": reason_text,
            "nearest_passing_candidate": str(nearest.get("symbol") or "") if nearest else None,
            "blocking_fields": blocker_labels[:6],
            "evidence": {
                "candidate_count": len(candidates),
                "nearest_readiness_level": str(dict(nearest.get("data_completeness") or {}).get("readiness_level") or "") if nearest else None,
            },
        }
    conn.execute(
        """
        INSERT OR REPLACE INTO sleeve_no_pick_reasons (
          snapshot_id, sleeve_key, reason_code, reason_text, nearest_passing_candidate,
          blocking_fields_json, evidence_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            reason["snapshot_id"],
            reason["sleeve_key"],
            reason["reason_code"],
            reason["reason_text"],
            reason["nearest_passing_candidate"],
            _json(reason["blocking_fields"]),
            _json(reason["evidence"]),
        ),
    )
    conn.commit()
    return reason
