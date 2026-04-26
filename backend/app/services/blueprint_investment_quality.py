from __future__ import annotations

import math
import sqlite3
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from app.config import Settings
from app.services.blueprint_decision_semantics import normalize_benchmark_fit_type
from app.services.blueprint_benchmark_registry import default_benchmark_profile_for_candidate
from app.services.ingest_etf_data import get_etf_factsheet_history_summary, get_preferred_market_history_summary
from app.services.provider_adapters import ProviderAdapterError, fetch_provider_data
from app.services.provider_registry import DATA_FAMILY_ROUTING
from app.services.upstream_truth_contract import normalize_source_state_base

try:
    import yfinance as yf
except ImportError:  # pragma: no cover
    yf = None

V1_MODEL = "quality_v1_existing_data_only"
V2_MODEL = "quality_v2_performance_enabled"

V1_WEIGHTS = {
    "cost": 0.27,
    "liquidity": 0.21,
    "structure": 0.18,
    "tax": 0.20,
    "governance": 0.14,
}
V2_WEIGHTS = {
    "performance": 0.20,
    "risk_adjusted": 0.20,
    "cost": 0.25,
    "liquidity": 0.15,
    "structure": 0.10,
    "tax": 0.10,
}


def _normalized_provider_identifier(identifier: str) -> list[str]:
    raw = str(identifier or "").strip()
    if not raw:
        return []
    candidates = [raw]
    if "." in raw:
        candidates.append(raw.split(".", 1)[0])
    return list(dict.fromkeys([item for item in candidates if item]))


def _history_series_from_payload(series: Any) -> list[tuple[datetime, float]]:
    rows: list[tuple[datetime, float]] = []
    if isinstance(series, dict):
        for date_key, row in series.items():
            if not isinstance(row, dict):
                continue
            close = row.get("4. close") or row.get("close") or row.get("adjClose") or row.get("adj_close")
            try:
                close_value = float(close)
                if not math.isfinite(close_value) or close_value <= 0:
                    continue
                dt = datetime.fromisoformat(str(date_key).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                rows.append((dt.astimezone(UTC), close_value))
            except Exception:
                continue
    elif isinstance(series, list):
        for row in series:
            if not isinstance(row, dict):
                continue
            raw_date = row.get("date") or row.get("datetime")
            close = row.get("close") or row.get("adjClose") or row.get("adj_close") or row.get("c")
            if raw_date in {None, ""} or close in {None, ""}:
                continue
            try:
                close_value = float(close)
                if not math.isfinite(close_value) or close_value <= 0:
                    continue
                raw_text = str(raw_date).replace("Z", "+00:00")
                dt = datetime.fromisoformat(raw_text if "T" in raw_text or "+" in raw_text else f"{raw_text}T00:00:00+00:00")
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                rows.append((dt.astimezone(UTC), close_value))
            except Exception:
                continue
    rows.sort(key=lambda item: item[0])
    return rows


def _fetch_provider_history_series(identifier: str, endpoint_family: str) -> tuple[list[tuple[datetime, float]], str | None, str | None]:
    for provider_name in DATA_FAMILY_ROUTING.get(endpoint_family, []):
        for candidate_identifier in _normalized_provider_identifier(identifier):
            try:
                payload = fetch_provider_data(provider_name, endpoint_family, candidate_identifier)
            except ProviderAdapterError:
                continue
            closes = _history_series_from_payload(payload.get("series"))
            if len(closes) >= 260:
                return closes, str(payload.get("source_ref") or ""), str(provider_name)
    return [], None, None

def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, ddl_fragment: str) -> None:
    rows = conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()
    if any(str(row[1]) == column_name for row in rows):
        return
    conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN {column_name} {ddl_fragment}')


def ensure_quality_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_price_history (
          history_id TEXT PRIMARY KEY,
          symbol TEXT NOT NULL,
          yahoo_ticker TEXT NOT NULL,
          trade_date TEXT NOT NULL,
          adj_close REAL NOT NULL,
          source_url TEXT NOT NULL,
          source_name TEXT NOT NULL,
          retrieved_at TEXT NOT NULL,
          UNIQUE(symbol, trade_date)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_performance_metrics (
          metrics_id TEXT PRIMARY KEY,
          symbol TEXT NOT NULL,
          as_of_date TEXT NOT NULL,
          benchmark_key TEXT,
          benchmark_label TEXT,
          benchmark_proxy_symbol TEXT,
          benchmark_source_type TEXT,
          benchmark_confidence TEXT,
          return_1y REAL,
          return_3y REAL,
          return_5y REAL,
          return_10y REAL,
          benchmark_return_1y REAL,
          benchmark_return_3y REAL,
          benchmark_return_5y REAL,
          benchmark_return_10y REAL,
          tracking_difference_1y REAL,
          tracking_difference_3y REAL,
          tracking_difference_5y REAL,
          tracking_error_1y REAL,
          volatility_1y REAL,
          sharpe_3y REAL,
          sortino_3y REAL,
          max_drawdown_3y REAL,
          observations_count INTEGER,
          source_url TEXT,
          source_name TEXT NOT NULL,
          retrieved_at TEXT NOT NULL,
          UNIQUE(symbol, as_of_date)
        )
        """
    )
    for column_name, ddl_fragment in (
        ("benchmark_key", "TEXT"),
        ("benchmark_label", "TEXT"),
        ("benchmark_proxy_symbol", "TEXT"),
        ("benchmark_source_type", "TEXT"),
        ("benchmark_confidence", "TEXT"),
        ("benchmark_return_1y", "REAL"),
        ("benchmark_return_3y", "REAL"),
        ("benchmark_return_5y", "REAL"),
        ("benchmark_return_10y", "REAL"),
        ("tracking_difference_1y", "REAL"),
        ("tracking_difference_3y", "REAL"),
        ("tracking_difference_5y", "REAL"),
        ("tracking_error_1y", "REAL"),
    ):
        _ensure_column(conn, "candidate_performance_metrics", column_name, ddl_fragment)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_quality_scores (
          score_id TEXT PRIMARY KEY,
          snapshot_id TEXT,
          symbol TEXT NOT NULL,
          sleeve_key TEXT NOT NULL,
          eligibility_state TEXT NOT NULL,
          eligibility_blockers_json TEXT NOT NULL,
          data_confidence TEXT NOT NULL,
          performance_score REAL,
          risk_adjusted_score REAL,
          cost_score REAL,
          liquidity_score REAL,
          structure_score REAL,
          tax_score REAL,
          governance_confidence_score REAL,
          composite_score REAL,
          rank_in_sleeve INTEGER,
          percentile_in_sleeve REAL,
          badge TEXT NOT NULL,
          recommendation_state TEXT NOT NULL,
          investment_thesis TEXT,
          role_in_portfolio TEXT,
          key_advantages_json TEXT NOT NULL,
          key_risks_json TEXT NOT NULL,
          comparison_vs_peers_json TEXT NOT NULL,
          score_provenance_json TEXT NOT NULL,
          score_version TEXT NOT NULL,
          as_of_date TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_candidate_quality_scores_snapshot
        ON candidate_quality_scores (snapshot_id, sleeve_key, rank_in_sleeve)
        """
    )
    conn.commit()


def maybe_refresh_performance_data(
    *,
    conn: sqlite3.Connection,
    candidates: list[dict[str, Any]],
    settings: Settings,
    now: datetime,
) -> None:
    ensure_quality_tables(conn)
    if not bool(getattr(settings, "blueprint_quality_performance_auto_refresh", False)):
        return
    if yf is None:
        return
    refresh_days = 7
    for candidate in candidates:
        symbol = str(candidate.get("symbol") or "").strip().upper()
        instrument_type = str(candidate.get("instrument_type") or "")
        if not symbol or instrument_type not in {"etf_ucits", "etf_us"}:
            continue
        last = conn.execute(
            "SELECT as_of_date FROM candidate_performance_metrics WHERE symbol = ? ORDER BY as_of_date DESC LIMIT 1",
            (symbol,),
        ).fetchone()
        if last is not None:
            try:
                last_dt = datetime.fromisoformat(str(last[0]))
                if last_dt.tzinfo is None:
                    last_dt = last_dt.replace(tzinfo=UTC)
                if now - last_dt <= timedelta(days=refresh_days):
                    continue
            except Exception:
                pass
        _refresh_symbol_performance(conn=conn, candidate=candidate, now=now)


def get_latest_performance_metrics(symbol: str, conn: sqlite3.Connection, candidate: dict[str, Any] | None = None) -> dict[str, Any] | None:
    ensure_quality_tables(conn)
    row = conn.execute(
        """
        SELECT as_of_date, return_1y, return_3y, return_5y, return_10y,
               benchmark_key, benchmark_label, benchmark_proxy_symbol, benchmark_source_type, benchmark_confidence,
               benchmark_return_1y, benchmark_return_3y, benchmark_return_5y, benchmark_return_10y,
               tracking_difference_1y, tracking_difference_3y, tracking_difference_5y, tracking_error_1y,
               volatility_1y, sharpe_3y, sortino_3y, max_drawdown_3y,
               observations_count, source_url, source_name, retrieved_at
        FROM candidate_performance_metrics
        WHERE symbol = ?
        ORDER BY as_of_date DESC
        LIMIT 1
        """,
        (symbol.upper(),),
    ).fetchone()
    metrics = dict(row) if row is not None else None
    factsheet_summary = get_etf_factsheet_history_summary(symbol, conn)
    market_summary = get_preferred_market_history_summary(symbol, conn)
    if metrics is None and factsheet_summary is None and market_summary is None:
        return None
    if metrics is None:
        assignment = _infer_benchmark_profile(candidate or {"symbol": symbol})
        metrics = {
            "as_of_date": (factsheet_summary or market_summary or {}).get("latest_asof_date"),
            "benchmark_key": assignment.get("benchmark_key"),
            "benchmark_label": assignment.get("benchmark_label"),
            "benchmark_proxy_symbol": assignment.get("benchmark_proxy_symbol"),
            "benchmark_source_type": assignment.get("benchmark_source_type"),
            "benchmark_confidence": assignment.get("benchmark_confidence"),
            "source_url": None,
            "source_name": "factsheet_or_market_summary",
            "retrieved_at": (factsheet_summary or market_summary or {}).get("citation", {}).get("retrieved_at"),
        }
    if factsheet_summary:
        for key in ("tracking_difference_1y", "tracking_difference_3y", "tracking_difference_5y", "tracking_error_1y"):
            if metrics.get(key) is None and factsheet_summary.get(key) is not None:
                metrics[key] = factsheet_summary.get(key)
        metrics["aum_usd_latest"] = factsheet_summary.get("latest_aum_usd")
        metrics["aum_usd_average"] = factsheet_summary.get("average_aum_usd")
        metrics["aum_history_points"] = factsheet_summary.get("points")
        metrics["aum_trend"] = factsheet_summary.get("trend")
    if market_summary:
        metrics["volume_30d_avg"] = market_summary.get("latest_volume_30d_avg")
        metrics["volume_30d_avg_average"] = market_summary.get("average_volume_30d_avg")
        metrics["volume_history_points"] = market_summary.get("points")
        metrics["volume_trend"] = market_summary.get("trend")
        metrics["spread_bps_latest"] = market_summary.get("latest_spread_bps")
    candidate_payload = dict(candidate or {})
    for key in ("tracking_difference_1y", "tracking_difference_3y", "tracking_difference_5y"):
        if metrics.get(key) is None and candidate_payload.get(key) is not None:
            try:
                metrics[key] = float(candidate_payload.get(key))
            except Exception:
                pass
    if metrics.get("tracking_difference_1y") is None:
        note_td = _tracking_difference_from_note(candidate_payload)
        if note_td is not None:
            metrics["tracking_difference_1y"] = note_td
    return metrics


def build_investment_quality_score(
    *,
    candidate: dict[str, Any],
    sleeve_key: str,
    sleeve_candidates: list[dict[str, Any]],
    eligibility: dict[str, Any],
    performance_metrics: dict[str, Any] | None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    settings = settings or Settings()
    peer_set = [item for item in sleeve_candidates if str(item.get("symbol") or "").strip()]
    cost_score = _cost_score(candidate, peer_set, performance_metrics)
    liquidity_score = _liquidity_score(candidate, performance_metrics)
    structure_score = _structure_score(candidate)
    tax_score = _tax_score(candidate)
    governance_score = _governance_score(candidate, eligibility)
    performance_score = _performance_score(candidate, performance_metrics, peer_set)
    risk_adjusted_score = _risk_adjusted_score(candidate, performance_metrics, peer_set)
    benchmark_confidence = str((performance_metrics or {}).get("benchmark_confidence") or "")
    benchmark_validation = str(dict(candidate.get("benchmark_assignment") or {}).get("validation_status") or "")
    benchmark_fit_type = normalize_benchmark_fit_type(dict(candidate.get("benchmark_assignment") or {}).get("benchmark_fit_type"))
    benchmark_authority_level = str(dict(candidate.get("benchmark_assignment") or {}).get("benchmark_authority_level") or "insufficient")

    has_performance = (
        performance_score is not None
        and risk_adjusted_score is not None
        and benchmark_confidence not in {"", "unknown", "low"}
        and benchmark_validation not in {"unassigned", "mismatch", "assigned_no_metrics"}
    )
    score_version = V2_MODEL if has_performance else V1_MODEL
    weights = V2_WEIGHTS if has_performance else V1_WEIGHTS
    dimension_meta = _build_dimension_metadata(
        candidate=candidate,
        peers=peer_set,
        eligibility=eligibility,
        performance_metrics=performance_metrics,
        cost_score=cost_score,
        liquidity_score=liquidity_score,
        structure_score=structure_score,
        tax_score=tax_score,
        governance_score=governance_score,
        performance_score=performance_score,
        risk_adjusted_score=risk_adjusted_score,
        has_performance=has_performance,
    )

    components: list[tuple[str, float]] = []
    if has_performance:
        if performance_score is not None:
            components.append(("performance", performance_score))
        if risk_adjusted_score is not None:
            components.append(("risk_adjusted", risk_adjusted_score))
    components.extend([
        ("cost", cost_score or 0.0),
        ("liquidity", liquidity_score or 0.0),
        ("structure", structure_score or 0.0),
        ("tax", tax_score or 0.0),
    ])
    if not has_performance:
        components.append(("governance", governance_score or 0.0))

    total_weight = sum(weights[name] for name, _ in components) or 1.0
    unknown_dimensions = sorted(
        name for name, meta in dimension_meta.items() if str(meta.get("score_state") or "") in {"unknown", "blocked"}
    )
    inferred_dimensions = sorted(
        name for name, meta in dimension_meta.items() if str(meta.get("score_state") or "") == "inferred"
    )
    observed_dimensions = sorted(
        name for name, meta in dimension_meta.items() if str(meta.get("score_state") or "") == "observed"
    )
    weighted_unknown_share = round(
        sum(weights.get(name, 0.0) for name in unknown_dimensions if name in weights) / total_weight,
        4,
    )
    require_liquidity_dimension = bool(getattr(settings, "blueprint_require_liquidity_dimension", True))
    unknown_threshold = int(getattr(settings, "blueprint_max_unknown_dimensions", 1))
    unknown_weight_threshold = float(getattr(settings, "blueprint_max_unknown_weight_share", 0.20))
    liquidity_missing = str(dict(dimension_meta.get("liquidity") or {}).get("score_state") or "") in {"unknown", "blocked"}
    composite_score_valid = (
        len(unknown_dimensions) <= unknown_threshold
        and weighted_unknown_share <= unknown_weight_threshold
        and (not require_liquidity_dimension or not liquidity_missing)
    )
    composite = None
    if composite_score_valid:
        composite_total = 0.0
        for name, value in components:
            composite_total += value * (weights[name] / total_weight)
        composite = composite_total

        if eligibility.get("eligibility_state") == "ineligible":
            composite = min(composite, 30.0)
        elif eligibility.get("eligibility_state") == "data_incomplete":
            composite = min(composite, 49.0)
        elif eligibility.get("eligibility_state") == "eligible_with_caution":
            composite = min(composite, 69.0)

    recommendation_confidence = str(eligibility.get("data_confidence") or "medium")
    if has_performance and benchmark_confidence == "high":
        recommendation_confidence = "high" if recommendation_confidence != "low" else "low"
    elif not has_performance and recommendation_confidence == "high":
        recommendation_confidence = "medium"
    if not composite_score_valid:
        recommendation_confidence = "low"
    if benchmark_fit_type == "acceptable_proxy":
        recommendation_confidence = "medium" if recommendation_confidence == "high" else recommendation_confidence
    elif benchmark_fit_type in {"weak_proxy", "mismatched"} or benchmark_authority_level == "insufficient":
        recommendation_confidence = "low"
    elif benchmark_authority_level == "strong" and recommendation_confidence == "medium" and has_performance:
        recommendation_confidence = "high"

    weighted_completeness_pct = round(max(0.0, (1.0 - weighted_unknown_share)) * 100.0, 2)
    total_dimensions = len(dimension_meta)
    known_dimensions = len(observed_dimensions) + len(inferred_dimensions)
    data_completeness_pct = round((known_dimensions / max(1, total_dimensions)) * 100.0, 2)
    if composite is None:
        composite_display = None
    elif weighted_completeness_pct < 85.0:
        composite = round(composite / 5.0) * 5.0
        composite_display = _banded_label(composite)
    else:
        composite_display = f"{round(composite, 2):.2f}"

    return {
        "symbol": str(candidate.get("symbol") or ""),
        "sleeve_key": str(sleeve_key),
        "eligibility_state": str(eligibility.get("eligibility_state") or "data_incomplete"),
        "eligibility_blockers": list(eligibility.get("eligibility_blockers") or []),
        "data_confidence": str(eligibility.get("data_confidence") or "medium"),
        "performance_score": _round_or_none(performance_score),
        "risk_adjusted_score": _round_or_none(risk_adjusted_score),
        "cost_score": _round_or_none(cost_score),
        "liquidity_score": _round_or_none(liquidity_score),
        "structure_score": _round_or_none(structure_score),
        "tax_score": _round_or_none(tax_score),
        "governance_confidence_score": _round_or_none(governance_score),
        "composite_score": _round_or_none(composite),
        "composite_score_valid": composite_score_valid,
        "composite_score_display": composite_display,
        "rank_in_sleeve": None,
        "percentile_in_sleeve": None,
        "badge": "not_ranked",
        "recommendation_state": "rejected_data_insufficient"
        if (str(eligibility.get("eligibility_state")) in {"ineligible", "data_incomplete"} or not composite_score_valid)
        else "watchlist_only",
        "recommendation_confidence": recommendation_confidence,
        "data_completeness_pct": data_completeness_pct,
        "weighted_data_completeness_pct": weighted_completeness_pct,
        "unknown_dimensions": unknown_dimensions,
        "inferred_dimensions": inferred_dimensions,
        "observed_dimensions": observed_dimensions,
        "investment_thesis": None,
        "role_in_portfolio": str(eligibility.get("role_in_portfolio") or ""),
        "key_advantages": [],
        "key_risks": [],
        "comparison_vs_peers": _comparison_vs_peers(candidate, sleeve_candidates, performance_metrics, cost_score, liquidity_score, structure_score, tax_score, performance_score, risk_adjusted_score),
        "score_provenance": {
            "inputs": {
                "expense_ratio": candidate.get("expense_ratio"),
                "liquidity_profile": dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {}),
                "risk_control_summary": dict(dict(candidate.get("investment_lens") or {}).get("risk_control_summary") or {}),
                "sg_lens": dict(candidate.get("sg_lens") or {}),
                "verification_status": candidate.get("verification_status"),
                "performance_metrics": performance_metrics or {},
                "benchmark_proxy": {
                    "benchmark_key": (performance_metrics or {}).get("benchmark_key"),
                    "benchmark_label": (performance_metrics or {}).get("benchmark_label"),
                    "benchmark_proxy_symbol": (performance_metrics or {}).get("benchmark_proxy_symbol"),
                    "benchmark_source_type": (performance_metrics or {}).get("benchmark_source_type"),
                    "benchmark_confidence": (performance_metrics or {}).get("benchmark_confidence"),
                    "validation_status": dict(candidate.get("benchmark_assignment") or {}).get("validation_status"),
                },
            },
            "active_dimensions": [name for name, _ in components],
            "weights": {name: weights[name] for name, _ in components},
            "dimension_states": dimension_meta,
            "model_derived": True,
            "performance_mode": "enabled" if has_performance else "suppressed_due_to_support",
            "unknown_thresholds": {
                "max_unknown_dimensions": unknown_threshold,
                "max_unknown_weight_share": unknown_weight_threshold,
                "require_liquidity_dimension": require_liquidity_dimension,
            },
        },
        "score_version": score_version,
        "as_of_date": str((performance_metrics or {}).get("as_of_date") or datetime.now(UTC).date().isoformat()),
    }


def _build_dimension_metadata(
    *,
    candidate: dict[str, Any],
    peers: list[dict[str, Any]],
    eligibility: dict[str, Any],
    performance_metrics: dict[str, Any] | None,
    cost_score: float,
    liquidity_score: float,
    structure_score: float,
    tax_score: float,
    governance_score: float,
    performance_score: float | None,
    risk_adjusted_score: float | None,
    has_performance: bool,
) -> dict[str, dict[str, Any]]:
    expense = _safe_float(candidate.get("expense_ratio"))
    peer_expense_count = sum(1 for item in peers if _safe_float(item.get("expense_ratio")) is not None)
    liquidity_profile = dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {})
    liquidity_status = str(liquidity_profile.get("liquidity_status") or "unknown")
    spread_status = str(liquidity_profile.get("spread_status") or "unknown")
    tax_score_input = _safe_float(dict(candidate.get("sg_lens") or {}).get("score"))
    verification_status = str(candidate.get("verification_status") or "unverified")
    risk_summary = str(dict(dict(candidate.get("investment_lens") or {}).get("risk_control_summary") or {}).get("status") or "unknown")

    metadata: dict[str, dict[str, Any]] = {
        "cost": {
            "score_value": _round_or_none(cost_score),
            "score_state": "observed" if expense is not None else "unknown",
            "evidence_count": int(expense is not None) + peer_expense_count,
            "provenance": "candidate.expense_ratio",
            "warning": None if expense is not None else "Expense ratio is unknown, so cost score is not recommendation-authoritative.",
        },
        "liquidity": {
            "score_value": _round_or_none(liquidity_score),
            "score_state": "observed"
            if liquidity_status != "unknown" or spread_status != "unknown"
            else "unknown",
            "evidence_count": sum(
                1
                for value in (
                    liquidity_profile.get("liquidity_status"),
                    liquidity_profile.get("spread_status"),
                    candidate.get("volume_30d_avg"),
                    (performance_metrics or {}).get("volume_30d_avg"),
                )
                if value not in {None, "", "unknown"}
            ),
            "provenance": "investment_lens.liquidity_profile",
            "warning": None
            if liquidity_status != "unknown" or spread_status != "unknown"
            else "Liquidity evidence is incomplete.",
        },
        "structure": {
            "score_value": _round_or_none(structure_score),
            "score_state": "observed",
            "evidence_count": sum(
                1
                for value in (
                    candidate.get("domicile"),
                    candidate.get("accumulation_or_distribution"),
                    candidate.get("replication_method"),
                    risk_summary,
                )
                if value not in {None, "", "unknown"}
            ),
            "provenance": "candidate.structure_profile",
            "warning": None,
        },
        "tax": {
            "score_value": _round_or_none(tax_score),
            "score_state": "observed" if tax_score_input is not None else "unknown",
            "evidence_count": int(tax_score_input is not None),
            "provenance": "candidate.sg_lens",
            "warning": None if tax_score_input is not None else "SG tax lens score is not yet verified.",
        },
        "governance": {
            "score_value": _round_or_none(governance_score),
            "score_state": "observed" if verification_status != "unverified" else "inferred",
            "evidence_count": sum(
                1
                for value in (
                    verification_status,
                    candidate.get("source_state"),
                    candidate.get("freshness_state"),
                    eligibility.get("eligibility_state"),
                )
                if value not in {None, "", "unknown"}
            ),
            "provenance": "candidate.truth_state",
            "warning": "Governance confidence is still partly inferred from source and verification status."
            if verification_status == "unverified"
            else None,
        },
    }
    if has_performance:
        metadata["performance"] = {
            "score_value": _round_or_none(performance_score),
            "score_state": "observed" if performance_score is not None else "unknown",
            "evidence_count": sum(
                1
                for value in (
                    (performance_metrics or {}).get("tracking_difference_3y"),
                    (performance_metrics or {}).get("tracking_difference_1y"),
                    (performance_metrics or {}).get("return_3y"),
                    (performance_metrics or {}).get("return_1y"),
                )
                if value not in {None, ""}
            ),
            "provenance": "candidate.performance_metrics",
            "warning": None if performance_score is not None else "Performance evidence is incomplete for benchmark-relative scoring.",
        }
        metadata["risk_adjusted"] = {
            "score_value": _round_or_none(risk_adjusted_score),
            "score_state": "observed" if risk_adjusted_score is not None else "unknown",
            "evidence_count": sum(
                1
                for value in (
                    (performance_metrics or {}).get("sharpe_3y"),
                    (performance_metrics or {}).get("sortino_3y"),
                    (performance_metrics or {}).get("tracking_error_1y"),
                    (performance_metrics or {}).get("max_drawdown_3y"),
                )
                if value not in {None, ""}
            ),
            "provenance": "candidate.performance_metrics",
            "warning": None if risk_adjusted_score is not None else "Risk-adjusted evidence is incomplete.",
        }
    return metadata


def persist_quality_scores(conn: sqlite3.Connection, *, snapshot_id: str | None, scores: list[dict[str, Any]]) -> None:
    ensure_quality_tables(conn)
    if snapshot_id is not None:
        conn.execute("DELETE FROM candidate_quality_scores WHERE snapshot_id = ?", (snapshot_id,))
    for score in scores:
        conn.execute(
            """
            INSERT INTO candidate_quality_scores (
              score_id, snapshot_id, symbol, sleeve_key, eligibility_state, eligibility_blockers_json,
              data_confidence, performance_score, risk_adjusted_score, cost_score, liquidity_score,
              structure_score, tax_score, governance_confidence_score, composite_score, rank_in_sleeve,
              percentile_in_sleeve, badge, recommendation_state, investment_thesis, role_in_portfolio,
              key_advantages_json, key_risks_json, comparison_vs_peers_json, score_provenance_json,
              score_version, as_of_date, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"candidate_quality_{uuid.uuid4().hex[:12]}",
                snapshot_id,
                score.get("symbol"),
                score.get("sleeve_key"),
                score.get("eligibility_state"),
                _to_json(score.get("eligibility_blockers") or []),
                score.get("data_confidence"),
                score.get("performance_score"),
                score.get("risk_adjusted_score"),
                score.get("cost_score"),
                score.get("liquidity_score"),
                score.get("structure_score"),
                score.get("tax_score"),
                score.get("governance_confidence_score"),
                score.get("composite_score"),
                score.get("rank_in_sleeve"),
                score.get("percentile_in_sleeve"),
                score.get("badge"),
                score.get("recommendation_state"),
                score.get("investment_thesis"),
                score.get("role_in_portfolio"),
                _to_json(score.get("key_advantages") or []),
                _to_json(score.get("key_risks") or []),
                _to_json(score.get("comparison_vs_peers") or {}),
                _to_json(score.get("score_provenance") or {}),
                score.get("score_version"),
                score.get("as_of_date"),
                datetime.now(UTC).isoformat(),
            ),
        )
    conn.commit()


def list_score_models() -> list[dict[str, Any]]:
    return [
        {
            "score_version": V1_MODEL,
            "label": "Existing-data quality model",
            "weights": V1_WEIGHTS,
            "requires": ["expense_ratio", "liquidity_profile", "risk_controls", "verification_status", "sg_lens"],
        },
        {
            "score_version": V2_MODEL,
            "label": "Performance-enabled quality model",
            "weights": V2_WEIGHTS,
            "requires": [
                "trailing_returns",
                "volatility",
                "sharpe",
                "sortino",
                "max_drawdown",
                "expense_ratio",
                "liquidity_profile",
                "risk_controls",
                "sg_lens",
                "benchmark_proxy",
                "benchmark_relative_returns",
                "tracking_error",
            ],
        },
    ]


def _refresh_symbol_performance(*, conn: sqlite3.Connection, candidate: dict[str, Any], now: datetime) -> None:
    symbol = str(candidate.get("symbol") or "").strip().upper()
    ticker = _guess_yahoo_ticker(candidate)
    if not ticker:
        return
    benchmark_profile = _infer_benchmark_profile(candidate)
    closes, source_url, source_name = _fetch_provider_history_series(ticker, "ohlcv_history")
    if not closes and yf is not None:
        try:
            history = yf.Ticker(ticker).history(period="10y", auto_adjust=True)
        except Exception:
            history = None
        if history is not None and not getattr(history, "empty", True):
            for idx, row in history.iterrows():
                value = row.get("Close")
                if value is None or not math.isfinite(float(value)):
                    continue
                date_value = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                if date_value.tzinfo is None:
                    date_value = date_value.replace(tzinfo=UTC)
                closes.append((date_value.astimezone(UTC), float(value)))
            source_url = f"https://finance.yahoo.com/quote/{ticker}/history"
            source_name = "yahoo_finance"
    if len(closes) < 260:
        return
    retrieved_at = now.isoformat()
    benchmark_closes: list[tuple[datetime, float]] = []
    benchmark_url = None
    benchmark_source_name = None
    if benchmark_profile.get("benchmark_proxy_symbol"):
        proxy_symbol = str(benchmark_profile["benchmark_proxy_symbol"])
        benchmark_closes, benchmark_url, benchmark_source_name = _fetch_provider_history_series(proxy_symbol, "benchmark_proxy")
        if not benchmark_closes and yf is not None:
            try:
                benchmark_history = yf.Ticker(proxy_symbol).history(period="10y", auto_adjust=True)
            except Exception:
                benchmark_history = None
            if benchmark_history is not None and not getattr(benchmark_history, "empty", True):
                benchmark_url = f"https://finance.yahoo.com/quote/{proxy_symbol}/history"
                benchmark_source_name = "yahoo_finance"
                for idx, row in benchmark_history.iterrows():
                    value = row.get("Close")
                    if value is None or not math.isfinite(float(value)):
                        continue
                    date_value = idx.to_pydatetime() if hasattr(idx, "to_pydatetime") else idx
                    if date_value.tzinfo is None:
                        date_value = date_value.replace(tzinfo=UTC)
                    benchmark_closes.append((date_value.astimezone(UTC), float(value)))
    for date_value, close in closes[-3000:]:
        conn.execute(
            """
            INSERT OR REPLACE INTO candidate_price_history (
              history_id, symbol, yahoo_ticker, trade_date, adj_close, source_url, source_name, retrieved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"candidate_price_{uuid.uuid4().hex[:12]}",
                symbol,
                ticker,
                date_value.date().isoformat(),
                close,
                source_url,
                source_name or "validated_history_derivation",
                retrieved_at,
            ),
        )
    metrics = _derive_performance_metrics(
        symbol=symbol,
        closes=closes,
        source_url=source_url,
        retrieved_at=retrieved_at,
        benchmark_profile=benchmark_profile,
        benchmark_closes=benchmark_closes,
        benchmark_url=benchmark_url,
        source_name=source_name or "validated_history_derivation",
        benchmark_source_name=benchmark_source_name,
    )
    if metrics:
        conn.execute(
            """
            INSERT OR REPLACE INTO candidate_performance_metrics (
              metrics_id, symbol, as_of_date, benchmark_key, benchmark_label, benchmark_proxy_symbol, benchmark_source_type, benchmark_confidence,
              return_1y, return_3y, return_5y, return_10y,
              benchmark_return_1y, benchmark_return_3y, benchmark_return_5y, benchmark_return_10y,
              tracking_difference_1y, tracking_difference_3y, tracking_difference_5y, tracking_error_1y,
              volatility_1y, sharpe_3y, sortino_3y, max_drawdown_3y, observations_count,
              source_url, source_name, retrieved_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                f"candidate_perf_{uuid.uuid4().hex[:12]}",
                symbol,
                metrics["as_of_date"],
                metrics.get("benchmark_key"),
                metrics.get("benchmark_label"),
                metrics.get("benchmark_proxy_symbol"),
                metrics.get("benchmark_source_type"),
                metrics.get("benchmark_confidence"),
                metrics.get("return_1y"),
                metrics.get("return_3y"),
                metrics.get("return_5y"),
                metrics.get("return_10y"),
                metrics.get("benchmark_return_1y"),
                metrics.get("benchmark_return_3y"),
                metrics.get("benchmark_return_5y"),
                metrics.get("benchmark_return_10y"),
                metrics.get("tracking_difference_1y"),
                metrics.get("tracking_difference_3y"),
                metrics.get("tracking_difference_5y"),
                metrics.get("tracking_error_1y"),
                metrics.get("volatility_1y"),
                metrics.get("sharpe_3y"),
                metrics.get("sortino_3y"),
                metrics.get("max_drawdown_3y"),
                metrics.get("observations_count"),
                benchmark_url or source_url,
                source_name or benchmark_source_name or "validated_history_derivation",
                retrieved_at,
            ),
        )
    conn.commit()

def _derive_performance_metrics(
    *,
    symbol: str,
    closes: list[tuple[datetime, float]],
    source_url: str,
    retrieved_at: str,
    benchmark_profile: dict[str, str],
    benchmark_closes: list[tuple[datetime, float]],
    benchmark_url: str | None,
    source_name: str,
    benchmark_source_name: str | None,
) -> dict[str, Any] | None:
    if len(closes) < 260:
        return None
    dates = [item[0] for item in closes]
    prices = [item[1] for item in closes]
    as_of_date = dates[-1].date().isoformat()
    returns = [(prices[i] / prices[i - 1]) - 1.0 for i in range(1, len(prices)) if prices[i - 1] > 0]
    if len(returns) < 252:
        return None
    metrics: dict[str, Any] = {
        "symbol": symbol,
        "as_of_date": as_of_date,
        "observations_count": len(prices),
        "benchmark_key": benchmark_profile.get("benchmark_key"),
        "benchmark_label": benchmark_profile.get("benchmark_label"),
        "benchmark_proxy_symbol": benchmark_profile.get("benchmark_proxy_symbol"),
        "benchmark_source_type": benchmark_profile.get("benchmark_source_type"),
        "benchmark_confidence": benchmark_profile.get("benchmark_confidence"),
        "history_depth_years": round(len(prices) / 252.0, 2),
        "source_name": source_name,
        "benchmark_history_source_name": benchmark_source_name or source_name,
    }
    for years, key in ((1, "return_1y"), (3, "return_3y"), (5, "return_5y"), (10, "return_10y")):
        lookback = 252 * years
        if len(prices) > lookback:
            start = prices[-lookback - 1]
            end = prices[-1]
            if start > 0:
                metrics[key] = (end / start) - 1.0
    window_1y = returns[-252:]
    if len(window_1y) >= 60:
        metrics["volatility_1y"] = _stddev(window_1y) * math.sqrt(252)
    window_3y = returns[-756:] if len(returns) >= 756 else returns
    if len(window_3y) >= 126:
        avg = sum(window_3y) / len(window_3y)
        std = _stddev(window_3y)
        downside = _stddev([min(0.0, value) for value in window_3y])
        if std and std > 0:
            metrics["sharpe_3y"] = (avg * 252) / (std * math.sqrt(252))
        if downside and downside > 0:
            metrics["sortino_3y"] = (avg * 252) / (downside * math.sqrt(252))
        metrics["max_drawdown_3y"] = _max_drawdown(prices[-(len(window_3y)+1):])
    if benchmark_closes:
        metrics.update(_derive_benchmark_relative_metrics(closes=closes, benchmark_closes=benchmark_closes))
    return metrics


def _derive_benchmark_relative_metrics(
    *,
    closes: list[tuple[datetime, float]],
    benchmark_closes: list[tuple[datetime, float]],
) -> dict[str, Any]:
    benchmark_by_date = {date_value.date().isoformat(): price for date_value, price in benchmark_closes if price > 0}
    aligned: list[tuple[str, float, float]] = []
    for date_value, price in closes:
        benchmark_price = benchmark_by_date.get(date_value.date().isoformat())
        if benchmark_price is None or price <= 0 or benchmark_price <= 0:
            continue
        aligned.append((date_value.date().isoformat(), price, benchmark_price))
    if len(aligned) < 260:
        return {}

    def total_return(series: list[tuple[str, float, float]], years: int, index: int) -> float | None:
        lookback = 252 * years
        if len(series) <= lookback:
            return None
        start = series[-lookback - 1][index]
        end = series[-1][index]
        if start <= 0:
            return None
        return (end / start) - 1.0

    result: dict[str, Any] = {}
    for years, suffix in ((1, "1y"), (3, "3y"), (5, "5y"), (10, "10y")):
        candidate_return = total_return(aligned, years, 1)
        benchmark_return = total_return(aligned, years, 2)
        if benchmark_return is not None:
            result[f"benchmark_return_{suffix}"] = benchmark_return
        if candidate_return is not None and benchmark_return is not None and suffix in {"1y", "3y", "5y"}:
            result[f"tracking_difference_{suffix}"] = candidate_return - benchmark_return

    candidate_daily = []
    benchmark_daily = []
    for idx in range(1, len(aligned)):
        prev = aligned[idx - 1]
        current = aligned[idx]
        if prev[1] > 0 and prev[2] > 0:
            candidate_daily.append((current[1] / prev[1]) - 1.0)
            benchmark_daily.append((current[2] / prev[2]) - 1.0)
    window = min(len(candidate_daily), 252)
    if window >= 60:
        active_returns = [candidate_daily[-window + i] - benchmark_daily[-window + i] for i in range(window)]
        result["tracking_error_1y"] = _stddev(active_returns) * math.sqrt(252)
    return result


def _infer_benchmark_profile(candidate: dict[str, Any]) -> dict[str, str]:
    assignment = dict(candidate.get("benchmark_assignment") or {})
    if assignment.get("benchmark_key"):
        return {
            "benchmark_key": str(assignment.get("benchmark_key") or ""),
            "benchmark_label": str(assignment.get("benchmark_label") or ""),
            "benchmark_proxy_symbol": str(assignment.get("benchmark_proxy_symbol") or ""),
            "benchmark_source_type": str(assignment.get("benchmark_source_type") or ""),
            "benchmark_confidence": str(assignment.get("benchmark_confidence") or "medium"),
        }
    symbol = str(candidate.get("symbol") or "").strip().upper()
    sleeve_key = str(candidate.get("sleeve_key") or "")
    return dict(default_benchmark_profile_for_candidate({"symbol": symbol}, sleeve_key))


def _cost_score(candidate: dict[str, Any], peers: list[dict[str, Any]], performance_metrics: dict[str, Any] | None) -> float | None:
    expense = _safe_float(candidate.get("expense_ratio"))
    peer_values = sorted(v for v in (_safe_float(item.get("expense_ratio")) for item in peers) if v is not None)
    if expense is None:
        return None
    if not peer_values:
        base = 55.0
    else:
        rank = sum(1 for value in peer_values if value <= expense)
        pct = rank / max(1, len(peer_values))
        base = max(0.0, min(100.0, 100.0 - pct * 100.0 + 10.0))
    tracking_penalty = 0.0
    if performance_metrics:
        td = _safe_float(performance_metrics.get("tracking_difference_1y")) or _tracking_difference_from_note(candidate)
        if td is not None:
            tracking_penalty = min(20.0, abs(td) * 10.0)
        te = _safe_float(performance_metrics.get("tracking_error_1y"))
        if te is not None:
            tracking_penalty += min(10.0, abs(te) * 20.0)
    elif candidate.get("tracking_difference_note"):
        td = _tracking_difference_from_note(candidate)
        if td is not None:
            tracking_penalty = min(20.0, abs(td) * 10.0)
    return max(0.0, min(100.0, base - tracking_penalty))


def _liquidity_score(candidate: dict[str, Any], performance_metrics: dict[str, Any] | None) -> float | None:
    profile = dict(dict(candidate.get("investment_lens") or {}).get("liquidity_profile") or {})
    status = str(profile.get("liquidity_status") or "unknown")
    spread = str(profile.get("spread_status") or "unknown")
    market_volume = _safe_float(candidate.get("volume_30d_avg")) or _safe_float((performance_metrics or {}).get("volume_30d_avg"))
    if status == "unknown" and spread == "unknown" and market_volume is None:
        return None
    base = {"strong": 90.0, "adequate": 70.0, "weak": 35.0, "unknown": 50.0}.get(status, 50.0)
    spread_adj = {"tight": 8.0, "acceptable": 0.0, "wide": -18.0, "unknown": -5.0}.get(spread, 0.0)
    if market_volume is not None:
        if market_volume >= 1_000_000:
            base += 5
        elif market_volume < 50_000:
            base -= 7
    return max(0.0, min(100.0, base + spread_adj))


def _structure_score(candidate: dict[str, Any]) -> float:
    score = 70.0
    domicile = str(candidate.get("domicile") or "")
    share_class = str(candidate.get("accumulation_or_distribution") or "")
    replication = str(candidate.get("replication_method") or "unknown").lower()
    instrument_type = str(candidate.get("instrument_type") or "")
    if domicile == "IE":
        score += 8
    elif domicile == "SG":
        score += 5
    elif domicile == "US":
        score -= 8
    if share_class == "accumulating":
        score += 6
    elif share_class == "distributing":
        score -= 3
    if "physical" in replication:
        score += 8
    elif "sampling" in replication:
        score += 3
    elif "synthetic" in replication or "swap" in replication:
        score -= 12
    if bool(candidate.get("policy_placeholder")):
        score -= 10
    if instrument_type == "long_put_overlay_strategy":
        score += 4 if candidate.get("max_loss_known") and not candidate.get("margin_required") else -20
    if bool(candidate.get("margin_required")) or candidate.get("max_loss_known") is False or bool(candidate.get("short_options")):
        score -= 25
    risk_summary = str(dict(dict(candidate.get("investment_lens") or {}).get("risk_control_summary") or {}).get("status") or "unknown")
    score += {"pass": 6, "warn": -4, "fail": -15, "unknown": -3}.get(risk_summary, 0)
    return max(0.0, min(100.0, score))


def _tax_score(candidate: dict[str, Any]) -> float | None:
    sg_lens = dict(candidate.get("sg_lens") or {})
    score = _safe_float(sg_lens.get("score"))
    if score is not None:
        return max(0.0, min(100.0, score))
    return None


def _governance_score(candidate: dict[str, Any], eligibility: dict[str, Any]) -> float:
    verification = str(candidate.get("verification_status") or "unverified")
    source_state = normalize_source_state_base(str(candidate.get("display_source_state") or candidate.get("source_state") or "unknown"))
    freshness = str(eligibility.get("factsheet_freshness") or "unknown")
    source_gaps = len(list(dict(candidate.get("investment_lens") or {}).get("source_gap_highlights") or []))
    comparison_blockers = len(list(dict(dict(candidate.get("investment_lens") or {}).get("comparison_readiness") or {}).get("blockers") or []))
    score = {"verified": 92.0, "partially_verified": 68.0, "unverified": 25.0}.get(verification, 40.0)
    score += {
        "source_validated": 10.0,
        "aging": 4.0,
        "manual_seed": -8.0,
        "stale_live": -15.0,
        "broken_source": -24.0,
        "strategy_placeholder": -24.0,
        "policy_placeholder": -18.0,
        "unknown": -10.0,
    }.get(source_state, -6.0)
    score += {"fresh": 6.0, "aging": 0.0, "stale": -8.0, "quarantined": -20.0, "unknown": -6.0}.get(freshness, 0.0)
    score -= min(18.0, source_gaps * 6.0)
    score -= min(18.0, comparison_blockers * 6.0)
    if str(eligibility.get("eligibility_state") or "") == "ineligible":
        score -= 20.0
    return max(0.0, min(100.0, score))


def _performance_score(candidate: dict[str, Any], performance_metrics: dict[str, Any] | None, peers: list[dict[str, Any]]) -> float | None:
    if not performance_metrics:
        return None
    benchmark_key = str(performance_metrics.get("benchmark_key") or "")
    candidate_td = _safe_float(performance_metrics.get("tracking_difference_3y"))
    if candidate_td is None:
        candidate_td = _safe_float(performance_metrics.get("tracking_difference_1y"))
    if candidate_td is not None:
        same_benchmark = []
        for peer in peers:
            peer_perf = dict(peer.get("performance_metrics") or {})
            if benchmark_key and str(peer_perf.get("benchmark_key") or "") != benchmark_key:
                continue
            peer_td = _safe_float(peer_perf.get("tracking_difference_3y"))
            if peer_td is None:
                peer_td = _safe_float(peer_perf.get("tracking_difference_1y"))
            if peer_td is not None:
                same_benchmark.append(peer_td)
        percentile = _percentile_score(candidate_td, same_benchmark) if same_benchmark else None
        excess_component = max(0.0, min(100.0, 50.0 + candidate_td * 250.0))
        if percentile is not None:
            return round((percentile * 0.65) + (excess_component * 0.35), 2)
        return round(excess_component, 2)
    value = _safe_float(performance_metrics.get("return_3y")) or _safe_float(performance_metrics.get("return_1y"))
    if value is None:
        return None
    peer_values = []
    for peer in peers:
        peer_perf = dict(peer.get("performance_metrics") or {})
        if benchmark_key and str(peer_perf.get("benchmark_key") or "") not in {"", benchmark_key}:
            continue
        peer_val = _safe_float(peer_perf.get("return_3y")) or _safe_float(peer_perf.get("return_1y"))
        if peer_val is not None:
            peer_values.append(peer_val)
    if not peer_values:
        return None
    return _percentile_score(value, peer_values)


def _risk_adjusted_score(candidate: dict[str, Any], performance_metrics: dict[str, Any] | None, peers: list[dict[str, Any]]) -> float | None:
    if not performance_metrics:
        return None
    val = _safe_float(performance_metrics.get("sharpe_3y"))
    if val is None:
        val = _safe_float(performance_metrics.get("sortino_3y"))
    if val is None:
        return None
    peer_values = []
    for peer in peers:
        peer_perf = dict(peer.get("performance_metrics") or {})
        peer_val = _safe_float(peer_perf.get("sharpe_3y")) or _safe_float(peer_perf.get("sortino_3y"))
        if peer_val is not None:
            peer_values.append(peer_val)
    if not peer_values:
        return None
    score = _percentile_score(val, peer_values)
    tracking_error = _safe_float(performance_metrics.get("tracking_error_1y"))
    max_drawdown = _safe_float(performance_metrics.get("max_drawdown_3y"))
    if tracking_error is not None:
        score -= min(12.0, tracking_error * 20.0)
    if max_drawdown is not None:
        score -= min(12.0, abs(max_drawdown) * 15.0)
    return max(0.0, min(100.0, score))


def _comparison_vs_peers(candidate: dict[str, Any], peers: list[dict[str, Any]], performance_metrics: dict[str, Any] | None, cost_score: float, liquidity_score: float, structure_score: float, tax_score: float, performance_score: float | None, risk_adjusted_score: float | None) -> dict[str, Any]:
    return {
        "peer_count": len(peers),
        "benchmark_key": (performance_metrics or {}).get("benchmark_key"),
        "benchmark_label": (performance_metrics or {}).get("benchmark_label"),
        "benchmark_proxy_symbol": (performance_metrics or {}).get("benchmark_proxy_symbol"),
        "benchmark_source_type": (performance_metrics or {}).get("benchmark_source_type"),
        "benchmark_confidence": (performance_metrics or {}).get("benchmark_confidence"),
        "benchmark_alignment": "matched" if (performance_metrics or {}).get("benchmark_key") else "unknown",
        "tracking_difference_1y": (performance_metrics or {}).get("tracking_difference_1y"),
        "tracking_difference_3y": (performance_metrics or {}).get("tracking_difference_3y"),
        "tracking_error_1y": (performance_metrics or {}).get("tracking_error_1y"),
        "cost_position": _relative_label(cost_score),
        "liquidity_position": _relative_label(liquidity_score),
        "structure_position": _relative_label(structure_score),
        "tax_position": _relative_label(tax_score),
        "performance_position": _relative_label(performance_score) if performance_score is not None else "unknown",
        "risk_adjusted_position": _relative_label(risk_adjusted_score) if risk_adjusted_score is not None else "unknown",
        "has_performance_data": performance_metrics is not None,
    }


def _relative_label(value: float | None) -> str:
    if value is None:
        return "unknown"
    if value >= 75:
        return "stronger"
    if value <= 40:
        return "weaker"
    return "middle"


def _percentile_score(value: float, peers: list[float]) -> float:
    if not peers:
        return 50.0
    rank = sum(1 for peer in peers if peer <= value)
    return max(0.0, min(100.0, (rank / max(1, len(peers))) * 100.0))


def _tracking_difference_from_note(candidate: dict[str, Any]) -> float | None:
    note = str(candidate.get("tracking_difference_note") or "")
    if not note:
        return None
    import re
    match = re.search(r"([+-]\d+(?:\.\d+)?)%", note)
    if not match:
        return None
    try:
        return float(match.group(1))
    except Exception:
        return None


def _guess_yahoo_ticker(candidate: dict[str, Any]) -> str | None:
    if candidate.get("yahoo_ticker"):
        return str(candidate.get("yahoo_ticker"))
    symbol = str(candidate.get("symbol") or "").upper()
    domicile = str(candidate.get("domicile") or "").upper()
    instrument_type = str(candidate.get("instrument_type") or "")
    if instrument_type == "etf_us":
        return symbol
    if domicile == "SG":
        return f"{symbol}.SI"
    if instrument_type == "etf_ucits":
        return f"{symbol}.L"
    return None


def _stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((value - mean) ** 2 for value in values) / len(values)
    return math.sqrt(max(0.0, variance))


def _max_drawdown(prices: list[float]) -> float:
    peak = prices[0]
    worst = 0.0
    for price in prices:
        peak = max(peak, price)
        if peak > 0:
            dd = (price / peak) - 1.0
            worst = min(worst, dd)
    return worst


def _round_or_none(value: float | None) -> float | None:
    return None if value is None else round(value, 2)


def _banded_label(value: float | None) -> str | None:
    if value is None:
        return None
    if value >= 80:
        return "80-100 strong"
    if value >= 65:
        return "65-79 acceptable"
    if value >= 50:
        return "50-64 mixed"
    if value >= 35:
        return "35-49 weak"
    return "0-34 constrained"


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except Exception:
        return None


def _to_json(value: Any) -> str:
    import json
    return json.dumps(value, sort_keys=True, ensure_ascii=True)


def prune_candidate_quality_scores(
    conn: sqlite3.Connection,
    *,
    retention_days: int = 14,
) -> int:
    """Delete quality scores older than retention_days, always keeping the latest per (symbol, sleeve_key)."""
    cutoff = (datetime.now(UTC) - timedelta(days=retention_days)).isoformat()
    # Identify the latest score_id per (symbol, sleeve_key) so we never delete it
    keep_rows = conn.execute(
        """
        SELECT score_id FROM candidate_quality_scores
        WHERE score_id IN (
            SELECT score_id FROM (
                SELECT score_id, ROW_NUMBER() OVER (PARTITION BY symbol, sleeve_key ORDER BY created_at DESC) AS rn
                FROM candidate_quality_scores
            ) WHERE rn = 1
        )
        """
    ).fetchall()
    keep_ids = {r[0] for r in keep_rows}

    if keep_ids:
        placeholders = ",".join("?" * len(keep_ids))
        cursor = conn.execute(
            f"DELETE FROM candidate_quality_scores WHERE created_at < ? AND score_id NOT IN ({placeholders})",
            [cutoff, *keep_ids],
        )
    else:
        cursor = conn.execute(
            "DELETE FROM candidate_quality_scores WHERE created_at < ?",
            (cutoff,),
        )
    conn.commit()
    return cursor.rowcount
