from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any


def ensure_candidate_comparison_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS candidate_comparison_snapshots (
          comparison_snapshot_id TEXT PRIMARY KEY,
          sleeve_key TEXT NOT NULL,
          candidate_symbols_json TEXT NOT NULL,
          comparison_json TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_candidate_comparison_snapshots_created
        ON candidate_comparison_snapshots (created_at DESC, sleeve_key)
        """
    )
    conn.commit()


def compare_candidates(*, sleeve_key: str, candidates: list[dict[str, Any]]) -> dict[str, Any]:
    rows = [
        _row("Rank", candidates, lambda c: _q(c, "rank_in_sleeve") or "n/a"),
        _row("Badge", candidates, lambda c: _q(c, "badge") or "n/a"),
        _row("Eligibility", candidates, lambda c: _q(c, "eligibility_state") or "n/a"),
        _row("Composite score", candidates, lambda c: _fmt_num(_q(c, "composite_score"))),
        _row("Data confidence", candidates, lambda c: str(_q(c, "data_confidence") or "unknown")),
        _row("Benchmark", candidates, lambda c: _perf(c, "benchmark_label") or "unknown"),
        _row("Benchmark alignment", candidates, lambda c: _cmp(c, "benchmark_alignment") or "unknown"),
        _row("Benchmark confidence", candidates, lambda c: _perf(c, "benchmark_confidence") or "unknown"),
        _row("Tracking diff 1Y", candidates, lambda c: _fmt_pct(_perf(c, "tracking_difference_1y"))),
        _row("Tracking diff 3Y", candidates, lambda c: _fmt_pct(_perf(c, "tracking_difference_3y"))),
        _row("Tracking diff 5Y", candidates, lambda c: _fmt_pct(_perf(c, "tracking_difference_5y"))),
        _row("Tracking error 1Y", candidates, lambda c: _fmt_pct(_perf(c, "tracking_error_1y"))),
        _row("Return 1Y", candidates, lambda c: _fmt_pct(_perf(c, "return_1y"))),
        _row("Return 3Y", candidates, lambda c: _fmt_pct(_perf(c, "return_3y"))),
        _row("Performance score", candidates, lambda c: _fmt_num(_q(c, "performance_score"))),
        _row("Risk-adjusted score", candidates, lambda c: _fmt_num(_q(c, "risk_adjusted_score"))),
        _row("Cost score", candidates, lambda c: _fmt_num(_q(c, "cost_score"))),
        _row("Liquidity score", candidates, lambda c: _fmt_num(_q(c, "liquidity_score"))),
        _row("Structure score", candidates, lambda c: _fmt_num(_q(c, "structure_score"))),
        _row("Tax score", candidates, lambda c: _fmt_num(_q(c, "tax_score"))),
        _row("AUM", candidates, lambda c: _fmt_dollars((c.get("aum_history_summary") or {}).get("latest_aum_usd") or c.get("aum_usd"))),
        _row("AUM trend", candidates, lambda c: str((c.get("aum_history_summary") or {}).get("trend") or "unknown")),
        _row("30d avg volume", candidates, lambda c: _fmt_num((c.get("market_history_summary") or {}).get("latest_volume_30d_avg") or c.get("volume_30d_avg"))),
        _row("Volume trend", candidates, lambda c: str((c.get("market_history_summary") or {}).get("trend") or "unknown")),
        _row("TER", candidates, lambda c: _fmt_pct(c.get("expense_ratio"))),
        _row("Domicile", candidates, lambda c: str(c.get("domicile") or "unknown")),
        _row("Share class", candidates, lambda c: str(c.get("accumulation_or_distribution") or "unknown")),
        _row("Liquidity status", candidates, lambda c: str(((c.get("investment_lens") or {}).get("liquidity_profile") or {}).get("liquidity_status") or "unknown")),
        _row("Risk controls", candidates, lambda c: str(((c.get("investment_lens") or {}).get("risk_control_summary") or {}).get("status") or "unknown")),
        _row("Decision state", candidates, lambda c: str((c.get("decision_state") or {}).get("status") or "draft")),
        _row("Source gaps", candidates, lambda c: ", ".join(list((c.get("investment_lens") or {}).get("source_gap_highlights") or [])) or "none"),
        _row("Thesis", candidates, lambda c: str(_q(c, "summary") or _q(c, "investment_thesis") or "unknown")),
    ]
    differing = [row for row in rows if len(set(row["values"])) > 1]
    benchmark_labels = {str(_perf(c, "benchmark_label") or "") for c in candidates if str(_perf(c, "benchmark_label") or "").strip()}
    summary = {
        "benchmark_mismatch_count": 0 if len(benchmark_labels) <= 1 else len(benchmark_labels),
        "source_gap_count": sum(len(list((c.get("investment_lens") or {}).get("source_gap_highlights") or [])) for c in candidates),
        "showable_rows": len(differing),
        "candidate_count": len(candidates),
    }
    return {
        "sleeve_key": sleeve_key,
        "candidate_symbols": [str(c.get("symbol") or "") for c in candidates],
        "rows": rows,
        "differing_rows": differing,
        "summary": summary,
    }


def persist_comparison_snapshot(conn: sqlite3.Connection, *, sleeve_key: str, comparison: dict[str, Any]) -> str:
    ensure_candidate_comparison_tables(conn)
    snapshot_id = f"candidate_compare_{uuid.uuid4().hex[:12]}"
    conn.execute(
        """
        INSERT INTO candidate_comparison_snapshots (
          comparison_snapshot_id, sleeve_key, candidate_symbols_json, comparison_json, created_at
        ) VALUES (?, ?, ?, ?, ?)
        """,
        (
            snapshot_id,
            sleeve_key,
            json.dumps(comparison.get("candidate_symbols") or [], sort_keys=True),
            json.dumps(comparison, sort_keys=True),
            datetime.now(UTC).isoformat(),
        ),
    )
    conn.commit()
    return snapshot_id


def list_comparison_snapshots(conn: sqlite3.Connection, *, limit: int = 50) -> list[dict[str, Any]]:
    ensure_candidate_comparison_tables(conn)
    rows = conn.execute(
        """
        SELECT comparison_snapshot_id, sleeve_key, candidate_symbols_json, comparison_json, created_at
        FROM candidate_comparison_snapshots
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (max(1, min(limit, 200)),),
    ).fetchall()
    out: list[dict[str, Any]] = []
    for row in rows:
        payload = dict(row)
        payload["candidate_symbols"] = list(json.loads(str(payload.pop("candidate_symbols_json") or "[]")))
        payload["comparison"] = json.loads(str(payload.pop("comparison_json") or "{}"))
        out.append(payload)
    return out


def _q(candidate: dict[str, Any], key: str) -> Any:
    return dict(candidate.get("investment_quality") or {}).get(key)


def _perf(candidate: dict[str, Any], key: str) -> Any:
    return dict(candidate.get("performance_metrics") or {}).get(key)


def _cmp(candidate: dict[str, Any], key: str) -> Any:
    return dict(dict(candidate.get("investment_quality") or {}).get("comparison_vs_peers") or {}).get(key)


def _row(label: str, candidates: list[dict[str, Any]], getter) -> dict[str, Any]:
    return {"label": label, "values": [getter(candidate) for candidate in candidates]}


def _fmt_num(value: Any) -> str:
    try:
        if value is None or value == "":
            return "unknown"
        return f"{float(value):.2f}"
    except Exception:
        return "unknown"


def _fmt_pct(value: Any) -> str:
    try:
        if value is None or value == "":
            return "unknown"
        return f"{float(value) * 100:.2f}%"
    except Exception:
        return "unknown"


def _fmt_dollars(value: Any) -> str:
    try:
        if value is None or value == "":
            return "unknown"
        amount = float(value)
        if amount >= 1_000_000_000:
            return f"${amount / 1_000_000_000:.2f}B"
        if amount >= 1_000_000:
            return f"${amount / 1_000_000:.1f}M"
        return f"${amount:,.0f}"
    except Exception:
        return "unknown"
