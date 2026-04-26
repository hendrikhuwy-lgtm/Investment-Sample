from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import UTC, datetime
from typing import Any

from app.services.blueprint_decision_semantics import classify_benchmark_truth, normalize_benchmark_fit_type

DEFAULT_BENCHMARK_ASSIGNMENTS: dict[str, dict[str, str]] = {
    "VWRA": {"benchmark_key": "FTSE_ALL_WORLD", "benchmark_label": "FTSE All-World proxy", "benchmark_proxy_symbol": "ACWI", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "VWRL": {"benchmark_key": "FTSE_ALL_WORLD", "benchmark_label": "FTSE All-World proxy", "benchmark_proxy_symbol": "ACWI", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "SSAC": {"benchmark_key": "MSCI_ACWI", "benchmark_label": "MSCI ACWI proxy", "benchmark_proxy_symbol": "ACWI", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "IWDA": {"benchmark_key": "MSCI_WORLD", "benchmark_label": "MSCI World proxy", "benchmark_proxy_symbol": "URTH", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "VEVE": {"benchmark_key": "FTSE_DEV_WORLD", "benchmark_label": "Developed world proxy", "benchmark_proxy_symbol": "URTH", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "low"},
    "CSPX": {"benchmark_key": "SP500", "benchmark_label": "S&P 500 ETF proxy", "benchmark_proxy_symbol": "SPY", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "high"},
    "EIMI": {"benchmark_key": "MSCI_EM_IMI", "benchmark_label": "Emerging markets proxy", "benchmark_proxy_symbol": "EEM", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "VFEA": {"benchmark_key": "FTSE_EM", "benchmark_label": "Emerging markets proxy", "benchmark_proxy_symbol": "EEM", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "HMCH": {"benchmark_key": "MSCI_CHINA", "benchmark_label": "China equity proxy", "benchmark_proxy_symbol": "MCHI", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "XCHA": {"benchmark_key": "MSCI_CHINA", "benchmark_label": "China equity proxy", "benchmark_proxy_symbol": "MCHI", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "AGGU": {"benchmark_key": "GLOBAL_AGG_BOND", "benchmark_label": "Global aggregate bond proxy", "benchmark_proxy_symbol": "BNDW", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "low"},
    "VAGU": {"benchmark_key": "GLOBAL_AGG_BOND_HDG", "benchmark_label": "Global aggregate bond proxy", "benchmark_proxy_symbol": "BNDW", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "low"},
    "A35": {"benchmark_key": "SGD_GOV_BOND", "benchmark_label": "SGD bond proxy", "benchmark_proxy_symbol": "MBH.SI", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "low"},
    "IB01": {"benchmark_key": "SHORT_TBILL", "benchmark_label": "0-1Y Treasury bills proxy", "benchmark_proxy_symbol": "SHV", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "BIL": {"benchmark_key": "SHORT_TBILL", "benchmark_label": "1-3M Treasury bills proxy", "benchmark_proxy_symbol": "BIL", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "high"},
    "SGOV": {"benchmark_key": "SHORT_TBILL", "benchmark_label": "0-3M Treasury bills proxy", "benchmark_proxy_symbol": "SGOV", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "high"},
    "BILS": {"benchmark_key": "SHORT_TBILL", "benchmark_label": "3-12M Treasury bills proxy", "benchmark_proxy_symbol": "SHV", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "SGLN": {"benchmark_key": "GOLD", "benchmark_label": "Gold proxy", "benchmark_proxy_symbol": "GLD", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "medium"},
    "CMOD": {"benchmark_key": "BROAD_COMMODITIES", "benchmark_label": "Broad commodities proxy", "benchmark_proxy_symbol": "DBC", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "low"},
    "IWDP": {"benchmark_key": "GLOBAL_REITS", "benchmark_label": "Global REIT proxy", "benchmark_proxy_symbol": "REET", "benchmark_source_type": "proxy_etf", "benchmark_confidence": "low"},
}

CANONICAL_BENCHMARK_FULL_NAMES: dict[str, str] = {
    "FTSE_ALL_WORLD": "FTSE All-World Index",
    "MSCI_ACWI": "MSCI ACWI Index",
    "MSCI_WORLD": "MSCI World Index",
    "FTSE_DEV_WORLD": "FTSE Developed World Index",
    "SP500": "S&P 500 Index",
    "MSCI_EM_IMI": "MSCI Emerging Markets Investable Market Index",
    "FTSE_EM": "FTSE Emerging Index",
    "MSCI_CHINA": "MSCI China Index",
    "BROAD_COMMODITIES": "Bloomberg Commodity Index",
    "GLOBAL_REITS": "FTSE EPRA Nareit Developed Dividend+ Index",
}

DEFAULT_SLEEVE_ASSIGNMENTS: dict[str, str] = {
    "global_equity_core": "VWRA",
    "developed_ex_us_optional": "IWDA",
    "emerging_markets": "EIMI",
    "china_satellite": "HMCH",
    "ig_bonds": "AGGU",
    "cash_bills": "IB01",
    "real_assets": "SGLN",
    "alternatives": "SGLN",
}

BENCHMARK_OPTIONAL_SLEEVES = {"alternatives", "convex"}


def canonical_benchmark_full_name(benchmark_key: str | None, fallback_label: str | None = None) -> str | None:
    key = str(benchmark_key or "").strip().upper()
    if key and key in CANONICAL_BENCHMARK_FULL_NAMES:
        return CANONICAL_BENCHMARK_FULL_NAMES[key]
    label = str(fallback_label or "").strip()
    if label and "proxy" not in label.lower():
        return label
    return None


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def ensure_benchmark_registry_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_benchmark_registry (
          registry_id TEXT PRIMARY KEY,
          scope_type TEXT NOT NULL,
          scope_value TEXT NOT NULL,
          benchmark_key TEXT NOT NULL,
          benchmark_label TEXT NOT NULL,
          benchmark_proxy_symbol TEXT,
          benchmark_source_type TEXT,
          benchmark_source TEXT,
          benchmark_confidence TEXT NOT NULL DEFAULT 'medium',
          allowed_proxy_flag INTEGER NOT NULL DEFAULT 1,
          methodology_notes TEXT,
          rationale TEXT,
          validation_rules_json TEXT NOT NULL DEFAULT '[]',
          status TEXT NOT NULL DEFAULT 'active',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_benchmark_registry_scope
        ON blueprint_benchmark_registry (scope_type, scope_value)
        """
    )
    existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(blueprint_benchmark_registry)").fetchall()}
    for column, ddl in (
        ("benchmark_source", "TEXT"),
        ("allowed_proxy_flag", "INTEGER NOT NULL DEFAULT 1"),
        ("methodology_notes", "TEXT"),
    ):
        if column not in existing:
            conn.execute(f"ALTER TABLE blueprint_benchmark_registry ADD COLUMN {column} {ddl}")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_candidate_benchmark_assignments (
          assignment_id TEXT PRIMARY KEY,
          sleeve_key TEXT NOT NULL,
          candidate_symbol TEXT NOT NULL,
          benchmark_key TEXT NOT NULL,
          benchmark_label TEXT NOT NULL,
          benchmark_proxy_symbol TEXT,
          benchmark_source_type TEXT,
          benchmark_confidence TEXT NOT NULL DEFAULT 'medium',
          assignment_source TEXT NOT NULL,
          validation_status TEXT NOT NULL DEFAULT 'assigned',
          validation_notes_json TEXT NOT NULL DEFAULT '[]',
          updated_at TEXT NOT NULL,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_candidate_benchmark_assignments_symbol
        ON blueprint_candidate_benchmark_assignments (sleeve_key, candidate_symbol)
        """
    )
    conn.commit()


def sync_default_benchmark_registry(conn: sqlite3.Connection) -> None:
    ensure_benchmark_registry_tables(conn)
    existing_scopes = {
        (str(row["scope_type"] or ""), str(row["scope_value"] or ""))
        for row in conn.execute(
            """
            SELECT scope_type, scope_value
            FROM blueprint_benchmark_registry
            WHERE status = 'active'
            """
        ).fetchall()
    }
    expected_scopes = {
        *{("symbol", symbol) for symbol in DEFAULT_BENCHMARK_ASSIGNMENTS},
        *{("sleeve", sleeve_key) for sleeve_key in DEFAULT_SLEEVE_ASSIGNMENTS},
    }
    if expected_scopes.issubset(existing_scopes):
        return
    now = _now_iso()
    for symbol, assignment in DEFAULT_BENCHMARK_ASSIGNMENTS.items():
        if ("symbol", symbol) in existing_scopes:
            continue
        existing = conn.execute(
            """
            SELECT registry_id, created_at
            FROM blueprint_benchmark_registry
            WHERE scope_type = 'symbol' AND scope_value = ?
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        conn.execute(
            """
            INSERT OR REPLACE INTO blueprint_benchmark_registry (
              registry_id, scope_type, scope_value, benchmark_key, benchmark_label,
              benchmark_proxy_symbol, benchmark_source_type, benchmark_source, benchmark_confidence,
              allowed_proxy_flag, methodology_notes, rationale, validation_rules_json, status, created_at, updated_at
            ) VALUES (?, 'symbol', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
            """,
            (
                str(existing["registry_id"]) if existing is not None else f"bp_benchmark_registry_{uuid.uuid4().hex[:12]}",
                symbol,
                assignment["benchmark_key"],
                assignment["benchmark_label"],
                assignment.get("benchmark_proxy_symbol"),
                assignment.get("benchmark_source_type"),
                "default_registry_seed",
                assignment.get("benchmark_confidence", "medium"),
                1,
                "Proxy benchmark assignment seeded from default benchmark registry until explicit benchmark history is ingested.",
                f"Default explicit benchmark assignment for {symbol}.",
                json.dumps(["candidate_symbol_match", "benchmark_proxy_present"], sort_keys=True),
                str(existing["created_at"]) if existing is not None else now,
                now,
            ),
        )
    for sleeve_key, symbol in DEFAULT_SLEEVE_ASSIGNMENTS.items():
        if ("sleeve", sleeve_key) in existing_scopes:
            continue
        assignment = DEFAULT_BENCHMARK_ASSIGNMENTS[symbol]
        existing = conn.execute(
            """
            SELECT registry_id, created_at
            FROM blueprint_benchmark_registry
            WHERE scope_type = 'sleeve' AND scope_value = ?
            LIMIT 1
            """,
            (sleeve_key,),
        ).fetchone()
        conn.execute(
            """
            INSERT OR REPLACE INTO blueprint_benchmark_registry (
              registry_id, scope_type, scope_value, benchmark_key, benchmark_label,
              benchmark_proxy_symbol, benchmark_source_type, benchmark_source, benchmark_confidence,
              allowed_proxy_flag, methodology_notes, rationale, validation_rules_json, status, created_at, updated_at
            ) VALUES (?, 'sleeve', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
            """,
            (
                str(existing["registry_id"]) if existing is not None else f"bp_benchmark_registry_{uuid.uuid4().hex[:12]}",
                sleeve_key,
                assignment["benchmark_key"],
                assignment["benchmark_label"],
                assignment.get("benchmark_proxy_symbol"),
                assignment.get("benchmark_source_type"),
                "default_registry_seed",
                assignment.get("benchmark_confidence", "medium"),
                1,
                "Sleeve-level proxy benchmark seeded until explicit candidate mapping is validated.",
                f"Default sleeve-level benchmark assignment for {sleeve_key}.",
                json.dumps(["sleeve_default", "benchmark_proxy_present"], sort_keys=True),
                str(existing["created_at"]) if existing is not None else now,
                now,
            ),
        )
    conn.commit()


def default_benchmark_profile_for_candidate(candidate: dict[str, Any], sleeve_key: str) -> dict[str, Any]:
    symbol = str(candidate.get("symbol") or "").strip().upper()
    if symbol in DEFAULT_BENCHMARK_ASSIGNMENTS:
        return dict(DEFAULT_BENCHMARK_ASSIGNMENTS[symbol])
    sleeve_symbol = DEFAULT_SLEEVE_ASSIGNMENTS.get(sleeve_key)
    if sleeve_symbol and sleeve_symbol in DEFAULT_BENCHMARK_ASSIGNMENTS:
        return dict(DEFAULT_BENCHMARK_ASSIGNMENTS[sleeve_symbol])
    return {}


def resolve_benchmark_assignment(conn: sqlite3.Connection, *, candidate: dict[str, Any], sleeve_key: str) -> dict[str, Any]:
    sync_default_benchmark_registry(conn)
    symbol = str(candidate.get("symbol") or "").strip().upper()
    explicit_benchmark_key = str(candidate.get("benchmark_key") or dict(candidate.get("extra") or {}).get("benchmark_key") or "").strip()
    if explicit_benchmark_key:
        seeded = dict(DEFAULT_BENCHMARK_ASSIGNMENTS.get(symbol) or {})
        return {
            "benchmark_key": explicit_benchmark_key,
            "benchmark_label": seeded.get("benchmark_label") or explicit_benchmark_key.replace("_", " "),
            "benchmark_proxy_symbol": seeded.get("benchmark_proxy_symbol"),
            "benchmark_source_type": seeded.get("benchmark_source_type") or "registry_explicit",
            "benchmark_source": "candidate_registry",
            "benchmark_confidence": "high" if sleeve_key in DEFAULT_SLEEVE_ASSIGNMENTS else str(seeded.get("benchmark_confidence") or "medium"),
            "allowed_proxy_flag": True,
            "methodology_notes": "Explicit benchmark assignment from candidate registry; return history may still rely on approved proxy market sources until direct benchmark history is ingested.",
            "rationale": f"Explicit benchmark key stored in candidate registry for {symbol}.",
            "validation_rules": ["candidate_registry_benchmark_key"],
            "assignment_source": "candidate_registry_explicit",
            "validation_status": "assigned",
            "validation_notes": [],
        }
    row = conn.execute(
        """
        SELECT benchmark_key, benchmark_label, benchmark_proxy_symbol, benchmark_source_type,
               benchmark_confidence, assignment_source, validation_status, validation_notes_json
        FROM blueprint_candidate_benchmark_assignments
        WHERE sleeve_key = ? AND candidate_symbol = ?
        LIMIT 1
        """,
        (sleeve_key, symbol),
    ).fetchone()
    source = "explicit_candidate"
    if row is None:
        row = conn.execute(
            """
            SELECT benchmark_key, benchmark_label, benchmark_proxy_symbol, benchmark_source_type,
                   benchmark_source, benchmark_confidence, allowed_proxy_flag, methodology_notes, rationale, validation_rules_json
            FROM blueprint_benchmark_registry
            WHERE scope_type = 'symbol' AND scope_value = ? AND status = 'active'
            LIMIT 1
            """,
            (symbol,),
        ).fetchone()
        source = "registry_symbol"
    if row is None:
        row = conn.execute(
            """
            SELECT benchmark_key, benchmark_label, benchmark_proxy_symbol, benchmark_source_type,
                   benchmark_source, benchmark_confidence, allowed_proxy_flag, methodology_notes, rationale, validation_rules_json
            FROM blueprint_benchmark_registry
            WHERE scope_type = 'sleeve' AND scope_value = ? AND status = 'active'
            LIMIT 1
            """,
            (sleeve_key,),
        ).fetchone()
        source = "registry_sleeve"
    if row is None:
        return {
            "assignment_source": "unassigned",
            "validation_status": "unassigned",
            "validation_notes": ["No benchmark assignment found for candidate or sleeve."],
        }
    if source == "explicit_candidate":
        return {
            "benchmark_key": str(row["benchmark_key"]),
            "benchmark_label": str(row["benchmark_label"]),
            "benchmark_proxy_symbol": str(row["benchmark_proxy_symbol"] or "") or None,
            "benchmark_source_type": str(row["benchmark_source_type"] or "") or None,
            "benchmark_source": None,
            "benchmark_confidence": str(row["benchmark_confidence"] or "medium"),
            "allowed_proxy_flag": True,
            "methodology_notes": None,
            "rationale": "",
            "validation_rules": [],
            "assignment_source": str(row["assignment_source"] or "explicit_candidate"),
            "validation_status": str(row["validation_status"] or "assigned"),
            "validation_notes": list(json.loads(str(row["validation_notes_json"] or "[]"))),
        }
    return {
        "benchmark_key": str(row["benchmark_key"]),
        "benchmark_label": str(row["benchmark_label"]),
        "benchmark_proxy_symbol": str(row["benchmark_proxy_symbol"] or "") or None,
        "benchmark_source_type": str(row["benchmark_source_type"] or "") or None,
        "benchmark_source": str(row["benchmark_source"] or "") or None,
        "benchmark_confidence": str(row["benchmark_confidence"] or "medium"),
        "allowed_proxy_flag": bool(row["allowed_proxy_flag"]),
        "methodology_notes": str(row["methodology_notes"] or "") or None,
        "rationale": str(row["rationale"] or ""),
        "validation_rules": list(json.loads(str(row["validation_rules_json"] or "[]"))),
        "assignment_source": source,
        "validation_status": "assigned",
        "validation_notes": [],
    }


def validate_benchmark_assignment(assignment: dict[str, Any], performance_metrics: dict[str, Any] | None) -> dict[str, Any]:
    out = dict(assignment or {})
    notes = list(out.get("validation_notes") or [])
    if not out.get("benchmark_key"):
        out["validation_status"] = "unassigned"
        notes.append("Benchmark key missing.")
        out["validation_notes"] = notes
        return out
    if not performance_metrics:
        out["validation_status"] = "assigned_no_metrics"
        notes.append("Performance metrics unavailable for benchmark validation.")
        out["validation_notes"] = notes
        return out
    metric_key = str(performance_metrics.get("benchmark_key") or "")
    metric_proxy = str(performance_metrics.get("benchmark_proxy_symbol") or "")
    if metric_key and metric_key == str(out.get("benchmark_key") or ""):
        out["validation_status"] = "matched"
    elif metric_proxy and metric_proxy == str(out.get("benchmark_proxy_symbol") or ""):
        out["validation_status"] = "proxy_matched" if bool(out.get("allowed_proxy_flag", True)) else "proxy_disallowed"
        notes.append(
            "Benchmark proxy matched, but benchmark key was not explicit in the metrics row."
            if bool(out.get("allowed_proxy_flag", True))
            else "Only a proxy benchmark matched and this candidate is not allowed to rely on proxy mapping."
        )
    else:
        out["validation_status"] = "mismatch"
        notes.append("Stored benchmark metrics do not match the assigned benchmark registry entry.")
    out["validation_notes"] = notes
    return out


def enrich_benchmark_assignment(assignment: dict[str, Any], *, sleeve_key: str) -> dict[str, Any]:
    out = dict(assignment or {})
    assignment_source = str(out.get("assignment_source") or "unassigned")
    source_type = str(out.get("benchmark_source_type") or "")
    validation_status = str(out.get("validation_status") or "unassigned")
    confidence = str(out.get("benchmark_confidence") or "unknown")
    benchmark_key = str(out.get("benchmark_key") or "")
    benchmark_label = str(out.get("benchmark_label") or benchmark_key.replace("_", " ") or "")
    proxy_symbol = str(out.get("benchmark_proxy_symbol") or "")
    allowed_proxy = bool(out.get("allowed_proxy_flag", True))
    notes = [str(item) for item in list(out.get("validation_notes") or []) if str(item).strip()]
    methodology = str(out.get("methodology_notes") or "").strip()
    rationale = str(out.get("rationale") or "").strip()

    if assignment_source in {"candidate_registry_explicit", "explicit_candidate"}:
        benchmark_kind = "direct"
    elif assignment_source == "registry_sleeve":
        benchmark_kind = "sleeve_default"
    elif source_type in {"proxy_etf", "proxy_index"} or validation_status in {"proxy_matched", "proxy_disallowed"}:
        benchmark_kind = "proxy"
    elif benchmark_key:
        benchmark_kind = "direct"
    else:
        benchmark_kind = "unassigned"

    out["benchmark_kind"] = benchmark_kind
    benchmark_truth = classify_benchmark_truth(assignment=out, sleeve_key=sleeve_key)
    benchmark_fit_type = normalize_benchmark_fit_type(benchmark_truth.get("benchmark_fit_type"))
    benchmark_authority_level = str(benchmark_truth.get("benchmark_authority_level") or "insufficient")
    benchmark_role = str(benchmark_truth.get("benchmark_role") or "supporting_anchor")

    if benchmark_role in {"context_only", "not_decisive"}:
        benchmark_effect_type = "benchmark_not_decisive"
    elif benchmark_fit_type == "strong_fit":
        benchmark_effect_type = "benchmark_fit_strong"
    elif benchmark_fit_type == "acceptable_proxy":
        benchmark_effect_type = "benchmark_fit_proxy_acceptable"
    elif benchmark_fit_type == "mismatched":
        benchmark_effect_type = "benchmark_not_decisive" if sleeve_key in BENCHMARK_OPTIONAL_SLEEVES else "benchmark_fit_weak"
    elif validation_status in {"unassigned", "assigned_no_metrics"}:
        benchmark_effect_type = "benchmark_data_incomplete"
    else:
        benchmark_effect_type = "benchmark_fit_weak"

    effect_label_map = {
        "benchmark_fit_strong": "Benchmark fit strong",
        "benchmark_fit_proxy_acceptable": "Benchmark fit acceptable but proxy-based",
        "benchmark_fit_weak": "Benchmark fit weak",
        "benchmark_not_decisive": "Benchmark not decisive",
        "benchmark_data_incomplete": "Benchmark data incomplete",
    }
    confidence_meaning = {
        "high": "High confidence means the benchmark match is explicit enough to support a strong relative comparison.",
        "medium": "Medium confidence means the benchmark is usable for comparison, but proxy use or validation limits still matter.",
        "low": "Low confidence means benchmark-relative evidence is only directional and should not drive the decision on its own.",
        "unknown": "Benchmark confidence is not yet established strongly enough to support a high-conviction comparison.",
    }.get(confidence, f"{confidence.title()} confidence means benchmark support remains constrained.")

    if benchmark_kind == "proxy":
        proxy_usage_explanation = (
            f"Comparison currently uses {proxy_symbol or 'an approved proxy series'} because direct benchmark history is still incomplete."
        )
    elif benchmark_kind == "sleeve_default":
        proxy_usage_explanation = (
            "The sleeve-default benchmark is being used because candidate-specific benchmark evidence is not yet explicit enough."
        )
    elif benchmark_kind == "direct":
        proxy_usage_explanation = "A direct benchmark assignment is available, so comparison does not have to rely on a sleeve-default proxy."
    else:
        proxy_usage_explanation = "A reliable benchmark mapping is not yet available."

    if benchmark_effect_type == "benchmark_fit_strong":
        confidence_effect = "Benchmark support strengthens recommendation confidence when the rest of the implementation case also clears."
    elif benchmark_effect_type == "benchmark_fit_proxy_acceptable":
        confidence_effect = "Benchmark support is usable, but proxy reliance means the comparison should not be treated as fully decisive."
    elif benchmark_effect_type == "benchmark_fit_weak":
        confidence_effect = "Weak benchmark fit reduces recommendation confidence and can keep the candidate in review rather than active selection."
    elif benchmark_effect_type == "benchmark_not_decisive":
        confidence_effect = "Benchmark evidence is secondary for this sleeve and should not override structure, governance, or scenario-role requirements."
    else:
        confidence_effect = "Benchmark evidence is still incomplete, so confidence has to rely more heavily on non-benchmark evidence."
    confidence_effect = str(benchmark_truth.get("benchmark_truth_summary") or confidence_effect)

    method_map = {
        "candidate_registry_explicit": "candidate_registry_explicit",
        "explicit_candidate": "candidate_assignment_record",
        "registry_symbol": "candidate_symbol_registry",
        "registry_sleeve": "sleeve_default_registry",
        "unassigned": "unassigned",
    }
    assignment_method = method_map.get(assignment_source, assignment_source or "unassigned")

    if benchmark_kind == "direct":
        why_this_benchmark = f"{benchmark_label or benchmark_key} is used because this candidate has an explicit benchmark mapping."
    elif benchmark_kind == "proxy":
        why_this_benchmark = f"{benchmark_label or benchmark_key} is used as the closest approved proxy for relative comparison."
    elif benchmark_kind == "sleeve_default":
        why_this_benchmark = f"{benchmark_label or benchmark_key} is being used as the sleeve's default comparison anchor until candidate-specific support improves."
    else:
        why_this_benchmark = "No benchmark is currently decisive because assignment or validation is still incomplete."

    explanation_parts = [why_this_benchmark, confidence_meaning, proxy_usage_explanation, confidence_effect]
    evidence_basis = [
        "benchmark_assignment.assignment_source",
        "benchmark_assignment.validation_status",
        "benchmark_assignment.benchmark_confidence",
    ]
    missing_inputs: list[str] = []
    if not benchmark_key:
        missing_inputs.append("benchmark_assignment.benchmark_key")
    if validation_status in {"unassigned", "assigned_no_metrics"}:
        missing_inputs.append("benchmark_assignment.validation_status")
    if confidence in {"", "unknown"}:
        missing_inputs.append("benchmark_assignment.benchmark_confidence")
    if methodology:
        explanation_parts.append(methodology)
        evidence_basis.append("benchmark_assignment.methodology_notes")
    if rationale:
        explanation_parts.append(rationale)
        evidence_basis.append("benchmark_assignment.rationale")
    if notes:
        explanation_parts.append(f"Additional benchmark notes: {'; '.join(notes[:3])}.")
        evidence_basis.append("benchmark_assignment.validation_notes")

    out.update(
        {
            "benchmark_fit_type": benchmark_fit_type,
            "benchmark_authority_level": benchmark_authority_level,
            "supports_fair_comparison": bool(benchmark_truth.get("supports_fair_comparison")),
            "benchmark_role": benchmark_role,
            "benchmark_kind": benchmark_kind,
            "benchmark_effect_type": benchmark_effect_type,
            "benchmark_effect_label": effect_label_map[benchmark_effect_type],
            "benchmark_assignment_method": assignment_method,
            "benchmark_confidence_meaning": confidence_meaning,
            "proxy_usage_explanation": proxy_usage_explanation,
            "recommendation_confidence_effect": confidence_effect,
            "why_this_benchmark": why_this_benchmark,
            "benchmark_explanation": " ".join(part.strip() for part in explanation_parts if part.strip()),
            "evidence_basis": list(dict.fromkeys(evidence_basis)),
            "missing_inputs": list(dict.fromkeys(missing_inputs)),
        }
    )
    return out


def upsert_candidate_benchmark_assignment(conn: sqlite3.Connection, *, sleeve_key: str, candidate_symbol: str, assignment: dict[str, Any]) -> None:
    ensure_benchmark_registry_tables(conn)
    if not str(assignment.get("benchmark_key") or "").strip():
        return
    now = _now_iso()
    symbol = candidate_symbol.upper()
    existing = conn.execute(
        """
        SELECT assignment_id, created_at
        FROM blueprint_candidate_benchmark_assignments
        WHERE sleeve_key = ? AND candidate_symbol = ?
        LIMIT 1
        """,
        (sleeve_key, symbol),
    ).fetchone()
    conn.execute(
        """
        INSERT INTO blueprint_candidate_benchmark_assignments (
          assignment_id, sleeve_key, candidate_symbol, benchmark_key, benchmark_label,
          benchmark_proxy_symbol, benchmark_source_type, benchmark_confidence,
          assignment_source, validation_status, validation_notes_json, updated_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(sleeve_key, candidate_symbol) DO UPDATE SET
          benchmark_key = excluded.benchmark_key,
          benchmark_label = excluded.benchmark_label,
          benchmark_proxy_symbol = excluded.benchmark_proxy_symbol,
          benchmark_source_type = excluded.benchmark_source_type,
          benchmark_confidence = excluded.benchmark_confidence,
          assignment_source = excluded.assignment_source,
          validation_status = excluded.validation_status,
          validation_notes_json = excluded.validation_notes_json,
          updated_at = excluded.updated_at
        """,
        (
            str(existing["assignment_id"]) if existing is not None else f"bp_benchmark_assignment_{uuid.uuid4().hex[:12]}",
            sleeve_key,
            symbol,
            assignment.get("benchmark_key"),
            assignment.get("benchmark_label"),
            assignment.get("benchmark_proxy_symbol"),
            assignment.get("benchmark_source_type"),
            assignment.get("benchmark_confidence", "medium"),
            assignment.get("assignment_source", "registry"),
            assignment.get("validation_status", "assigned"),
            json.dumps(assignment.get("validation_notes") or [], sort_keys=True),
            now,
            str(existing["created_at"]) if existing is not None else now,
        ),
    )
    conn.commit()


def list_benchmark_registry(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    sync_default_benchmark_registry(conn)
    rows = conn.execute(
        """
        SELECT registry_id, scope_type, scope_value, benchmark_key, benchmark_label,
               benchmark_proxy_symbol, benchmark_source_type, benchmark_source, benchmark_confidence,
               allowed_proxy_flag, methodology_notes, rationale, validation_rules_json, status, created_at, updated_at
        FROM blueprint_benchmark_registry
        WHERE status = 'active'
        ORDER BY scope_type, scope_value
        """
    ).fetchall()
    return [
        {
            **dict(row),
            "allowed_proxy_flag": bool(row["allowed_proxy_flag"]),
            "validation_rules": list(json.loads(str(row["validation_rules_json"] or "[]"))),
        }
        for row in rows
    ]


def build_benchmark_registry_summary(conn: sqlite3.Connection, *, sleeves: list[dict[str, Any]]) -> dict[str, Any]:
    registry = list_benchmark_registry(conn)
    explicit = 0
    matched = 0
    mismatched = 0
    assigned_no_metrics = 0
    unresolved = 0
    total = 0
    for sleeve in sleeves:
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        for candidate in list(sleeve.get("candidates") or []):
            total += 1
            assignment = dict(candidate.get("benchmark_assignment") or {})
            status = str(assignment.get("validation_status") or "unassigned")
            if str(assignment.get("assignment_source") or "").startswith("explicit"):
                explicit += 1
            if status in {"matched", "proxy_matched"}:
                matched += 1
            elif status == "mismatch":
                mismatched += 1
            elif status == "assigned_no_metrics":
                assigned_no_metrics += 1
            else:
                unresolved += 1
    return {
        "registry_count": len(registry),
        "candidate_count": total,
        "explicit_assignment_count": explicit,
        "matched_assignment_count": matched,
        "mismatched_assignment_count": mismatched,
        "assigned_without_metrics_count": assigned_no_metrics,
        "unresolved_assignment_count": unresolved,
        "entries": registry,
    }
