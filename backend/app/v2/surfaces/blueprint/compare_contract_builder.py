from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from typing import Any

from app.config import get_db_path
from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, export_live_candidate_registry, seed_default_candidate_registry
from app.v2.core.change_ledger import record_change
from app.v2.blueprint_market import build_candidate_market_path_support
from app.v2.donors.portfolio_truth import get_portfolio_truth
from app.v2.sources.freshness_registry import get_freshness
from app.v2.surfaces.blueprint.explanation_builders import build_compare_explanations
from app.v2.surfaces.common import degraded_section, ready_section, surface_state
from app.v2.truth.candidate_quality import build_candidate_truth_context, enrich_score_decomposition_with_market_path_support


_SURFACE_ID = "compare"
_CONTRACT_VERSION = "0.3.2"

_SLEEVE_LABELS = {
    "global_equity_core": "Global Equity Core",
    "developed_ex_us_optional": "Developed ex-US Optional",
    "emerging_markets": "Emerging Markets",
    "china_satellite": "China Satellite",
    "ig_bonds": "IG Bonds",
    "cash_bills": "Cash and Bills",
    "real_assets": "Real Assets",
    "alternatives": "Alternatives",
    "convex": "Convex Protection",
}

_DECISION_RANK = {
    "actionable": 4,
    "shortlisted": 3,
    "research_only": 2,
    "blocked": 1,
}
_INTEGRITY_RANK = {
    "strong": 5,
    "mixed": 4,
    "weak": 2,
    "conflicted": 1,
    "missing": 0,
}
_AUM_STATE_RANK = {"resolved": 3, "stale": 2, "missing": 1}


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _latest_explorer_contract(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT contract_json
        FROM v2_surface_snapshots
        WHERE surface_id = 'blueprint_explorer'
        ORDER BY created_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    try:
        return json.loads(str(row["contract_json"] or "{}"))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _normalize_candidate_symbol(candidate_id: str) -> str:
    raw = str(candidate_id or "").strip()
    if raw.startswith("candidate_instrument_"):
        return raw.removeprefix("candidate_instrument_").upper()
    if raw.startswith("instrument_"):
        return raw.removeprefix("instrument_").upper()
    return raw.upper()


def _humanize(value: str | None) -> str:
    raw = str(value or "").strip()
    if not raw:
        return "Unavailable"
    return raw.replace("_", " ").replace("-", " ").strip().title()


def _format_percent(value: float | None) -> str:
    if value is None:
        return "Not held"
    return f"{value:.2f}%"


def _format_bps(value: float | int | None) -> str:
    if value is None:
        return "Unavailable"
    return f"{float(value):.0f} bps"


def _format_aum(aum_usd: float | None, aum_state: str | None) -> str:
    if aum_usd is not None:
        if aum_usd >= 1_000_000_000:
            return f"${aum_usd / 1_000_000_000:.1f}B"
        if aum_usd >= 1_000_000:
            return f"${aum_usd / 1_000_000:.0f}M"
        return f"${aum_usd:,.0f}"
    return _humanize(aum_state)


def _present_text(value: Any, default: str = "Unavailable") -> str:
    raw = str(value or "").strip()
    return raw or default


def _short_text(value: Any, default: str = "Unavailable", limit: int = 52) -> str:
    text = _present_text(value, default=default)
    if len(text) <= limit:
        return text
    return f"{text[: limit - 1].rstrip()}…"


def _score_band(score: float | int | None) -> str:
    try:
        numeric = float(score or 0.0)
    except (TypeError, ValueError):
        numeric = 0.0
    if numeric >= 85:
        return "High"
    if numeric >= 70:
        return "Good"
    if numeric >= 55:
        return "Watch"
    return "Weak"


def _compare_primary_state(candidate: dict[str, Any]) -> str:
    raw = str(candidate.get("investor_decision_state") or "").strip().lower()
    if raw in {"blocked"}:
        return "Blocked"
    if raw in {"actionable", "lead_candidate"}:
        return "Actionable"
    if raw in {"research_only"}:
        return "Research only"
    return "Reviewable"


def _compare_reason_line(candidate: dict[str, Any]) -> str:
    gate = dict(candidate.get("recommendation_gate") or {})
    summary = str(gate.get("summary") or "").strip()
    if summary:
        return summary
    blocked_reasons = list(gate.get("blocked_reasons") or [])
    if blocked_reasons:
        return str(blocked_reasons[0]).strip()
    blocker = str(candidate.get("blocker_category") or "").strip()
    if blocker:
        return f"{_humanize(blocker)} still needs review."
    state = _compare_primary_state(candidate)
    if state == "Actionable":
        return "Clean enough to keep moving."
    if state == "Blocked":
        return "Still does not clear the sleeve job cleanly."
    if state == "Research only":
        return "Worth keeping in research, but not ready to act on."
    return "Fee, evidence, or execution still needs a pressure test."


def _ig_bond_exposure_summary(candidate: dict[str, Any]) -> str:
    symbol = str(candidate.get("symbol") or "").strip().upper()
    name = str(candidate.get("name") or "").strip().lower()
    benchmark = str(candidate.get("benchmark_full_name") or "").strip().lower()
    exposure = str(candidate.get("exposure_summary") or "").strip()
    if symbol == "A35" or "singapore" in benchmark:
        return "Singapore government and quasi sovereign SGD bonds"
    if "hedged" in name or "hedged" in benchmark:
        return "Global aggregate investment grade, USD hedged"
    if "global aggregate" in benchmark or "aggregate bond" in name:
        return "Global aggregate investment grade, mixed sovereign and corporate"
    return exposure or _present_text(candidate.get("benchmark_full_name"), "Exposure summary unavailable")


def _compare_exposure_summary(candidate: dict[str, Any]) -> str:
    if str(candidate.get("sleeve_key") or "").strip() == "ig_bonds":
        return _ig_bond_exposure_summary(candidate)
    return _present_text(
        candidate.get("exposure_summary") or candidate.get("benchmark_full_name"),
        "Exposure summary unavailable",
    )


def _compare_identity_tags(candidate: dict[str, Any]) -> list[str]:
    tags: list[str] = []
    for raw in [
        candidate.get("distribution_policy"),
        candidate.get("replication_method"),
        candidate.get("tax_posture_summary"),
    ]:
        tag = _short_text(raw, default="", limit=28)
        if tag and tag not in tags:
            tags.append(tag)
        if len(tags) == 2:
            break
    return tags


def _implementation_value(implementation_profile: dict[str, Any], keys: list[str]) -> str | None:
    for key in keys:
        raw = str(implementation_profile.get(key) or "").strip()
        if raw:
            return raw
    return None


def _append_stat(stats: list[dict[str, str]], label: str, value: str | None) -> None:
    if not value or value == "Unavailable":
        return
    if any(str(item.get("label") or "") == label for item in stats):
        return
    stats.append({"label": label, "value": value})


def _implementation_stats(candidate: dict[str, Any]) -> list[dict[str, str]]:
    implementation_profile = dict(candidate.get("implementation_profile") or {})
    stats: list[dict[str, str]] = []
    _append_stat(
        stats,
        "TER",
        _format_bps(candidate.get("ter_bps")) if candidate.get("ter_bps") is not None else _implementation_value(implementation_profile, ["expense_ratio"]),
    )
    _append_stat(
        stats,
        "Spread",
        _format_bps(candidate.get("spread_proxy_bps")) if candidate.get("spread_proxy_bps") is not None else _implementation_value(implementation_profile, ["spread_proxy"]),
    )
    _append_stat(
        stats,
        "Assets",
        _implementation_value(implementation_profile, ["aum"]) or _format_aum(candidate.get("aum_usd"), candidate.get("aum_state")),
    )
    if str(candidate.get("sleeve_key") or "").strip() == "ig_bonds":
        _append_stat(stats, "Duration", _implementation_value(implementation_profile, ["duration", "effective_duration", "weighted_average_duration", "option_adjusted_duration"]))
        _append_stat(stats, "YTM", _implementation_value(implementation_profile, ["yield_to_maturity", "ytm", "sec_yield", "yield"]))
        _append_stat(stats, "Quality", _implementation_value(implementation_profile, ["average_quality", "average_credit_quality", "credit_quality", "quality"]))
        _append_stat(stats, "Maturity", _implementation_value(implementation_profile, ["average_maturity", "weighted_average_maturity", "maturity"]))
    else:
        _append_stat(stats, "Benchmark", _present_text(candidate.get("benchmark_full_name"), default=""))
        _append_stat(stats, "Replication", _implementation_value(implementation_profile, ["replication_method"]))
        _append_stat(stats, "Distribution", _implementation_value(implementation_profile, ["distribution_policy"]))
        _append_stat(stats, "Currency", _implementation_value(implementation_profile, ["primary_trading_currency"]))
    return stats[:6]


def _evidence_status(candidate: dict[str, Any]) -> str:
    integrity = dict(candidate.get("source_integrity_summary") or {})
    state = str(integrity.get("state") or integrity.get("integrity_label") or "").strip().lower()
    return {
        "strong": "Strong",
        "mixed": "Mixed",
        "weak": "Thin",
        "conflicted": "Conflicted",
        "missing": "Missing",
    }.get(state, _humanize(state) if state else "Unavailable")


def _timing_status(candidate: dict[str, Any]) -> str:
    support = dict(candidate.get("market_path_support") or {})
    timing_state = str(support.get("timing_state") or "").strip().lower()
    timing_label = str(support.get("timing_label") or "").strip()
    if timing_state:
        return {
            "timing_ready": "Timing ready",
            "timing_review": "Timing review",
            "timing_fragile": "Timing fragile",
            "timing_constrained": "Timing constrained",
            "timing_unavailable": "Timing unavailable",
        }.get(timing_state, timing_label or "Timing review")
    state = str(support.get("market_setup_state") or "").strip().lower()
    if state in {"direct_usable", "direct_ready"}:
        return "Direct series ready"
    if state == "proxy_usable":
        return "Proxy-backed"
    if state == "stale":
        return "Stored setup"
    if state == "degraded":
        return "Bounded"
    if state == "unavailable":
        return "Unavailable"
    return "Under review" if support else "Unavailable"


def _impact_line(candidate: dict[str, Any]) -> str:
    summary = str(dict(candidate.get("recommendation_gate") or {}).get("summary") or "").strip()
    if summary:
        return summary
    integrity_summary = str(dict(candidate.get("source_integrity_summary") or {}).get("summary") or "").strip()
    if integrity_summary:
        return integrity_summary
    state = _compare_primary_state(candidate)
    if state == "Actionable":
        return "Does not change the current ETF preference."
    if state == "Blocked":
        return "Still changes the sleeve read enough to keep this line out."
    if state == "Research only":
        return "Still too early to let this line change the sleeve read."
    return "Still reviewable, but not yet the cleanest implementation."


def _scope_fit_label(verdict: str) -> str:
    if verdict == "direct_substitutes":
        return "Same sleeve job"
    if verdict == "partial_substitutes":
        return "Same sleeve, different implementation"
    if verdict == "different_jobs":
        return "Different job"
    return "Under review"


def _compare_thesis(candidate: dict[str, Any]) -> str:
    state = _compare_primary_state(candidate)
    sleeve_name = str(candidate.get("sleeve_name") or "current sleeve").strip()
    if state == "Actionable":
        return f"Matches the {sleeve_name} sleeve and is clean enough to keep moving."
    if state == "Blocked":
        return f"Still sits in the {sleeve_name} sleeve frame, but does not yet clear the clean implementation bar."
    if state == "Research only":
        return f"Still relates to the {sleeve_name} sleeve, but is not close enough to action."
    return f"Matches the {sleeve_name} sleeve, but is not yet the cleanest implementation."


def _compare_candidate_card(candidate: dict[str, Any], *, verdict: str) -> dict[str, Any]:
    scores = dict(candidate.get("score_decomposition") or {})
    sleeve_fit_score = int(round(float(scores.get("sleeve_fit_score") or 0.0)))
    benchmark_fit_score = int(round(float(scores.get("benchmark_fidelity_score") or 0.0)))
    return {
        "identity": {
            "exposure_summary": _compare_exposure_summary(candidate),
            "compact_tags": _compare_identity_tags(candidate),
        },
        "verdict": {
            "primary_state": _compare_primary_state(candidate),
            "reason_line": _compare_reason_line(candidate),
        },
        "sleeve_fit": {
            "role_fit": f"{_score_band(sleeve_fit_score)} · {sleeve_fit_score}",
            "benchmark_fit": f"{_score_band(benchmark_fit_score)} · {benchmark_fit_score}",
            "scope_fit": _scope_fit_label(verdict),
            "thesis": _compare_thesis(candidate),
        },
        "implementation": {
            "stats": _implementation_stats(candidate),
        },
        "risk_evidence": {
            "evidence_status": _evidence_status(candidate),
            "timing_status": _timing_status(candidate),
            "impact_line": _impact_line(candidate),
        },
    }


def _candidate_row(
    conn: sqlite3.Connection,
    candidate_id: str,
    *,
    sleeve_id: str | None = None,
) -> dict[str, Any] | None:
    ensure_candidate_registry_tables(conn)
    rows = export_live_candidate_registry(conn)
    if not rows:
        seed_default_candidate_registry(conn)
        rows = export_live_candidate_registry(conn)
    symbol = _normalize_candidate_symbol(candidate_id)
    matches = [row for row in rows if str(row.get("symbol") or "").strip().upper() == symbol]
    if not matches:
        return None
    requested_sleeve = str(sleeve_id or "").strip()
    if requested_sleeve:
        sleeve_matches = [
            row
            for row in matches
            if str(row.get("sleeve_key") or "").strip() == requested_sleeve
        ]
        if len(sleeve_matches) == 1:
            return sleeve_matches[0]
    return matches[0]


def _explorer_candidate_row(
    explorer_contract: dict[str, Any] | None,
    candidate_id: str,
    *,
    sleeve_id: str | None = None,
) -> tuple[dict[str, Any], str | None] | None:
    if not isinstance(explorer_contract, dict):
        return None
    target_candidate_id = str(candidate_id or "").strip()
    requested_sleeve_id = str(sleeve_id or "").strip()
    sleeves = list(explorer_contract.get("sleeves") or [])
    for strict in (True, False):
        for sleeve in sleeves:
            sleeve_row_id = str(sleeve.get("sleeve_id") or "").strip()
            if strict and requested_sleeve_id and sleeve_row_id != requested_sleeve_id:
                continue
            for row in list(sleeve.get("candidates") or []):
                if str(row.get("candidate_id") or "").strip() == target_candidate_id:
                    return dict(row), sleeve_row_id or None
        if requested_sleeve_id:
            break
    return None


def _candidate_snapshot_from_explorer(
    explorer_contract: dict[str, Any] | None,
    candidate_id: str,
    *,
    sleeve_id: str | None = None,
) -> dict[str, Any] | None:
    matched = _explorer_candidate_row(explorer_contract, candidate_id, sleeve_id=sleeve_id)
    if matched is None:
        return None
    row, matched_sleeve_id = matched
    symbol = str(row.get("symbol") or _normalize_candidate_symbol(candidate_id)).strip().upper()
    sleeve_key = str(row.get("sleeve_key") or "").strip()
    if not sleeve_key and matched_sleeve_id.startswith("sleeve_"):
        sleeve_key = matched_sleeve_id.removeprefix("sleeve_")
    recommendation_gate = dict(row.get("recommendation_gate") or {})
    source_integrity_summary = dict(row.get("source_integrity_summary") or {})
    score_decomposition = dict(row.get("score_decomposition") or {})
    implementation_profile = dict(row.get("implementation_profile") or {})
    market_path_support = dict(row.get("market_path_support") or {})
    current_weight_pct = _current_holding_weight_pct(symbol)
    return {
        "candidate_id": str(row.get("candidate_id") or candidate_id).strip(),
        "symbol": symbol,
        "name": str(row.get("name") or symbol).strip(),
        "sleeve_key": sleeve_key,
        "sleeve_name": _SLEEVE_LABELS.get(sleeve_key, _humanize(sleeve_key)),
        "benchmark_key": str(row.get("benchmark_key") or "").strip() or None,
        "benchmark_full_name": row.get("benchmark_full_name"),
        "exposure_summary": row.get("exposure_summary"),
        "ter_bps": row.get("ter_bps"),
        "spread_proxy_bps": row.get("spread_proxy_bps"),
        "aum_usd": row.get("aum_usd"),
        "aum_state": row.get("aum_state"),
        "distribution_policy": row.get("distribution_policy"),
        "tax_posture_summary": row.get("tax_posture_summary"),
        "replication_method": row.get("replication_method"),
        "replication_risk_note": row.get("replication_risk_note"),
        "current_weight_pct": current_weight_pct,
        "weight_state": _weight_state(current_weight_pct),
        "investor_decision_state": row.get("investor_decision_state") or row.get("decision_state"),
        "blocker_category": row.get("blocker_category"),
        "source_integrity_summary": source_integrity_summary,
        "score_decomposition": score_decomposition,
        "identity_state": dict(row.get("identity_state") or {}),
        "recommendation_gate": recommendation_gate,
        "market_path_support": market_path_support if market_path_support else None,
        "implementation_profile": implementation_profile,
        "institutional_facts": dict(row.get("institutional_facts") or {}),
    }


def _current_holding_weight_pct(symbol: str) -> float | None:
    try:
        portfolio = get_portfolio_truth("default")
    except Exception:
        return None
    holdings = list(getattr(portfolio, "holdings", []) or [])
    matched = next(
        (
            holding
            for holding in holdings
            if str(holding.get("symbol") or "").strip().upper() == str(symbol or "").strip().upper()
        ),
        None,
    )
    if not matched:
        return None
    try:
        weight = float(matched.get("weight") or matched.get("weight_pct") or 0.0)
    except (TypeError, ValueError):
        return None
    if weight <= 1.0:
        return round(weight * 100.0, 2)
    return round(weight, 2)


def _weight_state(current_weight_pct: float | None) -> str:
    if current_weight_pct is None:
        return "not_held"
    if current_weight_pct <= 0:
        return "not_held"
    return "held"


def _candidate_snapshot(
    conn: sqlite3.Connection,
    candidate_id: str,
    *,
    sleeve_id: str | None = None,
) -> dict[str, Any]:
    row = _candidate_row(conn, candidate_id, sleeve_id=sleeve_id)
    if row is None:
        raise ValueError(f"Unknown blueprint candidate: {candidate_id}")
    symbol = str(row.get("symbol") or "").strip().upper()
    sleeve_key = str(row.get("sleeve_key") or "").strip()
    truth_context = build_candidate_truth_context(conn, {**row, "symbol": symbol, "sleeve_key": sleeve_key})
    institutional_facts = dict(truth_context.get("institutional_facts") or {})
    source_integrity_summary = dict(truth_context.get("source_integrity_summary") or {})
    score_decomposition = dict(truth_context.get("score_decomposition") or {})
    recommendation_gate = dict(truth_context.get("recommendation_gate") or {})
    implementation_profile = dict(truth_context.get("implementation_profile") or {})
    market_path_support = build_candidate_market_path_support(candidate_id, allow_refresh=False)
    score_decomposition = dict(
        enrich_score_decomposition_with_market_path_support(
            score_decomposition,
            market_path_support if isinstance(market_path_support, dict) else None,
        ) or score_decomposition
    )
    current_weight_pct = _current_holding_weight_pct(symbol)
    return {
        "candidate_id": candidate_id,
        "symbol": symbol,
        "name": str(row.get("name") or symbol).strip(),
        "sleeve_key": sleeve_key,
        "sleeve_name": _SLEEVE_LABELS.get(sleeve_key, _humanize(sleeve_key)),
        "benchmark_key": str(row.get("benchmark_key") or "").strip() or None,
        "benchmark_full_name": institutional_facts.get("benchmark_full_name"),
        "exposure_summary": institutional_facts.get("exposure_summary"),
        "ter_bps": institutional_facts.get("ter_bps"),
        "spread_proxy_bps": institutional_facts.get("spread_proxy_bps"),
        "aum_usd": institutional_facts.get("aum_usd"),
        "aum_state": institutional_facts.get("aum_state"),
        "distribution_policy": institutional_facts.get("distribution_policy"),
        "tax_posture_summary": dict(institutional_facts.get("sg_tax_posture") or {}).get("summary"),
        "replication_method": implementation_profile.get("replication_method"),
        "replication_risk_note": institutional_facts.get("replication_risk_note"),
        "current_weight_pct": current_weight_pct,
        "weight_state": _weight_state(current_weight_pct),
        "investor_decision_state": truth_context.get("investor_decision_state"),
        "blocker_category": truth_context.get("blocker_category"),
        "source_integrity_summary": source_integrity_summary,
        "score_decomposition": score_decomposition,
        "identity_state": dict(truth_context.get("identity_state") or {}),
        "recommendation_gate": recommendation_gate,
        "market_path_support": market_path_support if isinstance(market_path_support, dict) else None,
        "implementation_profile": implementation_profile,
        "institutional_facts": institutional_facts,
    }


def _same_sleeve(candidates: list[dict[str, Any]]) -> bool:
    return len({str(candidate.get("sleeve_key") or "") for candidate in candidates}) == 1


def _all_equal(values: list[str]) -> bool:
    normalized = [value for value in values if value]
    return bool(normalized) and len(set(normalized)) == 1


def _decision_score(state: str | None) -> int:
    return _DECISION_RANK.get(str(state or "").strip(), 0)


def _integrity_score(state: str | None) -> int:
    return _INTEGRITY_RANK.get(str(state or "").strip(), 0)


def _candidate_leader(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    def rank(candidate: dict[str, Any]) -> tuple[float, float, float]:
        score = float(dict(candidate.get("score_decomposition") or {}).get("total_score") or 0)
        decision = float(_decision_score(candidate.get("investor_decision_state")))
        integrity = float(_integrity_score(dict(candidate.get("source_integrity_summary") or {}).get("state")))
        return (score, decision, integrity)

    return sorted(candidates, key=rank, reverse=True)[0]


def _dimension_values(candidates: list[dict[str, Any]], field_name: str) -> list[Any]:
    return [candidate.get(field_name) for candidate in candidates]


def _dimension_value_rows(candidates: list[dict[str, Any]], values: list[str], *, tones: list[str | None] | None = None) -> list[dict[str, Any]]:
    return [
        {
            "candidate_id": candidate["candidate_id"],
            "value": values[index],
            "tone": tones[index] if tones else None,
        }
        for index, candidate in enumerate(candidates)
    ]


def _compare_dimension(
    *,
    dimension_id: str,
    label: str,
    group: str,
    candidates: list[dict[str, Any]],
    values: list[str],
    importance: str = "medium",
    rationale: str | None = None,
    higher_is_better: bool | None = None,
    numeric_values: list[float | None] | None = None,
    tones: list[str | None] | None = None,
) -> dict[str, Any]:
    discriminating = len({value for value in values if value and value != "Unavailable"}) > 1
    winner = "tie"
    if numeric_values and higher_is_better is not None:
        present = [(index, value) for index, value in enumerate(numeric_values) if value is not None]
        if present:
            ordered = sorted(present, key=lambda item: item[1], reverse=higher_is_better)
            if len(ordered) == 1 or ordered[0][1] != ordered[1][1]:
                winner = candidates[ordered[0][0]]["candidate_id"]
    elif discriminating:
        present = [(index, value) for index, value in enumerate(values) if value and value != "Unavailable"]
        if len(present) == 1:
            winner = candidates[present[0][0]]["candidate_id"]
    return {
        "dimension_id": dimension_id,
        "dimension": dimension_id,
        "label": label,
        "group": group,
        "discriminating": discriminating,
        "importance": importance,
        "rationale": rationale,
        "values": _dimension_value_rows(candidates, values, tones=tones),
        "a_value": values[0] if values else "Unavailable",
        "b_value": values[1] if len(values) > 1 else "Unavailable",
        "winner": winner,
    }


def _candidate_role(candidate: dict[str, Any], candidates: list[dict[str, Any]]) -> str | None:
    candidate_id = str(candidate.get("candidate_id") or "")
    for index, row in enumerate(candidates[:2]):
        if str(row.get("candidate_id") or "") == candidate_id:
            return "candidate_a" if index == 0 else "candidate_b"
    return None


def _role_candidate(role: str, candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if role == "candidate_a" and candidates:
        return candidates[0]
    if role == "candidate_b" and len(candidates) > 1:
        return candidates[1]
    return None


def _other_role(role: str) -> str:
    return "candidate_b" if role == "candidate_a" else "candidate_a"


def _role_label(role: str, candidates: list[dict[str, Any]]) -> str:
    candidate = _role_candidate(role, candidates)
    if not candidate:
        return "No clear winner"
    return str(candidate.get("symbol") or candidate.get("name") or role).strip()


def _score_value(candidate: dict[str, Any], *keys: str) -> float:
    scores = dict(candidate.get("score_decomposition") or {})
    for key in keys:
        try:
            return float(scores.get(key) if scores.get(key) is not None else 0.0)
        except (TypeError, ValueError):
            continue
    return 0.0


def _score_text(candidate: dict[str, Any], *keys: str) -> str:
    value = _score_value(candidate, *keys)
    return str(int(round(value))) if value else "Unavailable"


def _winner_from_scores(
    candidates: list[dict[str, Any]],
    scores: list[float],
    *,
    higher_is_better: bool = True,
    material_gap: float = 3.0,
    allow_no_clear: bool = False,
) -> str:
    if len(candidates) < 2 or len(scores) < 2:
        return "no_clear_winner" if allow_no_clear else "tie"
    score_a, score_b = scores[0], scores[1]
    gap = (score_a - score_b) if higher_is_better else (score_b - score_a)
    if abs(gap) < material_gap:
        return "no_clear_winner" if allow_no_clear else "tie"
    return "candidate_a" if gap > 0 else "candidate_b"


def _winner_from_numeric_values(
    candidates: list[dict[str, Any]],
    values: list[float | None],
    *,
    higher_is_better: bool,
    material_gap: float = 0.0,
) -> str:
    if len(candidates) < 2 or len(values) < 2:
        return "not_applicable"
    if values[0] is None and values[1] is None:
        return "not_applicable"
    if values[0] is None:
        return "candidate_b"
    if values[1] is None:
        return "candidate_a"
    gap = (values[0] - values[1]) if higher_is_better else (values[1] - values[0])
    if abs(gap) <= material_gap:
        return "tie"
    return "candidate_a" if gap > 0 else "candidate_b"


def _impl(candidate: dict[str, Any], *keys: str) -> str | None:
    profile = dict(candidate.get("implementation_profile") or {})
    facts = dict(candidate.get("institutional_facts") or {})
    for source in (profile, facts, candidate):
        for key in keys:
            raw = source.get(key)
            if raw is None:
                continue
            text = str(raw).strip()
            if text:
                return text
    return None


def _tracking_value(candidate: dict[str, Any]) -> tuple[str, float | None]:
    raw = _impl(candidate, "tracking_difference", "tracking_difference_1y")
    if not raw:
        return "Unavailable", None
    text = str(raw).strip()
    numeric = None
    try:
        cleaned = text.replace("%", "").replace("+", "").strip()
        numeric = abs(float(cleaned))
    except (TypeError, ValueError):
        numeric = None
    return text, numeric


def _timing_rank(candidate: dict[str, Any]) -> float:
    support = dict(candidate.get("market_path_support") or {})
    timing = str(support.get("timing_state") or "").strip().lower()
    setup = str(support.get("market_setup_state") or "").strip().lower()
    return {
        "timing_ready": 5.0,
        "timing_review": 4.0,
        "timing_constrained": 3.0,
        "timing_fragile": 2.0,
        "timing_unavailable": 0.0,
        "direct_usable": 5.0,
        "direct_ready": 5.0,
        "proxy_usable": 3.5,
        "degraded": 2.0,
        "stale": 1.5,
        "unavailable": 0.0,
    }.get(timing or setup, 1.0 if support else 0.0)


def _diversification_profile(candidate: dict[str, Any]) -> tuple[str, float]:
    benchmark = str(candidate.get("benchmark_full_name") or candidate.get("exposure_summary") or "").lower()
    if any(term in benchmark for term in ("acwi", "all-world", "all world", "global aggregate")):
        return "Broad global exposure", 5.0
    if any(term in benchmark for term in ("msci world", "developed", "world index")):
        return "Broad developed-market exposure", 4.5
    if any(term in benchmark for term in ("s&p 500", "s&p", "us ", "u.s.", "united states")):
        return "US large-cap concentration", 3.0
    if any(term in benchmark for term in ("emerging", "china", "gold", "commodity", "trend")):
        return "Distinct satellite or diversifier exposure", 2.5
    return _present_text(candidate.get("exposure_summary"), "Diversification profile unavailable"), 2.0


def _source_issue_fields(candidate: dict[str, Any]) -> list[str]:
    integrity = dict(candidate.get("source_integrity_summary") or {})
    values: list[str] = []
    for key in ("hard_conflict_fields", "missing_critical_fields", "weakest_fields"):
        for item in list(integrity.get(key) or []):
            label = _humanize(str(item))
            if label not in values:
                values.append(label)
    scores = dict(candidate.get("score_decomposition") or {})
    for component in list(scores.get("components") or []):
        for key in ("missing_fields", "weak_fields", "conflict_fields", "stale_fields"):
            for item in list(dict(component).get(key) or []):
                label = _humanize(str(item))
                if label not in values:
                    values.append(label)
    return values[:8]


def _delta_row(
    *,
    attribute: str,
    a_value: Any,
    b_value: Any,
    winner: str,
    implication: str,
    materiality: str,
) -> dict[str, Any]:
    row_id = (
        attribute.strip()
        .lower()
        .replace("/", " ")
        .replace("-", " ")
        .replace(" ", "_")
    )
    return {
        "row_id": row_id,
        "label": attribute,
        "attribute": attribute,
        "candidate_a_value": a_value if a_value is not None else "Unavailable",
        "candidate_b_value": b_value if b_value is not None else "Unavailable",
        "winner": winner,
        "implication": implication,
        "materiality": materiality,
    }


def _build_decision_delta_table(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(candidates) < 2:
        return []
    candidate_a, candidate_b = candidates[:2]
    div_a, div_score_a = _diversification_profile(candidate_a)
    div_b, div_score_b = _diversification_profile(candidate_b)
    tracking_a, tracking_numeric_a = _tracking_value(candidate_a)
    tracking_b, tracking_numeric_b = _tracking_value(candidate_b)
    ter_values = [
        float(candidate.get("ter_bps")) if candidate.get("ter_bps") is not None else None
        for candidate in candidates[:2]
    ]
    spread_values = [
        float(candidate.get("spread_proxy_bps")) if candidate.get("spread_proxy_bps") is not None else None
        for candidate in candidates[:2]
    ]
    aum_values = [
        float(candidate.get("aum_usd")) if candidate.get("aum_usd") is not None else None
        for candidate in candidates[:2]
    ]
    deployment_scores = [_score_value(candidate, "deployability_score", "deployment_score", "readiness_score") for candidate in candidates[:2]]
    portfolio_scores = [_score_value(candidate, "portfolio_fit_score") for candidate in candidates[:2]]
    evidence_scores = [_score_value(candidate, "source_integrity_score", "evidence_score") for candidate in candidates[:2]]
    timing_scores = [_timing_rank(candidate) for candidate in candidates[:2]]
    return [
        _delta_row(
            attribute="Sleeve job",
            a_value=_present_text(candidate_a.get("sleeve_name")),
            b_value=_present_text(candidate_b.get("sleeve_name")),
            winner="tie" if str(candidate_a.get("sleeve_key")) == str(candidate_b.get("sleeve_key")) else "depends",
            implication="Same sleeve means this is a substitution question; different sleeves means it is an allocation question.",
            materiality="high",
        ),
        _delta_row(
            attribute="Exposure scope",
            a_value=_compare_exposure_summary(candidate_a),
            b_value=_compare_exposure_summary(candidate_b),
            winner="tie" if _compare_exposure_summary(candidate_a) == _compare_exposure_summary(candidate_b) else "depends",
            implication="Exposure scope decides whether the pair is a real substitute or just competes for sleeve budget.",
            materiality="high",
        ),
        _delta_row(
            attribute="Benchmark",
            a_value=_present_text(candidate_a.get("benchmark_full_name")),
            b_value=_present_text(candidate_b.get("benchmark_full_name")),
            winner="tie" if str(candidate_a.get("benchmark_full_name")) == str(candidate_b.get("benchmark_full_name")) else "depends",
            implication="Different benchmark lineage changes what the next dollar actually buys.",
            materiality="high",
        ),
        _delta_row(
            attribute="Diversification",
            a_value=div_a,
            b_value=div_b,
            winner=_winner_from_numeric_values(candidates, [div_score_a, div_score_b], higher_is_better=True, material_gap=0.75),
            implication="Broader exposure helps when the sleeve needs diversification; narrower exposure helps only when the intended bet is specific.",
            materiality="high",
        ),
        _delta_row(
            attribute="TER",
            a_value=_format_bps(ter_values[0]) if ter_values[0] is not None else "Unavailable",
            b_value=_format_bps(ter_values[1]) if ter_values[1] is not None else "Unavailable",
            winner=_winner_from_numeric_values(candidates, ter_values, higher_is_better=False, material_gap=1.0),
            implication="Lower ongoing cost wins only after the exposure job is close enough.",
            materiality="high",
        ),
        _delta_row(
            attribute="Spread",
            a_value=_format_bps(spread_values[0]) if spread_values[0] is not None else "Unavailable",
            b_value=_format_bps(spread_values[1]) if spread_values[1] is not None else "Unavailable",
            winner=_winner_from_numeric_values(candidates, spread_values, higher_is_better=False, material_gap=1.0),
            implication="Lower spread is the cleaner deployment route when trade implementation matters.",
            materiality="high",
        ),
        _delta_row(
            attribute="AUM",
            a_value=_format_aum(candidate_a.get("aum_usd"), candidate_a.get("aum_state")),
            b_value=_format_aum(candidate_b.get("aum_usd"), candidate_b.get("aum_state")),
            winner=_winner_from_numeric_values(candidates, aum_values, higher_is_better=True, material_gap=100_000_000),
            implication="Scale supports tradability and durability, but it should not override the sleeve job.",
            materiality="medium",
        ),
        _delta_row(
            attribute="Tracking difference",
            a_value=tracking_a,
            b_value=tracking_b,
            winner=_winner_from_numeric_values(candidates, [tracking_numeric_a, tracking_numeric_b], higher_is_better=False, material_gap=0.02),
            implication="Lower absolute tracking drag supports long-horizon quality if the numbers are measured on a comparable basis.",
            materiality="medium",
        ),
        _delta_row(
            attribute="Domicile",
            a_value=_impl(candidate_a, "domicile"),
            b_value=_impl(candidate_b, "domicile"),
            winner="tie" if _impl(candidate_a, "domicile") == _impl(candidate_b, "domicile") else "depends",
            implication="Domicile affects wrapper, tax, and implementation suitability rather than short-term timing.",
            materiality="medium",
        ),
        _delta_row(
            attribute="Trading currency",
            a_value=_impl(candidate_a, "primary_trading_currency"),
            b_value=_impl(candidate_b, "primary_trading_currency"),
            winner="tie" if _impl(candidate_a, "primary_trading_currency") == _impl(candidate_b, "primary_trading_currency") else "depends",
            implication="Trading currency changes implementation friction and FX handling, not the underlying benchmark by itself.",
            materiality="medium",
        ),
        _delta_row(
            attribute="Listing exchange",
            a_value=_impl(candidate_a, "primary_listing_exchange"),
            b_value=_impl(candidate_b, "primary_listing_exchange"),
            winner="tie" if _impl(candidate_a, "primary_listing_exchange") == _impl(candidate_b, "primary_listing_exchange") else "depends",
            implication="Exchange route matters for execution quality, quote freshness, and available trading line.",
            materiality="medium",
        ),
        _delta_row(
            attribute="Distribution type",
            a_value=_present_text(candidate_a.get("distribution_policy")),
            b_value=_present_text(candidate_b.get("distribution_policy")),
            winner="tie" if str(candidate_a.get("distribution_policy")) == str(candidate_b.get("distribution_policy")) else "depends",
            implication="Distribution mechanics matter for tax and cash-flow handling, not immediate market timing.",
            materiality="low",
        ),
        _delta_row(
            attribute="Replication method",
            a_value=_impl(candidate_a, "replication_method"),
            b_value=_impl(candidate_b, "replication_method"),
            winner="tie" if _impl(candidate_a, "replication_method") == _impl(candidate_b, "replication_method") else "depends",
            implication="Replication method affects structural risk and tracking reliability.",
            materiality="medium",
        ),
        _delta_row(
            attribute="Source confidence",
            a_value=_score_text(candidate_a, "source_integrity_score", "evidence_score"),
            b_value=_score_text(candidate_b, "source_integrity_score", "evidence_score"),
            winner=_winner_from_scores(candidates, evidence_scores, material_gap=3.0),
            implication="Cleaner evidence raises trust in the comparison but should not become the investment thesis.",
            materiality="high",
        ),
        _delta_row(
            attribute="Timing state",
            a_value=_timing_status(candidate_a),
            b_value=_timing_status(candidate_b),
            winner=_winner_from_scores(candidates, timing_scores, material_gap=1.0),
            implication="Timing only affects deployment urgency; it does not replace benchmark and mandate fit.",
            materiality="medium",
        ),
        _delta_row(
            attribute="Deployability posture",
            a_value=f"{_score_text(candidate_a, 'deployability_score', 'deployment_score', 'readiness_score')} · {_humanize(dict(candidate_a.get('score_decomposition') or {}).get('readiness_posture'))}",
            b_value=f"{_score_text(candidate_b, 'deployability_score', 'deployment_score', 'readiness_score')} · {_humanize(dict(candidate_b.get('score_decomposition') or {}).get('readiness_posture'))}",
            winner=_winner_from_scores(candidates, deployment_scores, material_gap=4.0, allow_no_clear=True),
            implication="Deployability decides what can receive capital now after the investment comparison is understood.",
            materiality="high",
        ),
        _delta_row(
            attribute="Portfolio fit",
            a_value=_score_text(candidate_a, "portfolio_fit_score"),
            b_value=_score_text(candidate_b, "portfolio_fit_score"),
            winner=_winner_from_scores(candidates, portfolio_scores, material_gap=3.0),
            implication="Portfolio fit captures marginal usefulness, substitution quality, and target-sleeve role.",
            materiality="high",
        ),
    ]


def _substitution_assessment(verdict: str, candidates: list[dict[str, Any]], readiness_state: str) -> dict[str, Any]:
    status = {
        "direct_substitutes": "true_substitutes",
        "partial_substitutes": "partial_substitutes",
        "different_jobs": "different_tools",
    }.get(verdict, "different_tools" if readiness_state == "cross_sleeve" else "partial_substitutes")
    if status == "true_substitutes":
        summary = "These are true substitutes for the current sleeve job."
        reason = "They sit in the same sleeve and share the same benchmark or exposure role closely enough for implementation quality to decide."
    elif status == "partial_substitutes":
        summary = "These are partial substitutes, not perfect substitutes."
        reason = "They compete for the same sleeve budget, but benchmark, diversification, or implementation differences still change the portfolio result."
    else:
        summary = "These are different tools, not clean substitutes."
        reason = "The selected pair does not answer the same sleeve implementation question cleanly."
    if len(candidates) < 2:
        reason = "Select two candidates before judging substitution quality."
    confidence = "high" if status == "true_substitutes" else "medium" if status == "partial_substitutes" else "low"
    return {
        "status": status,
        "are_true_substitutes": status == "true_substitutes",
        "summary": summary,
        "reason": reason,
        "confidence": confidence,
    }


def _winner_summary(candidates: list[dict[str, Any]], verdict: str) -> dict[str, str]:
    if len(candidates) < 2:
        return {
            "best_overall": "no_clear_winner",
            "investment_winner": "tie",
            "deployment_winner": "tie",
            "evidence_winner": "tie",
            "timing_winner": "tie",
            "sleeve_winner": "tie",
            "portfolio_winner": "tie",
            "summary": "No clear winner until two candidates are selected.",
            "where_loser_wins": None,
        }
    investment_winner = _winner_from_scores(
        candidates,
        [_score_value(candidate, "investment_merit_score", "optimality_score", "recommendation_merit_score") for candidate in candidates[:2]],
        material_gap=3.0,
    )
    deployment_winner = _winner_from_scores(
        candidates,
        [_score_value(candidate, "deployability_score", "deployment_score", "readiness_score") for candidate in candidates[:2]],
        material_gap=4.0,
        allow_no_clear=True,
    )
    evidence_winner = _winner_from_scores(
        candidates,
        [_score_value(candidate, "source_integrity_score", "evidence_score", "truth_confidence_score") for candidate in candidates[:2]],
        material_gap=3.0,
    )
    timing_winner = _winner_from_scores(candidates, [_timing_rank(candidate) for candidate in candidates[:2]], material_gap=1.0)
    sleeve_winner = _winner_from_scores(
        candidates,
        [_score_value(candidate, "sleeve_fit_score") for candidate in candidates[:2]],
        material_gap=3.0,
    )
    portfolio_winner = _winner_from_scores(
        candidates,
        [_score_value(candidate, "portfolio_fit_score") for candidate in candidates[:2]],
        material_gap=3.0,
    )
    recommendation_winner = _winner_from_scores(
        candidates,
        [_score_value(candidate, "recommendation_score", "total_score") for candidate in candidates[:2]],
        material_gap=4.0,
        allow_no_clear=True,
    )
    if verdict == "different_jobs":
        best_overall = "no_clear_winner"
    elif recommendation_winner in {"candidate_a", "candidate_b"}:
        best_overall = recommendation_winner
    elif investment_winner == deployment_winner and investment_winner in {"candidate_a", "candidate_b"}:
        best_overall = investment_winner
    else:
        best_overall = "no_clear_winner"
    if best_overall in {"candidate_a", "candidate_b"}:
        summary = f"{_role_label(best_overall, candidates)} is the best current next-dollar candidate, but the read remains bounded by substitution quality and deployability."
        loser = _other_role(best_overall)
        where_loser_wins = f"{_role_label(loser, candidates)} can still win if its benchmark exposure, implementation route, or evidence quality is the explicit priority."
    else:
        summary = "No single overall winner is clean enough to declare; use the component winners and flip conditions."
        where_loser_wins = "Use the scenario winners and flip conditions instead of forcing one absolute winner."
    return {
        "best_overall": best_overall,
        "investment_winner": investment_winner,
        "deployment_winner": deployment_winner,
        "evidence_winner": evidence_winner,
        "timing_winner": timing_winner,
        "sleeve_winner": sleeve_winner,
        "portfolio_winner": portfolio_winner,
        "summary": summary,
        "where_loser_wins": where_loser_wins,
    }


def _decision_rule(candidates: list[dict[str, Any]], winners: dict[str, str], substitution: dict[str, str]) -> dict[str, Any]:
    if len(candidates) < 2:
        return {
            "primary_rule": "Select two candidates before making a compare decision.",
            "choose_candidate_a_if": "A second candidate is not selected.",
            "choose_candidate_b_if": "A second candidate is not selected.",
            "do_not_treat_as_substitutes_if": "A second candidate is not selected.",
            "choose_a_if": [],
            "choose_b_if": [],
            "next_action": "Select two candidates.",
        }
    candidate_a, candidate_b = candidates[:2]
    choose_a: list[str] = []
    choose_b: list[str] = []
    for role, bucket, label in [
        ("candidate_a", choose_a, str(candidate_a.get("symbol") or "Candidate A")),
        ("candidate_b", choose_b, str(candidate_b.get("symbol") or "Candidate B")),
    ]:
        if winners.get("investment_winner") == role:
            bucket.append(f"Choose {label} if investment merit and sleeve role fit matter more than immediate deployment friction.")
        if winners.get("deployment_winner") == role:
            bucket.append(f"Choose {label} if the next dollar must go to the more deployable implementation now.")
        if winners.get("evidence_winner") == role:
            bucket.append(f"Choose {label} if cleaner source confidence is the gating requirement.")
        if winners.get("timing_winner") == role:
            bucket.append(f"Choose {label} if current timing support is required before deployment.")
    div_a, div_score_a = _diversification_profile(candidate_a)
    div_b, div_score_b = _diversification_profile(candidate_b)
    if div_score_a > div_score_b:
        choose_a.append(f"Choose {candidate_a['symbol']} if the sleeve needs {div_a.lower()} rather than {div_b.lower()}.")
    elif div_score_b > div_score_a:
        choose_b.append(f"Choose {candidate_b['symbol']} if the sleeve needs {div_b.lower()} rather than {div_a.lower()}.")
    if not choose_a:
        choose_a.append(f"Choose {candidate_a['symbol']} only if its specific benchmark exposure is the intended sleeve expression.")
    if not choose_b:
        choose_b.append(f"Choose {candidate_b['symbol']} only if its specific benchmark exposure is the intended sleeve expression.")
    blockers = []
    if substitution.get("status") != "true_substitutes":
        blockers.append("Benchmark or exposure scope differs enough that a lower cost line may still be a different portfolio bet.")
    if any(str(candidate.get("sleeve_key") or "") != str(candidates[0].get("sleeve_key") or "") for candidate in candidates[:2]):
        blockers.append("The candidates are not in the same sleeve.")
    if not blockers:
        blockers.append("The candidate's benchmark, exposure, or wrapper facts diverge from the selected sleeve mandate.")
    best = winners.get("best_overall")
    if best in {"candidate_a", "candidate_b"}:
        next_action = f"Treat {_role_label(best, candidates)} as the current deployment candidate, then verify the flip conditions before capital moves."
    else:
        next_action = "Keep both in review and resolve the explicit flip conditions before selecting the deployment candidate."
    primary_rule = (
        f"Prefer {_role_label(best, candidates)} for the next dollar if the sleeve job and flip conditions stay unchanged."
        if best in {"candidate_a", "candidate_b"}
        else "Do not force a single winner; resolve the benchmark, evidence, or deployment split first."
    )
    return {
        "primary_rule": primary_rule,
        "choose_candidate_a_if": " ".join(choose_a[:4]),
        "choose_candidate_b_if": " ".join(choose_b[:4]),
        "do_not_treat_as_substitutes_if": " ".join(blockers[:4]),
        "choose_a_if": choose_a[:4],
        "choose_b_if": choose_b[:4],
        "do_not_treat_as_substitutes_if_list": blockers[:4],
        "next_action": next_action,
    }


def _portfolio_consequence_for(candidate: dict[str, Any], other: dict[str, Any] | None) -> dict[str, str | None]:
    div_profile, div_score = _diversification_profile(candidate)
    other_profile, other_score = _diversification_profile(other or {})
    sleeve = str(candidate.get("sleeve_name") or "the sleeve")
    symbol = str(candidate.get("symbol") or "This candidate")
    held = candidate.get("current_weight_pct")
    funding = (
        f"{symbol} is already present at {_format_percent(float(held))}; compare is about add/replace sizing."
        if held is not None
        else "Portfolio impact is estimated at sleeve level because live holdings overlay is not available."
    )
    concentration = (
        f"{symbol} concentrates the sleeve more than {str((other or {}).get('symbol') or 'the alternative')}."
        if div_score < other_score
        else f"{symbol} broadens exposure relative to {str((other or {}).get('symbol') or 'the alternative')}."
        if div_score > other_score
        else f"{symbol} has a similar concentration profile to the alternative."
    )
    diversification = (
        f"Adds {div_profile.lower()}."
        if div_profile != "Diversification profile unavailable"
        else "Diversification impact remains bounded until exposure scope is cleaner."
    )
    summary = f"Choosing {symbol} expresses {sleeve} through {_present_text(candidate.get('benchmark_full_name'), 'its current benchmark')}."
    sleeve_effect = f"It keeps the decision inside {sleeve}, but the sleeve effect depends on whether this benchmark is the desired job."
    risk_effect = f"Main risk is treating {div_profile.lower()} as equivalent to {other_profile.lower()} without acknowledging the substitution gap."
    return {
        "candidate_id": str(candidate.get("candidate_id") or ""),
        "symbol": symbol,
        "portfolio_effect": summary,
        "summary": summary,
        "sleeve_mandate_effect": sleeve_effect,
        "sleeve_effect": sleeve_effect,
        "concentration_effect": concentration,
        "region_exposure_effect": div_profile,
        "currency_or_trading_line_effect": _present_text(
            _impl(candidate, "primary_trading_currency", "primary_listing_exchange"),
            "Trading line requires confirmation.",
        ),
        "overlap_effect": "Overlap with existing holdings is not measured until holdings overlay is loaded.",
        "diversification_effect": diversification,
        "funding_path_effect": funding,
        "funding_effect": funding,
        "target_allocation_drift_effect": "Target drift is sleeve-level only until current portfolio weights are attached.",
        "risk_effect": risk_effect,
        "confidence": "medium" if held is not None else "low",
    }


def _scenario_winners(candidates: list[dict[str, Any]]) -> list[dict[str, str]]:
    if len(candidates) < 2:
        return []
    candidate_a, candidate_b = candidates[:2]
    sleeve_key = str(candidate_a.get("sleeve_key") or "")
    div_a, div_score_a = _diversification_profile(candidate_a)
    div_b, div_score_b = _diversification_profile(candidate_b)
    lower_cost = _winner_from_numeric_values(
        candidates,
        [float(candidate.get("ter_bps")) if candidate.get("ter_bps") is not None else None for candidate in candidates[:2]],
        higher_is_better=False,
        material_gap=1.0,
    )
    lower_spread = _winner_from_numeric_values(
        candidates,
        [float(candidate.get("spread_proxy_bps")) if candidate.get("spread_proxy_bps") is not None else None for candidate in candidates[:2]],
        higher_is_better=False,
        material_gap=1.0,
    )
    broader = _winner_from_numeric_values(candidates, [div_score_a, div_score_b], higher_is_better=True, material_gap=0.75)
    narrower = _other_role(broader) if broader in {"candidate_a", "candidate_b"} else "depends"
    if sleeve_key == "ig_bonds":
        scenarios = [
            ("Rates fall", "Benefits if duration and bond beta are the intended add.", "Benefits if its bond exposure carries cleaner duration fit.", broader, "Use the line with cleaner duration/sleeve fit when rates rally."),
            ("Rates rise", "Lower duration or higher cash-like behaviour matters more.", "Lower duration or higher cash-like behaviour matters more.", "depends", "The winner depends on duration and cash carry, not just TER."),
            ("Liquidity stress", "Spread and route quality decide execution.", "Spread and route quality decide execution.", lower_spread, "Lower spread and cleaner route matter most under stress."),
        ]
    elif sleeve_key in {"real_assets", "alternatives", "convex"}:
        scenarios = [
            ("Inflation shock", "Wins if its exposure is the intended hedge.", "Wins if its exposure is the intended hedge.", "depends", "Different real-asset tools should not be treated as automatic substitutes."),
            ("Risk-off drawdown", "Wins if the sleeve needs explicit downside behaviour.", "Wins if the sleeve needs explicit downside behaviour.", "depends", "Use the hedge that matches the drawdown transmission channel."),
            ("Liquidity stress", "Implementation route and spread matter more.", "Implementation route and spread matter more.", lower_spread, "The more tradable line wins only if the hedge role is still valid."),
        ]
    else:
        scenarios = [
            ("US-led equity rally", "Wins if US large-cap concentration is the desired expression.", "Wins if broader developed-market exposure is preferred.", narrower, "A narrower US-heavy line wins only when the sleeve intentionally wants that tilt."),
            ("Broad developed-market recovery", "Can participate, but may leave ex-US breadth behind.", "Can capture broader developed-market breadth.", broader, "The broader benchmark wins if diversification is the point of the next dollar."),
            ("Risk-off drawdown", "Concentration can increase path dependency.", "Broader exposure can reduce single-market dependence.", broader, "Broader diversification usually carries the cleaner defensive substitution case."),
            ("Liquidity stress", "Execution cost and trading line quality dominate the deployment step.", "Execution cost and trading line quality dominate the deployment step.", lower_spread, "Lower spread is more valuable when implementation friction becomes material."),
            ("Cost minimization", "Lower TER is the main advantage if exposure is acceptable.", "Lower TER is the main advantage if exposure is acceptable.", lower_cost, "Cost wins only if the benchmark job is close enough."),
        ]
    return [
        {
            "scenario": scenario,
            "candidate_a_effect": effect_a,
            "candidate_b_effect": effect_b,
            "winner": winner,
            "why": why,
        }
        for scenario, effect_a, effect_b, winner, why in scenarios
    ]


def _flip_conditions(candidates: list[dict[str, Any]], winners: dict[str, str]) -> list[dict[str, str]]:
    if len(candidates) < 2:
        return []
    candidate_a, candidate_b = candidates[:2]
    rows: list[dict[str, str]] = []
    spread_a = candidate_a.get("spread_proxy_bps")
    spread_b = candidate_b.get("spread_proxy_bps")
    if spread_a is not None and spread_b is not None:
        better = _winner_from_numeric_values(candidates, [float(spread_a), float(spread_b)], higher_is_better=False, material_gap=1.0)
        worse = _other_role(better) if better in {"candidate_a", "candidate_b"} else "no_clear_winner"
        rows.append({
            "condition": "Trading spread",
            "current_state": f"{candidate_a['symbol']} {_format_bps(float(spread_a))} vs {candidate_b['symbol']} {_format_bps(float(spread_b))}.",
            "flips_toward": worse,
            "threshold_or_trigger": "Flip if the current spread winner loses its spread advantage by more than 2 bps on the actual trading line.",
        })
    rows.append({
        "condition": "Benchmark intent",
        "current_state": f"{candidate_a['symbol']} tracks {_present_text(candidate_a.get('benchmark_full_name'))}; {candidate_b['symbol']} tracks {_present_text(candidate_b.get('benchmark_full_name'))}.",
        "flips_toward": "candidate_a" if winners.get("investment_winner") == "candidate_b" else "candidate_b" if winners.get("investment_winner") == "candidate_a" else "no_clear_winner",
        "threshold_or_trigger": "Flip if the sleeve mandate explicitly prefers the other benchmark exposure for the next dollar.",
    })
    rows.append({
        "condition": "Source confidence",
        "current_state": f"{candidate_a['symbol']} source {_score_text(candidate_a, 'source_integrity_score', 'evidence_score')} vs {candidate_b['symbol']} source {_score_text(candidate_b, 'source_integrity_score', 'evidence_score')}.",
        "flips_toward": "no_clear_winner" if winners.get("evidence_winner") == "tie" else str(winners.get("evidence_winner") or "no_clear_winner"),
        "threshold_or_trigger": "Flip or downgrade if the evidence winner loses source-complete status or the loser clears its recommendation-critical gaps.",
    })
    rows.append({
        "condition": "Deployability",
        "current_state": f"{candidate_a['symbol']} deployability {_score_text(candidate_a, 'deployability_score', 'deployment_score', 'readiness_score')} vs {candidate_b['symbol']} {_score_text(candidate_b, 'deployability_score', 'deployment_score', 'readiness_score')}.",
        "flips_toward": "no_clear_winner" if winners.get("deployment_winner") in {"tie", "no_clear_winner"} else str(winners.get("deployment_winner")),
        "threshold_or_trigger": "Flip if the lower-deployability line clears implementation, timing, or source blockers enough to close a 5 point readiness gap.",
    })
    return rows


def _evidence_diff(candidates: list[dict[str, Any]], evidence_winner: str) -> dict[str, Any]:
    if len(candidates) < 2:
        return {
            "stronger_evidence": "tie",
            "unresolved_fields": [],
            "candidate_a_weak_fields": [],
            "candidate_b_weak_fields": [],
            "evidence_needed_to_decide": ["Select two candidates."],
        }
    weak_a = _source_issue_fields(candidates[0])
    weak_b = _source_issue_fields(candidates[1])
    unresolved: list[str] = []
    for value in weak_a + weak_b:
        if value not in unresolved:
            unresolved.append(value)
    needed = [
        "Confirm benchmark lineage and exposure scope from issuer or verified index source.",
        "Confirm actual trading line, spread, AUM, and route freshness for the selected exchange.",
    ]
    if unresolved:
        needed.append(f"Resolve weak fields: {', '.join(unresolved[:5])}.")
    return {
        "stronger_evidence": evidence_winner if evidence_winner in {"candidate_a", "candidate_b", "tie"} else "tie",
        "unresolved_fields": unresolved[:10],
        "candidate_a_weak_fields": weak_a,
        "candidate_b_weak_fields": weak_b,
        "evidence_needed_to_decide": needed,
    }


def _build_compare_decision(
    candidates: list[dict[str, Any]],
    *,
    verdict: str,
    readiness_state: str,
) -> dict[str, Any]:
    substitution = _substitution_assessment(verdict, candidates, readiness_state)
    winners = _winner_summary(candidates, verdict)
    raw_sleeve_id = str((candidates[0] if candidates else {}).get("sleeve_key") or "").strip()
    sleeve_id = f"sleeve_{raw_sleeve_id}" if raw_sleeve_id and not raw_sleeve_id.startswith("sleeve_") else raw_sleeve_id
    return {
        "compare_id": ":".join(str(candidate.get("candidate_id") or "") for candidate in candidates[:2]),
        "sleeve_id": sleeve_id,
        "candidate_a_id": str((candidates[0] if candidates else {}).get("candidate_id") or ""),
        "candidate_b_id": str((candidates[1] if len(candidates) > 1 else {}).get("candidate_id") or ""),
        "substitution_assessment": substitution,
        "winner_summary": winners,
        "decision_rule": _decision_rule(candidates, winners, substitution),
        "delta_table": _build_decision_delta_table(candidates),
        "portfolio_consequence": {
            "candidate_a": _portfolio_consequence_for(candidates[0], candidates[1]) if len(candidates) > 1 else None,
            "candidate_b": _portfolio_consequence_for(candidates[1], candidates[0]) if len(candidates) > 1 else None,
        },
        "scenario_winners": _scenario_winners(candidates),
        "flip_conditions": _flip_conditions(candidates, winners),
        "evidence_diff": _evidence_diff(candidates, winners.get("evidence_winner", "tie")),
    }


def _substitution_verdict(candidates: list[dict[str, Any]]) -> str:
    if len(candidates) < 2:
        return "insufficient"
    if not _same_sleeve(candidates):
        return "different_jobs"
    sleeve_key = str(candidates[0].get("sleeve_key") or "")
    benchmark_keys = [str(candidate.get("benchmark_key") or "") for candidate in candidates]
    exposure_summaries = [str(candidate.get("exposure_summary") or "") for candidate in candidates]
    if _all_equal(benchmark_keys) and _all_equal(exposure_summaries):
        return "direct_substitutes"
    if sleeve_key in {"real_assets", "alternatives", "convex"} and len(set(benchmark_keys)) == len(benchmark_keys):
        return "different_jobs"
    return "partial_substitutes"


def _readiness_state(candidates: list[dict[str, Any]]) -> tuple[str, str]:
    if len(candidates) < 2:
        return "insufficient", "Select at least two Blueprint candidates before running compare."
    if not _same_sleeve(candidates):
        return "cross_sleeve", "These candidates come from different sleeves, so compare can explain differences but not declare a clean substitute."
    missing_benchmark = [
        candidate["symbol"]
        for candidate in candidates
        if not str(candidate.get("benchmark_full_name") or "").strip()
    ]
    if len(missing_benchmark) == len(candidates):
        return "insufficient", "Benchmark identity is still too thin across the selected set for a clean substitute judgment."
    return "ready", "Compare is anchored to the same sleeve, so the main question is whether these products are true substitutes or different tools."


def _compare_dimensions(candidates: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    dimensions: list[dict[str, Any]] = []

    benchmark_values = [str(candidate.get("benchmark_full_name") or "Unavailable") for candidate in candidates]
    dimensions.append(
        _compare_dimension(
            dimension_id="benchmark_identity",
            label="Benchmark identity",
            group="mandate",
            candidates=candidates,
            values=benchmark_values,
            importance="high",
            rationale="This shows whether the products are tracking the same benchmark family or doing different sleeve jobs.",
        )
    )

    exposure_values = [str(candidate.get("exposure_summary") or "Unavailable") for candidate in candidates]
    dimensions.append(
        _compare_dimension(
            dimension_id="exposure_type",
            label="Exposure",
            group="mandate",
            candidates=candidates,
            values=exposure_values,
            importance="high",
            rationale="Exposure differences matter because they can turn an apparent substitute into a different portfolio tool.",
        )
    )

    sleeve_fit_numeric = [float(dict(candidate.get("score_decomposition") or {}).get("sleeve_fit_score") or 0.0) for candidate in candidates]
    dimensions.append(
        _compare_dimension(
            dimension_id="sleeve_fit",
            label="Sleeve fit",
            group="mandate",
            candidates=candidates,
            values=[str(int(value)) for value in sleeve_fit_numeric],
            importance="high",
            rationale="This shows which candidate is the cleaner fit for the sleeve job once benchmark identity and exposure are in view.",
            higher_is_better=True,
            numeric_values=sleeve_fit_numeric,
        )
    )

    benchmark_fidelity_numeric = [
        float(dict(candidate.get("score_decomposition") or {}).get("benchmark_fidelity_score") or 0.0)
        for candidate in candidates
    ]
    dimensions.append(
        _compare_dimension(
            dimension_id="benchmark_fidelity",
            label="Benchmark fidelity",
            group="mandate",
            candidates=candidates,
            values=[str(int(value)) for value in benchmark_fidelity_numeric],
            importance="high",
            rationale="Benchmark fidelity matters because two low-cost ETFs are not real substitutes if one still sits on thinner benchmark support.",
            higher_is_better=True,
            numeric_values=benchmark_fidelity_numeric,
        )
    )

    decision_numeric = [float(_decision_score(candidate.get("investor_decision_state"))) for candidate in candidates]
    decision_values = [_humanize(candidate.get("investor_decision_state")) for candidate in candidates]
    dimensions.append(
        _compare_dimension(
            dimension_id="decision_state",
            label="Decision state",
            group="decision",
            candidates=candidates,
            values=decision_values,
            importance="high",
            rationale="This shows whether one candidate is actually usable now while another still sits behind a blocker.",
            higher_is_better=True,
            numeric_values=decision_numeric,
        )
    )

    integrity_numeric = [float(_integrity_score(dict(candidate.get("source_integrity_summary") or {}).get("state"))) for candidate in candidates]
    integrity_values = [
        _humanize(
            dict(candidate.get("source_integrity_summary") or {}).get("integrity_label")
            or dict(candidate.get("source_integrity_summary") or {}).get("state")
        )
        for candidate in candidates
    ]
    dimensions.append(
        _compare_dimension(
            dimension_id="source_integrity",
            label="Source integrity",
            group="evidence",
            candidates=candidates,
            values=integrity_values,
            importance="high",
            rationale="This shows whether the compare is being made on cleaner evidence or still thinner support.",
            higher_is_better=True,
            numeric_values=integrity_numeric,
        )
    )

    ter_numbers = [float(candidate.get("ter_bps")) if candidate.get("ter_bps") is not None else None for candidate in candidates]
    dimensions.append(
        _compare_dimension(
            dimension_id="ter_delta",
            label="Cost",
            group="implementation",
            candidates=candidates,
            values=[_format_bps(value) if value is not None else "Unavailable" for value in ter_numbers],
            importance="high",
            rationale="Cost matters once the candidates are doing a sufficiently similar sleeve job.",
            higher_is_better=False,
            numeric_values=ter_numbers,
        )
    )

    spread_numbers = [float(candidate.get("spread_proxy_bps")) if candidate.get("spread_proxy_bps") is not None else None for candidate in candidates]
    dimensions.append(
        _compare_dimension(
            dimension_id="spread_proxy_delta",
            label="Trading spread",
            group="implementation",
            candidates=candidates,
            values=[_format_bps(value) if value is not None else "Unavailable" for value in spread_numbers],
            importance="high",
            rationale="Spread differences matter when implementation friction is a real part of the sleeve decision.",
            higher_is_better=False,
            numeric_values=spread_numbers,
        )
    )

    aum_rank_values = [
        float(_AUM_STATE_RANK.get(str(candidate.get("aum_state") or "missing"), 0)) * 10.0
        + (float(candidate.get("aum_usd") or 0.0) / 1_000_000_000.0 if candidate.get("aum_usd") is not None else 0.0)
        for candidate in candidates
    ]
    dimensions.append(
        _compare_dimension(
            dimension_id="aum_state",
            label="AUM support",
            group="implementation",
            candidates=candidates,
            values=[_format_aum(candidate.get("aum_usd"), candidate.get("aum_state")) for candidate in candidates],
            importance="medium",
            rationale="AUM helps frame scale and implementation confidence, especially when several candidates look otherwise similar.",
            higher_is_better=True,
            numeric_values=aum_rank_values,
        )
    )

    dimensions.append(
        _compare_dimension(
            dimension_id="blocker_category",
            label="Main restriction",
            group="decision",
            candidates=candidates,
            values=[_humanize(candidate.get("blocker_category")) if candidate.get("blocker_category") else "None" for candidate in candidates],
            importance="medium",
            rationale="Restriction type can matter more than a small cost edge when one candidate still needs cleaner truth.",
        )
    )

    market_path_backing = []
    for candidate in candidates:
        support = dict(candidate.get("market_path_support") or {})
        if not support:
            market_path_backing.append("Unavailable")
        else:
            state = str(support.get("market_setup_state") or "").strip().lower()
            if state == "proxy_usable":
                proxy_symbol = str(support.get("proxy_symbol") or support.get("driving_symbol") or "").strip().upper()
                market_path_backing.append(f"Proxy-backed via {proxy_symbol}" if proxy_symbol else "Proxy-backed support")
            elif state == "stale":
                market_path_backing.append("Stored market setup")
            elif state == "degraded":
                market_path_backing.append("Degraded market setup")
            elif state == "unavailable":
                market_path_backing.append("Unavailable")
            else:
                driving_symbol = str(support.get("driving_symbol") or "").strip().upper()
                market_path_backing.append(f"Direct {driving_symbol}" if driving_symbol else "Direct-series support")
    dimensions.append(
        _compare_dimension(
            dimension_id="market_path_provenance",
            label="Market-path backing",
            group="market_path",
            candidates=candidates,
            values=market_path_backing,
            importance="medium",
            rationale="Use this only as secondary context: direct series support is cleaner than proxy-backed support when the path read is otherwise similar.",
        )
    )

    score_numeric = [float(dict(candidate.get("score_decomposition") or {}).get("total_score") or 0.0) for candidate in candidates]
    dimensions.append(
        _compare_dimension(
            dimension_id="score_total",
            label="Recommendation score",
            group="decision",
            candidates=candidates,
            values=[str(int(value)) for value in score_numeric],
            importance="medium",
            rationale="The recommendation score summarizes long-term recommendation merit with a bounded deployability contribution, then caps the result when truth confidence, routing, or hard blockers remain unresolved.",
            higher_is_better=True,
            numeric_values=score_numeric,
        )
    )

    dimensions.append(
        _compare_dimension(
            dimension_id="replication_method",
            label="Replication",
            group="implementation",
            candidates=candidates,
            values=[str(candidate.get("replication_method") or "Unavailable") for candidate in candidates],
            importance="medium",
            rationale="Replication matters when wrapper complexity and tracking behaviour affect the sleeve read.",
        )
    )

    dimensions.append(
        _compare_dimension(
            dimension_id="tax_posture",
            label="Tax posture",
            group="implementation",
            candidates=candidates,
            values=[str(candidate.get("tax_posture_summary") or "Unavailable") for candidate in candidates],
            importance="medium",
            rationale="Wrapper and tax posture can matter even when the benchmark looks similar.",
        )
    )

    weight_values = [_format_percent(candidate.get("current_weight_pct")) for candidate in candidates]
    dimensions.append(
        _compare_dimension(
            dimension_id="current_weight",
            label="Current holding weight",
            group="portfolio",
            candidates=candidates,
            values=weight_values,
            importance="low",
            rationale="Weight state matters because a candidate already held is not the same decision as a fresh add.",
        )
    )

    discriminating = [str(dimension["dimension_id"]) for dimension in dimensions if dimension.get("discriminating")]
    return dimensions, discriminating


def _group_label(group: str | None) -> str:
    return {
        "mandate": "Mandate and sleeve job",
        "decision": "Decision posture",
        "evidence": "Evidence quality",
        "implementation": "Implementation friction",
        "market_path": "Market-path support",
        "portfolio": "Portfolio context",
    }.get(str(group or ""), _humanize(group))


def _dimension_groups(dimensions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered_groups: list[str] = []
    group_map: dict[str, list[str]] = {}
    for dimension in dimensions:
        group = str(dimension.get("group") or "other")
        if group not in group_map:
            ordered_groups.append(group)
            group_map[group] = []
        group_map[group].append(str(dimension.get("dimension_id") or dimension.get("dimension") or ""))
    return [
        {
            "group_id": group,
            "label": _group_label(group),
            "dimension_ids": group_map[group],
        }
        for group in ordered_groups
    ]


def _dimension_priority_order(dimensions: list[dict[str, Any]]) -> list[str]:
    importance_rank = {"high": 0, "medium": 1, "low": 2}
    ordered = sorted(
        dimensions,
        key=lambda item: (
            0 if item.get("discriminating") else 1,
            importance_rank.get(str(item.get("importance") or "medium"), 3),
            str(item.get("dimension_id") or item.get("dimension") or ""),
        ),
    )
    return [str(item.get("dimension_id") or item.get("dimension") or "") for item in ordered]


def _threshold_delta(support: dict[str, Any] | None, threshold_id: str) -> float | None:
    if not isinstance(support, dict):
        return None
    for item in list(support.get("threshold_map") or []):
        if str(item.get("threshold_id") or "") != threshold_id:
            continue
        try:
            return float(item.get("delta_pct"))
        except (TypeError, ValueError):
            return None
    return None


def _compare_market_path_enrichment(
    candidates: list[dict[str, Any]],
    *,
    readiness_state: str,
    verdict: str,
) -> dict[str, Any] | None:
    if len(candidates) < 2:
        return None
    if readiness_state != "ready" or verdict not in {"direct_substitutes", "partial_substitutes"}:
        return None
    supports = [
        dict(candidate.get("market_path_support") or {})
        if isinstance(candidate.get("market_path_support"), dict)
        else None
        for candidate in candidates[:2]
    ]
    if any(not isinstance(support, dict) or not support for support in supports):
        try:
            supports = [
                build_candidate_market_path_support(str(candidate.get("candidate_id") or ""), allow_refresh=False)
                for candidate in candidates[:2]
            ]
        except Exception:  # noqa: BLE001 - compare must not fail when optional market-path enrichment is unavailable.
            return None
    if any(not isinstance(support, dict) for support in supports):
        return None
    support_a = dict(supports[0] or {})
    support_b = dict(supports[1] or {})
    allowed_usefulness = {"strong", "usable", "usable_with_caution"}
    usefulness_labels = {str(support_a.get("usefulness_label") or ""), str(support_b.get("usefulness_label") or "")}
    if not usefulness_labels.issubset(allowed_usefulness):
        return None
    if any(str(support.get("suppression_reason") or "").strip() for support in (support_a, support_b)):
        return None
    if any(str(support.get("eligibility_state") or "") != "eligible" for support in (support_a, support_b)):
        return None
    if (
        str(support_a.get("forecast_interval") or "") != str(support_b.get("forecast_interval") or "")
        or int(support_a.get("forecast_horizon") or 0) != int(support_b.get("forecast_horizon") or 0)
    ):
        return None
    quality_a = str(dict(support_a.get("series_quality_summary") or {}).get("quality_label") or "")
    quality_b = str(dict(support_b.get("series_quality_summary") or {}).get("quality_label") or "")
    if "broken" in {quality_a, quality_b}:
        return None
    base_delta_a = _threshold_delta(support_a, "base_case")
    base_delta_b = _threshold_delta(support_b, "base_case")
    downside_delta_a = _threshold_delta(support_a, "downside_case")
    downside_delta_b = _threshold_delta(support_b, "downside_case")
    if None in {base_delta_a, base_delta_b, downside_delta_a, downside_delta_b}:
        return None
    fragility_a = float(support_a.get("candidate_fragility_score") or 0.0)
    fragility_b = float(support_b.get("candidate_fragility_score") or 0.0)
    quality_score_a = float(support_a.get("path_quality_score") or 0.0)
    quality_score_b = float(support_b.get("path_quality_score") or 0.0)
    if abs((quality_score_a - fragility_a) - (quality_score_b - fragility_b)) < 4.0:
        stability_advantage = "tie"
    else:
        stability_advantage = candidates[0]["candidate_id"] if (quality_score_a - fragility_a) > (quality_score_b - fragility_b) else candidates[1]["candidate_id"]
    path_asymmetry = round(float(base_delta_a) - float(base_delta_b), 3)
    downside_asymmetry = round(float(downside_delta_a) - float(downside_delta_b), 3)
    advantage_label = (
        "Neither candidate has a meaningful stability edge."
        if stability_advantage == "tie"
        else f"{next(candidate['symbol'] for candidate in candidates[:2] if candidate['candidate_id'] == stability_advantage)} carries the cleaner market-path stability profile."
    )
    return {
        "path_asymmetry": path_asymmetry,
        "downside_asymmetry": downside_asymmetry,
        "stability_advantage": stability_advantage,
        "market_path_compare_note": (
            f"Base-path asymmetry is {path_asymmetry:.2f} pct points and downside asymmetry is {downside_asymmetry:.2f} pct points. "
            f"{advantage_label}"
        ),
    }


def _winner_for_sleeve_job(
    *,
    leader: dict[str, Any] | None,
    readiness_state: str,
    verdict: str,
) -> str | None:
    if leader is None:
        return None
    if readiness_state != "ready":
        return f"{leader['name']} leads on current evidence, but the pair is not clean enough for a full sleeve-job substitution call yet."
    if verdict == "different_jobs":
        return f"{leader['name']} leads on score, but these candidates still do different sleeve jobs."
    return f"{leader['name']} is the cleaner candidate for the current sleeve job on benchmark fit, implementation, and decision readiness."


def _loser_weakness_summary(candidates: list[dict[str, Any]], leader_id: str | None) -> str | None:
    loser = next((candidate for candidate in candidates if str(candidate.get("candidate_id") or "") != str(leader_id or "")), None)
    if loser is None:
        return None
    gate = dict(loser.get("recommendation_gate") or {})
    if gate.get("blocked_reasons"):
        return str(list(gate.get("blocked_reasons") or [])[0])
    if loser.get("blocker_category"):
        return f"{loser['name']} is still bounded by {str(loser.get('blocker_category')).replace('_', ' ')}."
    integrity = dict(loser.get("source_integrity_summary") or {})
    return str(integrity.get("summary") or f"{loser['name']} still trails on current evidence quality.")


def _substitution_answer(
    *,
    verdict: str,
    leader: dict[str, Any] | None,
    readiness_state: str,
) -> str:
    if leader is None:
        return "Compare is not ready yet."
    if readiness_state != "ready":
        return "Compare can explain the gap, but it is not ready to answer the substitution question cleanly."
    if verdict == "direct_substitutes":
        return f"Yes. {leader['name']} is the cleaner substitute for the current sleeve job."
    if verdict == "partial_substitutes":
        return f"Partially. {leader['name']} leads, but the pair is not a perfect one-for-one substitute."
    return "No. These candidates are not clean substitutes for the same sleeve job."


def build(candidate_ids: list[str], sleeve_id: str | None = None) -> dict[str, object]:
    normalized_ids: list[str] = []
    for candidate_id in candidate_ids:
        raw = str(candidate_id or "").strip()
        if raw and raw not in normalized_ids:
            normalized_ids.append(raw)
    with _connection() as conn:
        explorer_contract = _latest_explorer_contract(conn)
        candidates = [
            _candidate_snapshot_from_explorer(explorer_contract, candidate_id, sleeve_id=sleeve_id)
            or _candidate_snapshot(conn, candidate_id, sleeve_id=sleeve_id)
            for candidate_id in normalized_ids[:2]
        ]

    readiness_state, readiness_note = _readiness_state(candidates)
    verdict = _substitution_verdict(candidates) if readiness_state != "insufficient" else "insufficient"
    leader = _candidate_leader(candidates) if candidates else None
    dimensions, discriminating_ids = _compare_dimensions(candidates) if candidates else ([], [])
    blocked_candidates = [
        str(candidate.get("blocker_category") or "")
        for candidate in candidates
        if candidate.get("blocker_category")
    ]
    explanations = build_compare_explanations(
        substitution_verdict=verdict,
        sleeve_name=(leader or {}).get("sleeve_name"),
        leader_name=(leader or {}).get("name"),
        leader_symbol=(leader or {}).get("symbol"),
        leader_reason=(leader or {}).get("recommendation_gate", {}).get("summary") if leader else None,
        blocked_candidates=blocked_candidates,
        discriminating_labels=[
            str(dimension.get("label") or "")
            for dimension in dimensions
            if dimension.get("discriminating")
        ],
    )
    generated_at = datetime.now(UTC).isoformat()
    leader_id = str((leader or {}).get("candidate_id") or normalized_ids[0] if normalized_ids else "")
    leader_name = str((leader or {}).get("name") or normalized_ids[0] if normalized_ids else "")
    market_path_enrichment = _compare_market_path_enrichment(candidates, readiness_state=readiness_state, verdict=verdict)
    dimension_groups = _dimension_groups(dimensions)
    dimension_priority_order = _dimension_priority_order(dimensions)
    substitution_answer = _substitution_answer(verdict=verdict, leader=leader, readiness_state=readiness_state)
    winner_for_sleeve_job = _winner_for_sleeve_job(leader=leader, readiness_state=readiness_state, verdict=verdict)
    loser_weakness_summary = _loser_weakness_summary(candidates, leader_id or None)
    compare_decision = _build_compare_decision(candidates, verdict=verdict, readiness_state=readiness_state)
    compare_investor_summary = " ".join(
        part
        for part in [
            str(dict(compare_decision.get("winner_summary") or {}).get("summary") or substitution_answer).strip(),
            str(explanations["substitution_rationale"] or "").strip(),
            str((market_path_enrichment or {}).get("market_path_compare_note") or "").strip(),
        ]
        if part
    ).strip()
    compare_candidates = [
        {
            "candidate_id": candidate["candidate_id"],
            "symbol": candidate["symbol"],
            "name": candidate["name"],
            "investor_decision_state": candidate.get("investor_decision_state"),
            "blocker_category": candidate.get("blocker_category"),
            "benchmark_full_name": candidate.get("benchmark_full_name"),
            "exposure_summary": candidate.get("exposure_summary"),
            "ter_bps": candidate.get("ter_bps"),
            "spread_proxy_bps": candidate.get("spread_proxy_bps"),
            "aum_usd": candidate.get("aum_usd"),
            "aum_state": candidate.get("aum_state"),
            "distribution_policy": candidate.get("distribution_policy"),
            "replication_method": _impl(candidate, "replication_method"),
            "domicile": _impl(candidate, "domicile"),
            "primary_trading_currency": _impl(candidate, "primary_trading_currency"),
            "primary_listing_exchange": _impl(candidate, "primary_listing_exchange"),
            "current_weight_pct": candidate.get("current_weight_pct"),
            "weight_state": candidate.get("weight_state"),
            "source_integrity_state": dict(candidate.get("source_integrity_summary") or {}).get("state"),
            "total_score": int(dict(candidate.get("score_decomposition") or {}).get("total_score") or 0),
            "recommendation_score": int(round(_score_value(candidate, "recommendation_score", "total_score"))),
            "investment_merit_score": int(round(_score_value(candidate, "investment_merit_score", "optimality_score", "recommendation_merit_score"))),
            "deployability_score": int(round(_score_value(candidate, "deployability_score", "deployment_score", "readiness_score"))),
            "truth_confidence_score": int(round(_score_value(candidate, "truth_confidence_score"))),
            "decision_summary": str((candidate.get("recommendation_gate") or {}).get("summary") or ""),
            "compare_card": _compare_candidate_card(candidate, verdict=verdict),
        }
        for candidate in candidates
    ]
    insufficient_dimensions = [
        str(dimension.get("dimension_id") or "")
        for dimension in dimensions
        if all(str(value.get("value") or "") == "Unavailable" for value in list(dimension.get("values") or []))
    ]
    freshness_state = get_freshness("market_price").freshness_class.value
    candidate_a = compare_candidates[0] if compare_candidates else None
    candidate_b = compare_candidates[1] if len(compare_candidates) > 1 else None
    contract = {
        "contract_version": _CONTRACT_VERSION,
        "surface_id": _SURFACE_ID,
        "generated_at": generated_at,
        "freshness_state": freshness_state,
        "surface_state": surface_state(
            "ready" if readiness_state != "insufficient" else "degraded",
            reason_codes=[] if readiness_state != "insufficient" else ["compare_insufficient"],
            summary="Compare is now backend-owned and recommendation-aware." if readiness_state != "insufficient" else readiness_note,
        ),
        "section_states": {
            "comparison_summary": ready_section() if candidates else degraded_section("no_candidates", "No candidates were selected for compare."),
            "dimensions": ready_section() if dimensions else degraded_section("no_compare_dimensions", "No compare dimensions were emitted."),
        },
        "compare_ids": normalized_ids,
        "sleeve_id": sleeve_id or (leader or {}).get("sleeve_key"),
        "sleeve_name": (leader or {}).get("sleeve_name"),
        "candidates": compare_candidates,
        "leader_candidate_id": leader_id or None,
        "compare_readiness_state": readiness_state,
        "compare_readiness_note": readiness_note,
        "substitution_verdict": verdict,
        "substitution_rationale": explanations["substitution_rationale"],
        "substitution_answer": substitution_answer,
        "winner_for_sleeve_job": winner_for_sleeve_job,
        "loser_weakness_summary": loser_weakness_summary,
        "change_the_read_summary": explanations["what_would_change"],
        "compare_investor_summary": compare_investor_summary,
        "compare_decision": compare_decision,
        "compare_dimensions": dimensions,
        "dimension_groups": dimension_groups,
        "dimension_priority_order": dimension_priority_order,
        "discriminating_dimension_ids": discriminating_ids,
        "insufficient_dimensions": insufficient_dimensions,
        "candidate_a_id": (candidate_a or {}).get("candidate_id") or normalized_ids[0] if normalized_ids else "",
        "candidate_b_id": (candidate_b or {}).get("candidate_id") or normalized_ids[1] if len(normalized_ids) > 1 else "",
        "candidate_a_name": (candidate_a or {}).get("name") or "",
        "candidate_b_name": (candidate_b or {}).get("name") or "",
        "who_leads": leader_id or "tie",
        "winner_name": leader_name,
        "why_leads": explanations["why_leads"],
        "where_loser_wins": None,
        "what_would_change_comparison": explanations["what_would_change"],
        "forecast_support": None,
        "flip_risk_note": None,
        "path_asymmetry": (market_path_enrichment or {}).get("path_asymmetry"),
        "downside_asymmetry": (market_path_enrichment or {}).get("downside_asymmetry"),
        "stability_advantage": (market_path_enrichment or {}).get("stability_advantage"),
        "market_path_compare_note": (market_path_enrichment or {}).get("market_path_compare_note"),
        "compare_summary": {
            "cleaner_for_sleeve_job": winner_for_sleeve_job,
            "main_separation": explanations["why_leads"] or explanations["substitution_rationale"],
            "change_trigger": explanations["what_would_change"] or readiness_note,
        },
        "dimensions": dimensions,
    }
    record_change(
        event_type="rebuild",
        surface_id="compare",
        summary=f"Blueprint compare rebuilt for {', '.join(normalized_ids[:2])}.",
        candidate_id=leader_id or None,
        change_trigger="compare_refresh",
        reason_summary=explanations["why_leads"],
        current_state="comparison_ready",
        implication_summary=explanations["substitution_rationale"],
        report_tab="competition",
        impact_level="low",
        deep_link_target={
            "target_type": "candidate_report",
            "target_id": leader_id,
            "tab": "competition",
        } if leader_id else None,
    )
    return contract
