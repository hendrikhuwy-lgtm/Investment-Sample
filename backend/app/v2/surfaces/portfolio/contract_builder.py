from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from app.v2.core.domain_objects import (
    CandidateAssessment,
    InstrumentTruth,
    MarketDataPoint,
    MarketSeriesTruth,
    PolicyBoundary,
    PortfolioPressure,
    PortfolioTruth,
    utc_now_iso,
)
from app.v2.contracts.chart_contracts import degraded_chart_panel, snapshot_comparison_panel
from app.v2.core.change_ledger import record_change
from app.v2.core.ips_targets import SleeveTargetProfile, get_ips_sleeve_profile, ordered_ips_sleeves
from app.v2.core.holdings_overlay import apply_overlay
from app.v2.core.interpretation_engine import interpret
from app.v2.core.mandate_rubric import apply_rubric
from app.v2.doctrine.doctrine_evaluator import evaluate
from app.v2.donors.portfolio_truth import get_portfolio_truth
from app.v2.features.forecast_feature_service import build_candidate_support_bundle
from app.v2.portfolio.control import build_portfolio_status
from app.v2.sources.registry import get_macro_adapter
from app.v2.surfaces.common import degraded_section, empty_section, ready_section, surface_state
from app.v2.translators.macro_signal_translator import translate as translate_macro_signal


_CONTRACT_VERSION = "0.1.0"
_SURFACE_ID = "portfolio"
_REVIEW_DRIFT_THRESHOLD = 2.0
_OFF_TARGET_DRIFT_THRESHOLD = 5.0
_SLEEVE_ASSET_CLASS = {
    "global_equity": "equity",
    "ig_bond": "fixed_income",
    "cash": "cash",
    "real_asset": "real_assets",
    "alt": "multi_asset",
    "convex": "multi_asset",
}
_FORECAST_PROXY_SYMBOL = {
    "global_equity": "ACWI",
    "ig_bond": "AGG",
    "cash": "DXY",
    "real_asset": "GLD",
    "alt": "ACWI",
    "convex": "AGG",
}


def _sleeve_label(sleeve_id: str) -> str:
    profile = get_ips_sleeve_profile(sleeve_id)
    if profile is not None:
        return profile.sleeve_name
    return sleeve_id.replace("_", " ").title()


def _asset_class(sleeve_id: str) -> str:
    return _SLEEVE_ASSET_CLASS.get(sleeve_id, "multi_asset")


def _current_pct(exposures: dict[str, float], sleeve_id: str) -> float:
    value = exposures.get(sleeve_id)
    try:
        return round(float(value or 0.0) * 100.0, 2)
    except (TypeError, ValueError):
        return 0.0


def _current_pct_or_none(exposures: dict[str, float], sleeve_id: str) -> float | None:
    if sleeve_id not in exposures:
        return None
    return _current_pct(exposures, sleeve_id)


def _band_status(current_pct: float | None, profile: SleeveTargetProfile) -> str:
    if current_pct is None:
        return "awaiting_holdings"
    return "in_band" if profile.min_pct <= current_pct <= profile.max_pct else "out_of_band"


def _status_for_drift(drift_pct: float) -> str:
    absolute_drift = abs(drift_pct)
    if absolute_drift >= _OFF_TARGET_DRIFT_THRESHOLD:
        return "off_target"
    if absolute_drift >= _REVIEW_DRIFT_THRESHOLD:
        return "needs_review"
    return "on_target"


def _profile_current_pct(portfolio: PortfolioTruth, profile: SleeveTargetProfile) -> float | None:
    exposures = dict(portfolio.exposures or {})
    if not portfolio.holdings:
        return None
    current_pct = _current_pct_or_none(exposures, profile.portfolio_sleeve_id)
    if current_pct is not None:
        return current_pct
    current_pct = _current_pct_or_none(exposures, profile.sleeve_key)
    if current_pct is not None:
        return current_pct
    return 0.0 if profile.is_nested else None


def _drift_rows(portfolio: PortfolioTruth) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for profile in ordered_ips_sleeves():
        target_anchor = round(profile.drift_anchor_pct, 2)
        current_pct = _profile_current_pct(portfolio, profile)
        drift_pct = round(current_pct - target_anchor, 2) if current_pct is not None else None
        rows.append(
            {
                "sleeve_id": profile.sleeve_key,
                "sleeve_name": profile.sleeve_name,
                "rank": profile.rank,
                "target_pct": target_anchor,
                "target_display": profile.target_display,
                "min_pct": profile.min_pct,
                "max_pct": profile.max_pct,
                "sort_midpoint_pct": profile.sort_midpoint_pct,
                "is_nested": profile.is_nested,
                "parent_sleeve_id": profile.parent_sleeve_id,
                "parent_sleeve_name": profile.parent_sleeve_name,
                "counts_as_top_level_total": profile.counts_as_top_level_total,
                "target_label": profile.target_label,
                "range_label": profile.range_label,
                "current_pct": current_pct,
                "drift_pct": drift_pct,
                "status": _status_for_drift(drift_pct or 0.0) if drift_pct is not None else "awaiting_holdings",
                "band_status": _band_status(current_pct, profile),
            }
        )
    return rows


def _pressure_for_row(portfolio: PortfolioTruth, row: dict[str, Any]) -> tuple[PortfolioPressure, CandidateAssessment]:
    sleeve_id = str(row["sleeve_id"])
    drift_pct = float(row.get("drift_pct") or 0.0)
    sleeve_label = str(row.get("sleeve_name") or _sleeve_label(sleeve_id))
    observed_at = portfolio.as_of or utc_now_iso()
    instrument = InstrumentTruth(
        instrument_id=f"instrument_{sleeve_id}",
        symbol=sleeve_id.upper(),
        name=f"{sleeve_label} Sleeve",
        asset_class=_asset_class(sleeve_id),
        metrics={
            "sleeve_affiliation": sleeve_id,
            "benchmark_authority_level": "direct",
            "target_pct": row["target_pct"],
            "current_pct": row["current_pct"],
            "drift_pct": drift_pct,
        },
        as_of=observed_at,
    )
    market = MarketSeriesTruth(
        series_id=f"series_{sleeve_id}_drift",
        label=f"{sleeve_label} drift",
        frequency="snapshot",
        units="percent",
        points=[
            MarketDataPoint(at=observed_at, value=100.0),
            MarketDataPoint(at=observed_at, value=100.0 + drift_pct),
        ],
        as_of=observed_at,
    )
    signal, interpretation = interpret(instrument, market)
    assessment = CandidateAssessment(
        candidate_id=f"candidate_{portfolio.portfolio_id}_{sleeve_id}",
        sleeve_id=sleeve_id,
        instrument=instrument,
        interpretation=interpretation,
        mandate_fit="outside" if row["status"] == "off_target" else "watch",
        conviction=round(min(0.95, max(0.35, abs(drift_pct) / 10.0)), 2),
        score_breakdown={"drift_pct": abs(drift_pct)},
        key_supports=[],
        key_risks=[signal.summary],
        holdings_context={
            "current_pct": row["current_pct"],
            "target_pct": row["target_pct"],
            "drift_pct": drift_pct,
        },
    )
    level = "acute" if row["status"] == "off_target" else "elevated"
    pressure = PortfolioPressure(
        portfolio_id=portfolio.portfolio_id,
        pressure_id=f"pressure_{portfolio.portfolio_id}_{sleeve_id}",
        level=level,
        drivers=[signal.summary],
        affected_sleeves=[sleeve_id],
        summary=f"{sleeve_label} is {abs(drift_pct):.2f} percentage points from target.",
    )
    return pressure, assessment


def _dominant_row(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not rows:
        return None
    return max(rows, key=lambda row: (abs(float(row.get("drift_pct") or 0.0)), str(row["sleeve_id"])))


def _mandate_summary(portfolio: PortfolioTruth, rows: list[dict[str, Any]]) -> tuple[str, str]:
    dominant_row = _dominant_row(rows)
    if dominant_row is None:
        return "no_data", "review"

    dominant_pressure, dominant_assessment = _pressure_for_row(portfolio, dominant_row)
    restraints = evaluate(dominant_assessment)
    drifted = any(row["status"] != "on_target" for row in rows)
    boundary = PolicyBoundary(
        boundary_id=f"boundary_{portfolio.portfolio_id}_allocation_drift",
        code="allocation_drift",
        action_boundary="review" if drifted else None,
        scope="portfolio",
        severity="warning" if drifted else "info",
        passes=not drifted,
        summary=(
            "Portfolio sleeves remain inside the current target bands."
            if not drifted
            else f"{_sleeve_label(str(dominant_row['sleeve_id']))} is outside the preferred allocation band."
        ),
        required_action=(
            None
            if not drifted
            else f"Review {_sleeve_label(str(dominant_row['sleeve_id']))} before treating the portfolio as back on mandate."
        ),
    )
    constraint_summary = apply_rubric(dominant_assessment, boundary, restraints)
    mandate_state = "drifted" if drifted else "on_mandate"
    action_posture = "review" if drifted else constraint_summary.visible_decision_state.allowed_action
    if dominant_pressure.level == "acute":
        action_posture = "review_now"
    return mandate_state, action_posture


def _blueprint_consequence(rows: list[dict[str, Any]]) -> str | None:
    off_target = [row for row in rows if row["status"] == "off_target"]
    if not off_target:
        return None
    lead = max(off_target, key=lambda row: abs(float(row.get("drift_pct") or 0.0)))
    return (
        f"Portfolio drift now matters because {str(lead.get('sleeve_name') or _sleeve_label(str(lead['sleeve_id'])))} is "
        f"{abs(float(lead.get('drift_pct') or 0.0)):.2f} points from the strategic target anchor, so blueprint review should stay live until weights normalize."
    )


def _macro_rows() -> list[dict[str, Any]]:
    adapter = get_macro_adapter()
    try:
        payload = adapter.fetch_all()
    except Exception:
        return []
    return [row for row in payload if isinstance(row, dict)]


def _macro_affects_holdings(portfolio: PortfolioTruth, sleeve_id: str) -> bool:
    try:
        return float(portfolio.exposures.get(sleeve_id, 0.0) or 0.0) > 0
    except (TypeError, ValueError):
        return False


def _daily_brief_consequence(portfolio: PortfolioTruth) -> str | None:
    if not portfolio.holdings:
        return None

    for raw in _macro_rows():
        truth = translate_macro_signal(raw)
        indicator_id = str(truth.indicators.get("indicator_id") or "").upper()
        current_value = truth.indicators.get("current_value")
        previous_value = truth.indicators.get("previous_value")
        if current_value in {None, ""} or previous_value in {None, ""}:
            continue

        if indicator_id == "SP500" and _macro_affects_holdings(portfolio, "global_equity"):
            sleeve_id = "global_equity"
        elif indicator_id in {"DGS10", "FEDFUNDS"} and (
            _macro_affects_holdings(portfolio, "ig_bond") or _macro_affects_holdings(portfolio, "cash")
        ):
            sleeve_id = "ig_bond" if _macro_affects_holdings(portfolio, "ig_bond") else "cash"
        elif indicator_id == "CPIAUCSL" and _macro_affects_holdings(portfolio, "real_asset"):
            sleeve_id = "real_asset"
        else:
            continue

        return (
            f"Macro context needs attention because {truth.summary.lower()} "
            f"That can change the read on {_sleeve_label(sleeve_id)} holdings even before the target bands move."
        )
    return None


def _what_matters_now(rows: list[dict[str, Any]], pressures: Iterable[PortfolioPressure]) -> str:
    significant_pressures = list(pressures)
    if not rows:
        return "No current holdings are available, so portfolio drift cannot be assessed yet."
    if significant_pressures:
        dominant_row = _dominant_row([row for row in rows if row["status"] != "on_target"])
        lead = next(
            (
                pressure
                for pressure in significant_pressures
                if dominant_row is not None and pressure.affected_sleeves == [str(dominant_row["sleeve_id"])]
            ),
            significant_pressures[0],
        )
        return f"Portfolio drift needs attention because {lead.summary} {lead.drivers[0]}"
    return "Sleeve weights remain close to target, so no immediate portfolio drift pressure is visible."


def _work_items(portfolio: PortfolioTruth, significant_rows: list[dict[str, Any]], all_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for row in significant_rows:
        sleeve_id = str(row["sleeve_id"])
        urgency = "act" if row["status"] == "off_target" else "review"
        drift_abs = abs(float(row["drift_pct"]))
        affected_holdings = [
            str(h.get("symbol") or "").upper()
            for h in portfolio.holdings
            if isinstance(h, dict) and str(h.get("sleeve") or "") == sleeve_id
        ]
        items.append({
            "work_id": f"work_{portfolio.portfolio_id}_{sleeve_id}",
            "title": f"{_sleeve_label(sleeve_id)} allocation review",
            "urgency": urgency,
            "affected_sleeves": [sleeve_id],
            "affected_holdings": affected_holdings,
            "action_boundary": (
                f"Rebalance {_sleeve_label(sleeve_id)} before treating portfolio as on mandate."
            ),
            "what_invalidates_view": (
                f"If drift falls below {_REVIEW_DRIFT_THRESHOLD:.1f}%, this item can be closed."
                if drift_abs < _OFF_TARGET_DRIFT_THRESHOLD
                else f"Reduce drift to within {_REVIEW_DRIFT_THRESHOLD:.1f}% of target to resolve."
            ),
        })
    return items


def _holdings_rows(portfolio: PortfolioTruth, all_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    off_target_sleeves = {str(row["sleeve_id"]) for row in all_rows if row["status"] != "on_target"}
    total_value = 0.0
    sleeve_values: dict[str, float] = {}
    normalized_holdings: list[dict[str, Any]] = []
    for holding in portfolio.holdings:
        if not isinstance(holding, dict):
            continue
        quantity = float(holding.get("quantity") or 0.0)
        cost_basis = float(holding.get("cost_basis") or 0.0)
        sleeve_id = str(holding.get("sleeve") or "")
        market_value = max(0.0, quantity * cost_basis)
        total_value += market_value
        sleeve_values[sleeve_id] = sleeve_values.get(sleeve_id, 0.0) + market_value
        normalized_holdings.append({**holding, "_market_value": market_value, "_sleeve_id": sleeve_id})

    result: list[dict[str, Any]] = []
    for h in normalized_holdings:
        symbol = str(h.get("symbol") or "").upper()
        sleeve_id = str(h.get("_sleeve_id") or "")
        name = str(h.get("name") or symbol)
        market_value = float(h.get("_market_value") or 0.0)
        weight_pct = round((market_value / total_value) * 100.0, 2) if total_value > 0 else None
        sleeve_total = float(sleeve_values.get(sleeve_id) or 0.0)
        profile = get_ips_sleeve_profile(sleeve_id)
        sleeve_target_pct = float(profile.drift_anchor_pct) if profile is not None else 0.0
        target_pct = round(((market_value / sleeve_total) * sleeve_target_pct), 2) if sleeve_total > 0 and sleeve_target_pct > 0 else None
        drift_pct = round(weight_pct - target_pct, 2) if weight_pct is not None and target_pct is not None else None
        review_status = "review" if sleeve_id in off_target_sleeves else "monitor"
        result.append({
            "holding_id": f"holding_{portfolio.portfolio_id}_{symbol}",
            "symbol": symbol,
            "name": name,
            "sleeve_id": sleeve_id,
            "market_value": round(market_value, 2),
            "weight_pct": weight_pct,
            "target_pct": target_pct,
            "drift_pct": drift_pct,
            "review_status": review_status,
            "action_boundary": None,
            "next_review_reason": (
                f"Sleeve drift check for {_sleeve_label(sleeve_id) if sleeve_id else symbol}."
            ),
        })
    return result


def _forecast_watchlist(portfolio: PortfolioTruth, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ranked_rows = sorted(rows, key=lambda row: abs(float(row["drift_pct"])), reverse=True)
    watch_rows = [row for row in ranked_rows if abs(float(row["drift_pct"])) >= 1.0][:3]
    if not watch_rows:
        return []

    watches: list[dict[str, Any]] = []
    for row in watch_rows:
        sleeve_id = str(row["sleeve_id"])
        proxy_symbol = _FORECAST_PROXY_SYMBOL.get(sleeve_id)
        if not proxy_symbol:
            continue
        label = _sleeve_label(sleeve_id)
        drift_pct = float(row["drift_pct"])
        summary = (
            f"{label} is {abs(drift_pct):.2f} percentage points from target, so keep a cross-sleeve pressure watch active."
        )
        bundle = build_candidate_support_bundle(
            candidate_id=f"portfolio_watch_{portfolio.portfolio_id}_{sleeve_id}",
            symbol=proxy_symbol,
            label=label,
            sleeve_purpose=f"{label} portfolio watch",
            implication=summary,
            summary=summary,
            current_value=float(row["current_pct"]),
            history=[],
            timestamps=[],
            surface_name="portfolio",
        )
        watches.append(
            {
                "label": label,
                "summary": summary,
                "forecast_support": bundle["forecast_support"],
            }
        )
    return watches


def build(account_id: str) -> dict[str, object]:
    portfolio = get_portfolio_truth(account_id)
    import sqlite3
    from app.config import get_db_path

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    try:
        portfolio_status = build_portfolio_status(conn, account_id=account_id)
    finally:
        conn.close()
    holdings = portfolio.holdings or None
    if not portfolio.holdings:
        rows = _drift_rows(portfolio)
        portfolio_source_state = dict(portfolio_status.get("portfolio_source_state") or {})
        active_upload = portfolio_status.get("active_upload")
        empty_summary = str(
            portfolio_source_state.get("summary")
            or (
                "A holdings upload is available but no active positions have been promoted into the live portfolio yet."
                if active_upload
                else "No current holdings are available, so portfolio drift cannot be assessed yet."
            )
        )
        base_contract = {
            "contract_version": _CONTRACT_VERSION,
            "surface_id": _SURFACE_ID,
            "generated_at": utc_now_iso(),
            "freshness_state": "no_data",
            "surface_state": surface_state(
                "empty",
                reason_codes=["no_holdings"],
                summary="Portfolio keeps the full Cortex structure visible while no holdings are loaded.",
            ),
            "section_states": {
                "summary": ready_section(),
                "allocation": ready_section(),
                "holdings": empty_section("no_holdings", "No holdings are loaded yet."),
                "work_queue": empty_section("no_work_items", "No work items are available until holdings are loaded."),
                "upload_sync": empty_section("no_holdings", "No upload or sync state is available yet."),
            },
            "account_id": account_id,
            "mandate_state": "no_data",
            "what_matters_now": empty_summary,
            "action_posture": "review_upload_state" if active_upload else "wait_for_holdings",
            "sleeve_drift_summary": rows,
            "allocation_chart_panels": [
                degraded_chart_panel(
                    panel_id="portfolio_allocation_degraded",
                    title="Allocation vs target",
                    chart_type="snapshot_compare",
                    summary="Portfolio charts stay visible, but no holdings truth exists yet.",
                    what_to_notice="Upload holdings before treating this section as actionable.",
                    degraded_state="no_series_available",
                ),
                degraded_chart_panel(
                    panel_id="portfolio_drift_degraded",
                    title="Sleeve drift summary",
                    chart_type="snapshot_compare",
                    summary="Sleeve drift cannot be charted until holdings truth exists.",
                    what_to_notice="Preserve the chart shell and render this typed degraded state instead of dropping it.",
                    degraded_state="no_series_available",
                ),
            ],
            "work_items": [],
            "holdings": [],
            "blueprint_consequence": None,
            "daily_brief_consequence": None,
            "portfolio_source_state": portfolio_source_state,
            "active_upload": active_upload,
            "upload_history": portfolio_status.get("upload_history"),
            "mapping_summary": portfolio_status.get("mapping_summary"),
            "unresolved_mapping_rows": portfolio_status.get("unresolved_mapping_rows"),
            "account_summary": portfolio_status.get("account_summary"),
            "base_currency": portfolio_status.get("base_currency"),
            "mapping_overrides": portfolio_status.get("mapping_overrides"),
            "forecast_watchlist": [],
        }
        return apply_overlay(base_contract, holdings=None)

    rows = _drift_rows(portfolio)
    significant_rows = [row for row in rows if row["status"] != "on_target"]
    pressures = [_pressure_for_row(portfolio, row)[0] for row in significant_rows]
    mandate_state, action_posture = _mandate_summary(portfolio, rows)
    forecast_watchlist = _forecast_watchlist(portfolio, rows)

    portfolio_source_state = dict(portfolio_status.get("portfolio_source_state") or {})
    unresolved_mapping_rows = list(portfolio_status.get("unresolved_mapping_rows") or [])
    mapping_summary = dict(portfolio_status.get("mapping_summary") or {})
    stale_price_count = int(mapping_summary.get("stale_price_count") or 0)
    upload_section_state = (
        ready_section()
        if portfolio_source_state.get("state") == "ready"
        else degraded_section(
            str((portfolio_source_state.get("reason_codes") or ["degraded"])[0]),
            str(portfolio_source_state.get("summary") or "Upload and sync state is degraded."),
        )
        if portfolio_status.get("active_upload")
        else empty_section("no_holdings", "No upload and sync state is available yet.")
    )
    mapping_section_state = (
        ready_section()
        if not unresolved_mapping_rows
        else degraded_section("unresolved_mappings", "Portfolio still has unresolved mapping or pricing issues.")
    )
    allocation_chart_rows = [
        {
            "label": str(row["sleeve_name"]),
            "current_pct": float(row["current_pct"] or 0.0),
            "target_pct": float(row["target_pct"]),
            "drift_pct": float(row["drift_pct"] or 0.0),
            "status": str(row["status"]),
            "note": (
                f"{row['sleeve_name']} is {abs(float(row['drift_pct'] or 0.0)):.2f} points from the strategic target anchor. "
                f"Preferred range: {row['range_label']}."
            ),
        }
        for row in rows
    ]
    allocation_chart_panels = [
        snapshot_comparison_panel(
            panel_id="portfolio_allocation_vs_target",
            title="Allocation vs target",
            rows=allocation_chart_rows,
            primary_label="Current allocation",
            comparison_label="Target allocation",
            primary_key="current_pct",
            comparison_key="target_pct",
            summary="Current sleeve allocations are compared directly against target weights from the live portfolio contract.",
            what_to_notice="Look for sleeves sitting clearly away from target before escalating portfolio action.",
            threshold_values=[
                ("Review band", _REVIEW_DRIFT_THRESHOLD, "review_band", "Review when a sleeve drifts beyond the review band."),
                ("Off-target band", _OFF_TARGET_DRIFT_THRESHOLD, "off_target_band", "Treat sleeves outside this band as active drift issues."),
            ],
        ),
        snapshot_comparison_panel(
            panel_id="portfolio_sleeve_drift",
            title="Sleeve drift summary",
            rows=allocation_chart_rows,
            primary_label="Drift vs target",
            comparison_label=None,
            primary_key="drift_pct",
            summary="Sleeve drift is shown directly so the portfolio posture can stay consequence-first.",
            what_to_notice="Positive or negative drift outside the review band should remain in the work queue until it normalizes.",
            threshold_values=[
                ("Review band", _REVIEW_DRIFT_THRESHOLD, "review_band", "Review when drift moves beyond this band."),
                ("Off-target band", _OFF_TARGET_DRIFT_THRESHOLD, "off_target_band", "Actively treat sleeves outside this band as off target."),
                ("Negative review band", -_REVIEW_DRIFT_THRESHOLD, "review_band", "Review when negative drift moves beyond this band."),
                ("Negative off-target band", -_OFF_TARGET_DRIFT_THRESHOLD, "off_target_band", "Treat sleeves below this band as off target."),
            ],
        ),
    ]

    base_contract = {
        "contract_version": _CONTRACT_VERSION,
        "surface_id": _SURFACE_ID,
        "generated_at": utc_now_iso(),
        "freshness_state": "degraded_monitoring_mode" if stale_price_count else "stored_valid_context",
        "surface_state": surface_state(
            str(portfolio_source_state.get("state") or "ready"),
            reason_codes=list(portfolio_source_state.get("reason_codes") or []),
            summary=str(
                portfolio_source_state.get("summary")
                or "Portfolio surface is hydrated from holdings truth, drift checks, and consequence summaries."
            ),
        ),
        "section_states": {
            "summary": ready_section(),
            "allocation": ready_section() if rows else degraded_section("no_drift_rows", "No sleeve drift rows were emitted."),
            "holdings": ready_section() if portfolio.holdings else empty_section("no_holdings", "No holdings were emitted."),
            "work_queue": ready_section() if significant_rows else empty_section("no_work_items", "No active work items are open."),
            "upload_sync": upload_section_state,
            "mapping": mapping_section_state,
        },
        "account_id": account_id,
        "mandate_state": mandate_state,
        "what_matters_now": _what_matters_now(rows, pressures),
        "action_posture": action_posture,
        "sleeve_drift_summary": rows,
        "allocation_chart_panels": allocation_chart_panels,
        "work_items": _work_items(portfolio, significant_rows, rows),
        "holdings": _holdings_rows(portfolio, rows),
        "blueprint_consequence": _blueprint_consequence(rows),
        "daily_brief_consequence": _daily_brief_consequence(portfolio),
        "portfolio_source_state": portfolio_source_state,
        "active_upload": portfolio_status.get("active_upload"),
        "upload_history": portfolio_status.get("upload_history"),
        "mapping_summary": mapping_summary,
        "unresolved_mapping_rows": unresolved_mapping_rows,
        "account_summary": portfolio_status.get("account_summary"),
        "base_currency": portfolio_status.get("base_currency") or portfolio.base_currency,
        "mapping_overrides": portfolio_status.get("mapping_overrides"),
        "forecast_watchlist": forecast_watchlist,
    }
    record_change(
        event_type="rebuild",
        surface_id="portfolio",
        summary="Portfolio contract rebuilt.",
        change_trigger="portfolio_refresh",
        reason_summary=base_contract["what_matters_now"],
        current_state=mandate_state,
        implication_summary=base_contract["what_matters_now"],
        portfolio_consequence=base_contract["blueprint_consequence"] or base_contract["daily_brief_consequence"],
        next_action=action_posture,
        report_tab="investment_case",
        impact_level="medium" if significant_rows else "low",
        requires_review=bool(significant_rows),
    )
    return apply_overlay(base_contract, holdings=holdings)
