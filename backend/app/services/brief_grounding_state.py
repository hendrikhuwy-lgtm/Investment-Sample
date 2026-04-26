from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.config import Settings
from app.services.exposure_aggregator import build_exposure_snapshot
from app.services.portfolio_ingest import latest_snapshot_rows, latest_upload_run


def _parse_dt(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        if len(raw) == 10:
            return datetime.fromisoformat(f"{raw}T00:00:00+00:00")
        parsed = datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    except ValueError:
        return None


def evaluate_brief_grounding(conn: Any, settings: Settings) -> dict[str, Any]:
    active_run = latest_upload_run(conn)
    has_active_run = active_run is not None
    run_id = str((active_run or {}).get("run_id") or "")
    holdings_rows = latest_snapshot_rows(conn, run_id=run_id) if run_id else []
    exposures = build_exposure_snapshot(conn, run_id=run_id) if run_id else {}

    uploaded_at = _parse_dt((active_run or {}).get("uploaded_at"))
    holdings_as_of = _parse_dt((active_run or {}).get("holdings_as_of_date"))
    reference_dt = uploaded_at or holdings_as_of
    portfolio_age_hours = None
    if reference_dt is not None:
        portfolio_age_hours = round((datetime.now(UTC) - reference_dt).total_seconds() / 3600.0, 2)

    summary = dict((exposures or {}).get("summary") or {})
    total_value = float(summary.get("total_value") or 0.0)
    has_holdings = bool(holdings_rows)
    has_nonzero_value = total_value > 0
    has_exposure_summary = bool(exposures) and (
        bool((exposures or {}).get("sleeve_concentration")) or has_nonzero_value
    )
    base_currency = None
    for row in holdings_rows:
        candidate = str(row.get("base_currency") or "").strip().upper()
        if candidate:
            base_currency = candidate
            break
    if not base_currency:
        base_currency = "SGD"

    warnings: list[str] = []
    grounding_mode = "macro_only"
    require_portfolio = bool(settings.daily_brief_require_portfolio)
    if not has_active_run:
        warnings.append("No active portfolio snapshot is available.")
        if settings.daily_brief_allow_target_proxy and not require_portfolio:
            grounding_mode = "target_proxy"
    elif has_holdings and has_nonzero_value:
        max_age = float(settings.daily_brief_max_portfolio_age_hours)
        if portfolio_age_hours is not None and portfolio_age_hours > max_age:
            grounding_mode = "stale_holding_grounded"
            warnings.append(
                f"Portfolio snapshot is older than the {int(max_age)} hour grounding threshold."
            )
        else:
            grounding_mode = "live_holding_grounded"
    elif has_exposure_summary:
        grounding_mode = "live_sleeve_grounded"
        warnings.append("Exposure summary is available but direct holdings mapping is limited.")
    elif settings.daily_brief_allow_target_proxy and not require_portfolio:
        grounding_mode = "target_proxy"
        warnings.append("Live portfolio grounding is unavailable; using target-sleeve proxy logic.")
    else:
        grounding_mode = "macro_only"
        if require_portfolio:
            warnings.append("Portfolio grounding is required, but no usable portfolio snapshot was found.")
        else:
            warnings.append("Portfolio grounding is disabled because no usable portfolio snapshot was found.")

    return {
        "has_active_run": has_active_run,
        "run_id": run_id or None,
        "portfolio_as_of": str((active_run or {}).get("holdings_as_of_date") or "") or None,
        "portfolio_age_hours": portfolio_age_hours,
        "has_holdings": has_holdings,
        "has_nonzero_value": has_nonzero_value,
        "has_exposure_summary": has_exposure_summary,
        "grounding_mode": grounding_mode,
        "grounding_warnings": warnings,
        "live_holdings_used": grounding_mode in {"live_holding_grounded", "stale_holding_grounded"},
        "target_proxy_used": grounding_mode in {"target_proxy", "macro_only"},
        "base_currency": base_currency,
    }
