from __future__ import annotations

from typing import Any


def build_domain_availability(
    *,
    portfolio_summary: dict[str, Any] | None,
    blueprint_summary: dict[str, Any] | None,
    brief_summary: dict[str, Any] | None,
) -> dict[str, Any]:
    portfolio_status = "healthy"
    blueprint_status = "healthy"
    brief_status = "healthy"
    issues: list[str] = []

    if not portfolio_summary or not portfolio_summary.get("has_portfolio"):
        portfolio_status = "portfolio_unavailable"
        issues.append("No portfolio upload snapshot is available.")
    else:
        if int(portfolio_summary.get("stale_price_count") or 0) > 0:
            portfolio_status = "degraded_stale_prices"
            issues.append("Latest portfolio valuation includes stale or fallback prices.")
        if int(portfolio_summary.get("mapping_issue_count") or 0) > 0:
            portfolio_status = "degraded_mapping_incomplete"
            issues.append("Latest portfolio snapshot has unmapped or low-confidence holdings.")

    if not blueprint_summary or not blueprint_summary.get("blueprint"):
        blueprint_status = "blueprint_unavailable"
        issues.append("Blueprint target design is unavailable.")

    if not brief_summary or not brief_summary.get("run"):
        brief_status = "brief_unavailable"
        issues.append("Daily brief is unavailable.")

    overall = "healthy"
    if any(status == "portfolio_unavailable" for status in (portfolio_status, blueprint_status, brief_status)):
        overall = "partial"
    if any(status.startswith("degraded") for status in (portfolio_status, blueprint_status, brief_status)):
        overall = "degraded"
    if portfolio_status == "portfolio_unavailable" and blueprint_status == "blueprint_unavailable" and brief_status == "brief_unavailable":
        overall = "partial"

    return {
        "overall": overall,
        "portfolio": portfolio_status,
        "blueprint": blueprint_status,
        "daily_brief": brief_status,
        "issues": issues,
    }
