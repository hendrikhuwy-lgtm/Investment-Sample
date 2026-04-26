from __future__ import annotations

from typing import Any


def build_fund_selection_table(blueprint_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sleeve in list(blueprint_payload.get("sleeves") or []):
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        sleeve_name = str(sleeve.get("name") or sleeve_key.replace("_", " ").title())
        target = float(dict(sleeve.get("policy_weight_range") or {}).get("target") or 0.0) / 100.0
        for candidate in list(sleeve.get("candidates") or [])[:2]:
            rows.append(
                {
                    "ticker": str(candidate.get("symbol") or ""),
                    "name": str(candidate.get("name") or ""),
                    "sleeve_key": sleeve_key,
                    "sleeve_name": sleeve_name,
                    "target_weight": target,
                    "ter": float(candidate.get("expense_ratio") or 0.0),
                    "aum": candidate.get("aum") or candidate.get("assets_under_management") or "not available",
                    "liquidity_proxy": str(candidate.get("liquidity_proxy") or "not available"),
                    "liquidity_score": candidate.get("liquidity_score"),
                    "singapore_tax_efficiency_score": candidate.get("tax_efficiency_score") or candidate.get("tax_score"),
                    "rationale": str(candidate.get("rationale") or ""),
                    "factsheet_asof": candidate.get("factsheet_asof"),
                    "verification_status": candidate.get("verification_status"),
                }
            )
    return rows
