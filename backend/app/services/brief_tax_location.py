from __future__ import annotations

from typing import Any


DEFAULT_ACCOUNT_GUIDANCE = {
    "global_equity_core": "Prefer Singapore-friendly or IE UCITS wrappers in taxable accounts when feasible.",
    "emerging_markets": "Treat as taxable-account capable, but review withholding drag and concentration jointly.",
    "china_satellite": "Use only where policy allows a capped satellite. Keep wrapper and trading friction under review.",
    "ig_bonds": "Often more suitable for reserve or lower-turnover accounts due to income sensitivity.",
    "cash_bills": "Operational liquidity sleeve; keep in the account type with the strongest funding flexibility.",
    "real_assets": "Review wrapper structure and withholding mechanics before locating in taxable accounts.",
    "alternatives": "Place where operational complexity and reporting burden are acceptable.",
    "convex": "Keep in accounts that can absorb premium decay and operational review cadence.",
}


def build_tax_location_guidance(blueprint_payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sleeve in list(blueprint_payload.get("sleeves") or []):
        sleeve_key = str(sleeve.get("sleeve_key") or "")
        best_candidate = next(iter(list(sleeve.get("candidates") or [])), {})
        rows.append(
            {
                "sleeve_key": sleeve_key,
                "sleeve_name": str(sleeve.get("name") or sleeve_key.replace("_", " ").title()),
                "preferred_account_type": "taxable_guidance_only",
                "guidance": DEFAULT_ACCOUNT_GUIDANCE.get(
                    sleeve_key,
                    "No mature account typing is available yet. Treat this as policy guidance only.",
                ),
                "tax_efficiency_score": best_candidate.get("tax_efficiency_score") or best_candidate.get("tax_score"),
                "notes": "Guidance is policy-level and should be adapted if account taxonomy matures.",
            }
        )
    return rows
