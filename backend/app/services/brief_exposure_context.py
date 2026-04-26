from __future__ import annotations

from typing import Any


def _float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def build_brief_exposure_context(exposure_snapshot: dict[str, Any] | None) -> dict[str, Any]:
    snapshot = exposure_snapshot or {}
    sleeve_rows = list(snapshot.get("sleeve_concentration") or [])
    currency_rows = list(snapshot.get("currency_exposure") or [])
    top_positions = list(snapshot.get("top_positions") or [])

    sleeve_weight_map = {str(item.get("sleeve") or ""): _float(item.get("weight")) for item in sleeve_rows}
    currency_weight_map = {str(item.get("currency") or "").upper(): _float(item.get("weight")) for item in currency_rows}
    base_currency = str(snapshot.get("base_currency") or snapshot.get("summary", {}).get("base_currency") or "SGD").upper()

    sleeve_positions: dict[str, list[dict[str, Any]]] = {}
    for position in top_positions:
        sleeve = str(position.get("sleeve") or "")
        if not sleeve:
            continue
        sleeve_positions.setdefault(sleeve, []).append(position)

    theme_weights = {
        "china": sleeve_weight_map.get("china_satellite", 0.0),
        "em": sleeve_weight_map.get("emerging_markets", 0.0),
        "em_plus_china": sleeve_weight_map.get("emerging_markets", 0.0) + sleeve_weight_map.get("china_satellite", 0.0),
        "duration": sleeve_weight_map.get("IG_bonds", 0.0),
        "reserve": sleeve_weight_map.get("cash_bills", 0.0),
        "duration_plus_reserve": sleeve_weight_map.get("IG_bonds", 0.0) + sleeve_weight_map.get("cash_bills", 0.0),
        "credit": sleeve_weight_map.get("IG_bonds", 0.0) + sleeve_weight_map.get("alternatives", 0.0),
        "equity_beta": (
            sleeve_weight_map.get("global_equity_core", 0.0)
            + sleeve_weight_map.get("developed_ex_us_optional", 0.0)
            + sleeve_weight_map.get("emerging_markets", 0.0)
            + sleeve_weight_map.get("china_satellite", 0.0)
        ),
        "developed_ex_us": sleeve_weight_map.get("developed_ex_us_optional", 0.0),
        "protection": sleeve_weight_map.get("convex", 0.0),
        "real_assets": sleeve_weight_map.get("real_assets", 0.0),
        "non_sgd_fx": sum(weight for currency, weight in currency_weight_map.items() if currency not in {"", "SGD"}),
        "non_base_fx": sum(weight for currency, weight in currency_weight_map.items() if currency not in {"", base_currency}),
        "cash_like": sleeve_weight_map.get("cash_bills", 0.0),
        "bond_like": sleeve_weight_map.get("IG_bonds", 0.0),
        "risk_assets": (
            sleeve_weight_map.get("global_equity_core", 0.0)
            + sleeve_weight_map.get("developed_ex_us_optional", 0.0)
            + sleeve_weight_map.get("emerging_markets", 0.0)
            + sleeve_weight_map.get("china_satellite", 0.0)
            + sleeve_weight_map.get("real_assets", 0.0)
            + sleeve_weight_map.get("alternatives", 0.0)
        ),
    }

    return {
        "summary": dict(snapshot.get("summary") or {}),
        "base_currency": base_currency,
        "sleeve_weight_map": sleeve_weight_map,
        "currency_weight_map": currency_weight_map,
        "sleeve_positions": sleeve_positions,
        "top_positions": top_positions,
        "theme_weights": theme_weights,
    }


def cumulative_theme_exposure(card: dict[str, Any], exposure_context: dict[str, Any] | None) -> tuple[bool, str | None, float | None]:
    context = exposure_context or {}
    sleeve_weight_map = dict(context.get("sleeve_weight_map") or {})
    if not sleeve_weight_map:
        return False, "Exposure snapshot is unavailable, so cumulative theme exposure cannot be inferred.", None
    theme_weights = dict(context.get("theme_weights") or {})
    family = str(card.get("signal_family") or "").lower()
    implication = dict(card.get("portfolio_implication") or {})
    primary = [str(item) for item in list(implication.get("primary_affected_sleeves") or []) if str(item)]
    secondary = [str(item) for item in list(implication.get("secondary_affected_sleeves") or []) if str(item)]

    if family == "em and china context":
        return True, None, round(theme_weights.get("em_plus_china", 0.0) * 100.0, 2)
    if family == "rates and inflation":
        weight = theme_weights.get("duration_plus_reserve", 0.0)
        if "developed_ex_us_optional" in primary or "developed_ex_us_optional" in secondary:
            weight += theme_weights.get("developed_ex_us", 0.0)
        return True, None, round(weight * 100.0, 2)
    if family == "credit and liquidity":
        return True, None, round(theme_weights.get("credit", 0.0) * 100.0, 2)
    if family == "volatility and stress":
        weight = theme_weights.get("equity_beta", 0.0) + theme_weights.get("protection", 0.0) + theme_weights.get("reserve", 0.0)
        return True, None, round(weight * 100.0, 2)
    if family == "equity breadth and risk appetite":
        return True, None, round((theme_weights.get("equity_beta", 0.0) + theme_weights.get("developed_ex_us", 0.0)) * 100.0, 2)
    if family == "fx and singapore context":
        weight = theme_weights.get("non_base_fx", theme_weights.get("non_sgd_fx", 0.0))
        return True, None, round(weight * 100.0, 2)
    if family == "cross asset summary":
        sleeves = primary + [item for item in secondary if item not in primary]
        if sleeves:
            total = sum(_float(sleeve_weight_map.get(sleeve)) for sleeve in sleeves)
            return True, None, round(total * 100.0, 2)
        multi_theme = (
            theme_weights.get("duration_plus_reserve", 0.0)
            + theme_weights.get("credit", 0.0)
            + theme_weights.get("equity_beta", 0.0)
            + theme_weights.get("protection", 0.0)
        )
        return True, None, round(min(multi_theme, 1.0) * 100.0, 2)
    if primary or secondary:
        sleeves = primary + [item for item in secondary if item not in primary]
        total = sum(_float(sleeve_weight_map.get(sleeve)) for sleeve in sleeves)
        return True, None, round(total * 100.0, 2)
    return False, "No cumulative theme exposure rule is defined for this signal family.", None


def implementation_context_for_card(
    card: dict[str, Any],
    grounding: dict[str, Any] | None,
    exposure_context: dict[str, Any] | None,
) -> str | None:
    context = exposure_context or {}
    theme_weights = dict(context.get("theme_weights") or {})
    base_currency = str(context.get("base_currency") or "SGD").upper()
    implication = dict(card.get("portfolio_implication") or {})
    primary = [str(item) for item in list(implication.get("primary_affected_sleeves") or []) if str(item)]
    secondary = [str(item) for item in list(implication.get("secondary_affected_sleeves") or []) if str(item)]
    sleeves = primary + [item for item in secondary if item not in primary]
    family = str(card.get("signal_family") or "").lower()
    consequence = str(card.get("consequence_type") or "").lower()

    non_sgd = theme_weights.get("non_sgd_fx", 0.0)
    if family == "fx and singapore context" and non_sgd > 0:
        return (
            f"Implementation context: roughly {round(theme_weights.get('non_base_fx', non_sgd) * 100.0, 1)}% of tracked exposure is non-{base_currency}, "
            f"so local-currency translation changes allocator experience and the {base_currency} reserve-versus-deployment hurdle more than the underlying asset case."
        )
    if family == "rates and inflation" and any(sleeve in {"IG_bonds", "cash_bills"} for sleeve in sleeves):
        return (
            "Implementation context: reserve and duration sleeves now compete more directly on local hurdle quality, "
            "so execution quality and term exposure matter more than simple headline yield moves."
        )
    if family == "credit and liquidity" or consequence in {"implementation pressure", "liquidity pressure"}:
        return (
            "Implementation context: current conditions raise the value of resilient wrappers, cleaner trading lines, and lower-friction vehicles more than usual."
        )
    if family == "em and china context":
        em_weight = theme_weights.get("em_plus_china", 0.0)
        return (
            f"Implementation context: EM and China together account for roughly {round(em_weight * 100.0, 1)}% of tracked sleeve exposure, "
            "so wrapper quality, concentration discipline, and benchmark fit matter more than fine-grained relative-return differences."
        )
    if family == "cross asset summary" and sleeves:
        return (
            "Implementation context: several sleeves are being affected at once, so portfolio-level pacing and implementation quality matter more than isolated instrument selection."
        )
    if str((grounding or {}).get("grounding_mode") or "") in {"target_proxy", "macro_only"} and sleeves:
        return (
            "Implementation context: this remains a sleeve-level inference rather than a confirmed holding-level impact, so wrapper, tax, and execution implications should be treated as review inputs rather than portfolio facts."
        )
    return None
