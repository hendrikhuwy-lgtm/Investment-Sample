from __future__ import annotations

from typing import Any


_CONVEXITY_WEIGHTS: dict[str, int] = {
    "inflation": 18,
    "policy": 17,
    "duration": 16,
    "credit": 15,
    "dollar_fx": 15,
    "energy": 14,
    "volatility": 14,
    "real_assets": 12,
    "growth": 10,
    "liquidity": 10,
    "market": 8,
}


def build_contingent_drivers(drivers: list[dict[str, Any]], *, limit: int = 5) -> list[dict[str, Any]]:
    ranked = sorted(drivers, key=_contingent_score, reverse=True)
    contingent: list[dict[str, Any]] = []
    selected_drivers: list[dict[str, Any]] = []
    grouped_support: dict[str, list[dict[str, Any]]] = {}
    minimum_target = min(limit, 3)
    used_groups: set[str] = set()
    used_families: set[str] = set()
    selected_ids: set[str] = set()

    def _try_add(pool: list[dict[str, Any]], *, strict: bool) -> None:
        candidates = []
        for driver in pool:
            if len(contingent) >= limit:
                break
            if str(driver.get("actionability_class") or "") == "evidence_only":
                continue
            if strict and not _strict_trigger_candidate(driver):
                continue
            if (not strict) and not _coverage_fill_candidate(driver):
                continue
            driver_id = f"contingent_{driver['signal_id']}"
            if driver_id in selected_ids:
                continue
            folded_into = _folded_driver_id(driver, selected_drivers)
            if folded_into:
                selected_ids.add(driver_id)
                grouped_support.setdefault(folded_into, []).append(driver)
                continue
            group = str(driver.get("duplication_group") or "")
            if group and group in used_groups and _is_same_consequence_family(driver, selected_drivers):
                continue
            if _is_redundant_with_selected(driver, selected_drivers):
                continue
            family = _driver_family(driver)
            diversity_bonus = 8 if family not in used_families else 0
            candidates.append((diversity_bonus, _contingent_score(driver), driver))
        candidates.sort(key=lambda item: (item[0], item[1]), reverse=True)
        for _, _, driver in candidates:
            if len(contingent) >= limit:
                break
            folded_into = _folded_driver_id(driver, selected_drivers)
            if folded_into:
                selected_ids.add(f"contingent_{driver['signal_id']}")
                grouped_support.setdefault(folded_into, []).append(driver)
                continue
            card = _build_contingent_card(driver, grouped_support.get(str(driver.get("signal_id") or ""), []))
            contingent.append(card)
            selected_ids.add(card["driver_id"])
            group = str(driver.get("duplication_group") or "")
            if group:
                used_groups.add(group)
            used_families.add(_driver_family(driver))
            selected_drivers.append(driver)

    _try_add(ranked, strict=True)
    if len(contingent) < minimum_target:
        _try_add(ranked, strict=False)
    for card in contingent:
        signal_id = str(card.get("driver_id") or "").removeprefix("contingent_")
        card["supporting_lines"] = _supporting_lines(grouped_support.get(signal_id, []))
    return contingent


def _build_contingent_card(driver: dict[str, Any], supporting_drivers: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    current_status = _current_status(driver)
    affected_sleeves = [
        str(item).replace("sleeve_", "").replace("_", " ").title()
        for item in list(driver.get("affected_sleeves") or [])
        if str(item).strip()
    ]
    return {
        "driver_id": f"contingent_{driver['signal_id']}",
        "label": str(driver.get("decision_title") or driver.get("label") or "Signal"),
        "trigger_title": _trigger_title(driver),
        "effect_type": driver.get("effect_type"),
        "why_it_matters_now": _why_now(driver),
        "what_changes_if_confirmed": _what_changes_if_confirmed(driver),
        "trigger_condition": _trigger_condition(driver),
        "what_to_watch_next": _what_to_watch_next(driver),
        "current_status": current_status,
        "affected_sleeves": affected_sleeves,
        "support_label": _support_label(driver),
        "supporting_lines": _supporting_lines(supporting_drivers or []),
        "near_term_trigger": str(driver.get("near_term_trigger") or driver.get("confirms") or ""),
        "thesis_trigger": str(driver.get("thesis_trigger") or driver.get("implication") or ""),
        "break_condition": str(driver.get("breaks") or ""),
        "affected_sleeve": _affected_sleeve(driver),
        "portfolio_consequence": _portfolio_consequence(driver),
        "next_action": str(driver.get("next_action") or "Monitor"),
        "confidence_class": driver.get("confidence_class"),
        "sufficiency_state": driver.get("sufficiency_state"),
        "source_kind": driver.get("source_kind"),
        "path_risk_note": driver.get("path_risk_note"),
        "forecast_support": driver.get("forecast_support"),
    }


def _contingent_score(driver: dict[str, Any]) -> float:
    bucket = str(driver.get("primary_effect_bucket") or "market")
    magnitude = str(driver.get("magnitude") or "minor")
    support = str(driver.get("signal_support_class") or "").lower()
    next_action = str(driver.get("next_action") or "").lower()
    forecast_support = dict(driver.get("forecast_support") or {})
    trigger_pressure = float(forecast_support.get("trigger_pressure") or 0.0)
    persistence_score = float(forecast_support.get("persistence_score") or 0.0)
    cross_asset_confirmation_score = float(forecast_support.get("cross_asset_confirmation_score") or 0.0)
    escalation_flag = bool(forecast_support.get("escalation_flag"))
    base = float(_CONVEXITY_WEIGHTS.get(bucket, 8))
    base += 6 if magnitude == "significant" else 4 if magnitude == "moderate" else 1
    base += 4 if support in {"strong", "tight_interval_support"} else 2 if support else 0
    if "review" in next_action:
        base -= 4
    if not str(driver.get("breaks") or "").strip():
        base -= 2
    if not str(driver.get("near_term_trigger") or "").strip():
        base -= 2
    base += 4 if "high" in str(driver.get("path_risk_note") or "").lower() else 0
    base += 2 if str(driver.get("actionability_class") or "") == "contextual_monitor" else 0
    base += trigger_pressure * 6.0
    base += persistence_score * 4.0
    base += cross_asset_confirmation_score * 3.0
    if escalation_flag:
        base += 4.0
    return base


def _affected_sleeve(driver: dict[str, Any]) -> str | None:
    sleeves = list(driver.get("affected_sleeves") or [])
    if not sleeves:
        return None
    return str(sleeves[0]).replace("sleeve_", "").replace("_", " ").title()


def _portfolio_consequence(driver: dict[str, Any]) -> str:
    bucket = str(driver.get("primary_effect_bucket") or "market").strip().lower()
    sleeves = _humanized_sleeves(driver)
    sleeve_text = _joined_items(sleeves) or "the relevant sleeve mix"
    implementation_text = _implementation_direction(bucket=bucket)
    candidates = [str(item).strip() for item in list(driver.get("affected_candidates") or []) if str(item).strip()]
    if bucket == "duration":
        sleeve_sentence = f"For {sleeve_text}, keep bond posture patient and delay easy duration adds if the trigger confirms."
    elif bucket == "inflation":
        sleeve_sentence = f"For {sleeve_text}, keep inflation hedges active and leave easy duration adds secondary if the trigger confirms."
    elif bucket in {"credit", "liquidity"}:
        sleeve_sentence = f"For {sleeve_text}, keep the risk budget tighter and carry more selective if the trigger confirms."
    elif bucket in {"growth", "market", "volatility"}:
        sleeve_sentence = f"For {sleeve_text}, keep risk adds conditional on broader confirmation rather than forcing them early."
    elif bucket == "dollar_fx":
        sleeve_sentence = f"For {sleeve_text}, keep cross-border risk sizing selective and leave broad global adds patient."
    elif bucket in {"energy", "real_assets"}:
        sleeve_sentence = f"For {sleeve_text}, keep real-assets hedges relevant and leave easy duration adds secondary."
    else:
        sleeve_sentence = f"For {sleeve_text}, keep the sleeve posture conditional on confirmation rather than forcing a quick change."
    parts = [sleeve_sentence, implementation_text]
    if candidates:
        candidate_text = ", ".join(candidates[:4])
        parts.append(f"{candidate_text} stay the most relevant implementation set if the trigger confirms.")
    return " ".join(part for part in parts if part)


def _implementation_direction(*, bucket: str) -> str:
    if bucket == "duration":
        return "Favor higher-quality ballast and patient duration timing over aggressive extension."
    if bucket == "inflation":
        return "Favor inflation-sensitive hedges over treating easier duration adds as the first response."
    if bucket in {"credit", "liquidity"}:
        return "Favor higher-quality carry and funding resilience over lower-quality spread adds."
    if bucket in {"growth", "market", "volatility"}:
        return "Favor broad liquid equity exposure only after breadth improves, not defensive cash substitution by default."
    if bucket == "dollar_fx":
        return "Favor liquid global exposure and disciplined sizing over forcing unhedged cross-border risk."
    if bucket in {"energy", "real_assets"}:
        return "Favor inflation-sensitive hedges over treating the move as a stand-alone commodity trade."
    return "Favor sleeve timing first and only rotate instruments if the confirming evidence broadens."


def _humanized_sleeves(driver: dict[str, Any]) -> list[str]:
    return [
        str(item).replace("sleeve_", "").replace("_", " ").title()
        for item in list(driver.get("affected_sleeves") or [])
        if str(item).strip()
    ]


def _joined_items(items: list[str]) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return f"{', '.join(items[:-1])}, and {items[-1]}"


def _driver_family(driver: dict[str, Any]) -> str:
    source_class = str((driver.get("source_context") or {}).get("source_class") or "").lower()
    bucket = str(driver.get("primary_effect_bucket") or "market")
    if source_class in {"policy_event", "geopolitical_news"} or bucket == "policy":
        return "policy_global_news"
    if bucket in {"duration", "inflation"}:
        return "rates"
    if bucket in {"credit", "liquidity"}:
        return "credit"
    if bucket in {"growth", "market", "volatility"}:
        return "equity"
    if bucket == "dollar_fx":
        return "fx"
    if bucket in {"energy", "real_assets"}:
        return "commodity"
    return bucket


def _strict_trigger_candidate(driver: dict[str, Any]) -> bool:
    forecast_support = dict(driver.get("forecast_support") or {})
    trigger_pressure = float(forecast_support.get("trigger_pressure") or 0.0)
    confirmation = float(forecast_support.get("cross_asset_confirmation_score") or 0.0)
    scenario_support_strength = str(forecast_support.get("scenario_support_strength") or "").lower()
    threshold_state = _threshold_state(driver)
    novelty_class = str(driver.get("novelty_class") or "").lower()
    if threshold_state == "breached":
        return True
    if trigger_pressure >= 0.62:
        return True
    if trigger_pressure >= 0.5 and confirmation >= 0.42:
        return True
    if scenario_support_strength in {"strong", "moderate"} and novelty_class in {"threshold_break", "reactivated", "reversal"}:
        return True
    return False


def _coverage_fill_candidate(driver: dict[str, Any]) -> bool:
    forecast_support = dict(driver.get("forecast_support") or {})
    trigger_pressure = float(forecast_support.get("trigger_pressure") or 0.0)
    confirmation = float(forecast_support.get("cross_asset_confirmation_score") or 0.0)
    scenario_support_strength = str(forecast_support.get("scenario_support_strength") or "").lower()
    threshold_state = _threshold_state(driver)
    novelty_class = str(driver.get("novelty_class") or "").lower()
    prominence = float(driver.get("prominence_score") or 0.0) or _contingent_score(driver)
    if prominence < 22.0:
        return False
    if threshold_state in {"watch", "breached"}:
        return True
    if trigger_pressure >= 0.4 and confirmation >= 0.28:
        return True
    if trigger_pressure >= 0.34 and scenario_support_strength in {"strong", "moderate"}:
        return True
    return novelty_class in {"threshold_break", "reactivated"} and confirmation >= 0.26


def _threshold_state(driver: dict[str, Any]) -> str:
    return str(
        ((driver.get("monitoring_condition") or {}).get("trigger_support") or {}).get("threshold_state")
        or driver.get("threshold_state")
        or ""
    ).lower()


def _is_same_consequence_family(driver: dict[str, Any], selected_cards: list[dict[str, Any]]) -> bool:
    family = _driver_family(driver)
    label = str(driver.get("label") or "")
    for item in selected_cards:
        if _driver_family(item) != family:
            continue
        if _is_repetitive(label, str(item.get("label") or "")):
            return True
    return False


def _is_redundant_with_selected(driver: dict[str, Any], selected_cards: list[dict[str, Any]]) -> bool:
    group = str(driver.get("duplication_group") or "")
    label = str(driver.get("label") or "")
    sleeves = {str(item).strip().lower() for item in list(driver.get("affected_sleeves") or []) if str(item).strip()}
    for item in selected_cards:
        other_group = str(item.get("duplication_group") or "")
        if group and group == other_group:
            other_sleeves = {str(member).strip().lower() for member in list(item.get("affected_sleeves") or []) if str(member).strip()}
            if not sleeves or not other_sleeves or sleeves == other_sleeves:
                return True
        if _is_repetitive(label, str(item.get("label") or "")):
            return True
    return False


def _folded_driver_id(driver: dict[str, Any], selected_cards: list[dict[str, Any]]) -> str | None:
    for item in selected_cards:
        if _same_trigger_cluster(driver, item):
            return str(item.get("signal_id") or "")
    return None


def _same_trigger_cluster(left: dict[str, Any], right: dict[str, Any]) -> bool:
    if _driver_family(left) != _driver_family(right):
        return False
    left_sleeves = {str(item).strip().lower() for item in list(left.get("affected_sleeves") or []) if str(item).strip()}
    right_sleeves = {str(item).strip().lower() for item in list(right.get("affected_sleeves") or []) if str(item).strip()}
    if left_sleeves and right_sleeves and not (left_sleeves & right_sleeves):
        return False
    if _token_overlap(_what_changes_if_confirmed(left), _what_changes_if_confirmed(right)) < 0.62:
        return False
    left_impl = _implementation_signature(left)
    right_impl = _implementation_signature(right)
    if left_impl and right_impl and _token_overlap(left_impl, right_impl) < 0.4:
        return False
    return True


def _supporting_lines(drivers: list[dict[str, Any]]) -> list[str]:
    lines: list[str] = []
    seen: set[str] = set()
    for driver in drivers:
        text = _supporting_line(driver)
        _, _, body = text.partition(":")
        normalized = (body or text).strip().lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        lines.append(text)
        if len(lines) >= 2:
            break
    return lines


def _why_now(driver: dict[str, Any]) -> str:
    bucket = str(driver.get("primary_effect_bucket") or "market").strip().lower()
    if bucket == "duration":
        return "Higher short term yields make it less attractive to buy longer bonds right now."
    if bucket == "inflation":
        return "Sticky inflation keeps inflation protection useful and makes quick bond relief less likely."
    if bucket in {"credit", "liquidity"}:
        return "Wider credit spreads make investors more cautious and keep riskier credit less attractive."
    if bucket == "dollar_fx":
        return "A stronger dollar makes it harder for global markets to keep rallying."
    if bucket in {"growth", "market", "volatility"}:
        return "This line shows whether investors are still willing to take more risk."
    if bucket == "energy":
        return "A stronger oil move can keep inflation worries alive."
    if bucket == "real_assets":
        return "This line shows whether real assets are still helping as protection."
    if bucket == "policy":
        return "This line shows whether the Fed is still keeping conditions tight."
    subtitle = str(driver.get("interpretation_subtitle") or "").strip()
    why = str(driver.get("why_it_matters_short_term") or driver.get("why_it_matters_economically") or driver.get("why_it_matters") or "").strip()
    if subtitle:
        return _compact_text(subtitle, max_words=20, max_chars=160) or subtitle
    if why:
        return _compact_text(why, max_words=20, max_chars=160) or why
    return "This line could still change today’s brief if it starts confirming."


def _what_changes_if_confirmed(driver: dict[str, Any]) -> str:
    bucket = str(driver.get("primary_effect_bucket") or "market").strip().lower()
    sleeves = _joined_items(_humanized_sleeves(driver)) or "the relevant sleeves"
    if bucket == "duration":
        return f"If this holds, safer bond holdings in {sleeves} stay more attractive than buying longer bonds too early."
    if bucket == "inflation":
        return f"If this holds, inflation protection in {sleeves} stays more useful than betting on easier conditions for bonds."
    if bucket in {"credit", "liquidity"}:
        return f"If this holds, safer credit in {sleeves} stays more attractive than reaching for extra yield."
    if bucket in {"growth", "market", "volatility"}:
        return f"If this holds, broad risk adds in {sleeves} are less likely and selective exposure becomes more likely."
    if bucket == "dollar_fx":
        return f"If this holds, global exposure in {sleeves} is more likely to stay selective than broad international buying."
    if bucket in {"energy", "real_assets"}:
        return f"If this holds, real assets in {sleeves} stay more useful than betting on quick bond relief."
    if bucket == "policy":
        return f"If this holds, markets in {sleeves} are more likely to wait for relief than add risk quickly."
    return f"If this holds, positions in {sleeves} are more likely to stay selective than shift quickly."


def _trigger_condition(driver: dict[str, Any]) -> str:
    base = str(driver.get("near_term_trigger") or driver.get("thesis_trigger") or driver.get("confirms") or "").strip()
    forecast_support = dict(driver.get("forecast_support") or {})
    trigger_pressure = float(forecast_support.get("trigger_pressure") or 0.0)
    cross_asset_confirmation_score = float(forecast_support.get("cross_asset_confirmation_score") or 0.0)
    threshold_state = _threshold_state(driver)
    if threshold_state == "breached" and cross_asset_confirmation_score < 0.4:
        suffix = "The threshold is through the line, but broader confirmation is still light."
    elif trigger_pressure >= 0.68:
        suffix = "The watched threshold is now under rising pressure."
    elif trigger_pressure <= 0.33:
        suffix = "The level matters, but pressure is still limited."
    else:
        suffix = "The watch line is active and needs follow-through."
    return " ".join(part for part in [base, suffix] if part)


def _trigger_title(driver: dict[str, Any]) -> str:
    forecast_support = dict(driver.get("forecast_support") or {})
    trigger_pressure = float(forecast_support.get("trigger_pressure") or 0.0)
    cross_asset_confirmation_score = float(forecast_support.get("cross_asset_confirmation_score") or 0.0)
    threshold_state = _threshold_state(driver)
    near_term_trigger = str(driver.get("near_term_trigger") or "").strip()
    thesis_trigger = str(driver.get("thesis_trigger") or "").strip()
    label = _watch_label(driver)
    if threshold_state == "breached" and cross_asset_confirmation_score < 0.4:
        return f"{label} broke the line but still needs confirmation"
    if threshold_state == "breached":
        return f"{label} broke the line"
    if trigger_pressure >= 0.68:
        return f"{label} is near the line"
    if trigger_pressure <= 0.33 and near_term_trigger:
        return f"{label} is still below the line"
    return label or near_term_trigger or thesis_trigger or "Watch this line"


def _watch_label(driver: dict[str, Any]) -> str:
    raw_label = str(driver.get("label") or "").strip()
    lowered = raw_label.lower()
    bucket = str(driver.get("primary_effect_bucket") or "market").strip().lower()
    source_context = dict(driver.get("source_context") or {})
    source_class = str(source_context.get("source_class") or "").strip().lower()
    if source_class == "geopolitical_news":
        return "Headline risk"
    if source_class == "policy_event":
        return "Policy risk"
    if bucket == "policy":
        return "Rate relief"
    if bucket == "duration":
        if "mortgage" in lowered:
            return "Mortgage pressure"
        if "real yield" in lowered:
            return "Real-yield pressure"
        if any(term in lowered for term in ("fed funds", "2y", "2-year", "2 year", "front end", "short rate")):
            return "Short-term rate pressure"
        return "Bond-yield pressure" if raw_label else "Rate pressure"
    if bucket == "inflation":
        return "Inflation pressure"
    if bucket in {"credit", "liquidity"}:
        return "Credit stress" if bucket == "credit" else "Funding pressure"
    if bucket == "dollar_fx":
        return "Dollar strength"
    if bucket == "energy":
        return "Oil"
    if bucket == "real_assets":
        return "Inflation protection"
    if bucket == "volatility":
        return "Market stress"
    if bucket == "growth":
        return "Equity breadth"
    if bucket == "market":
        return "Risk appetite"
    return raw_label or "this threshold"


def _current_status(driver: dict[str, Any]) -> str:
    threshold_state = _threshold_state(driver)
    forecast_support = dict(driver.get("forecast_support") or {})
    trigger_pressure = float(forecast_support.get("trigger_pressure") or 0.0)
    cross_asset_confirmation_score = float(forecast_support.get("cross_asset_confirmation_score") or 0.0)
    persistence_score = float(forecast_support.get("persistence_score") or 0.0)
    fade_risk = float(forecast_support.get("fade_risk") or 0.0)
    escalation_flag = bool(forecast_support.get("escalation_flag"))
    if threshold_state == "breached":
        if cross_asset_confirmation_score < 0.4:
            return "triggered_but_unconfirmed"
        return "triggered"
    if fade_risk >= 0.66 and fade_risk > persistence_score + 0.08:
        return "fading"
    if trigger_pressure >= 0.48 or threshold_state == "watch" or escalation_flag:
        return "near_trigger"
    return "watching"


def _support_label(driver: dict[str, Any]) -> str:
    forecast_support = dict(driver.get("forecast_support") or {})
    scenario_support_strength = str(forecast_support.get("scenario_support_strength") or "").strip().lower()
    confirmation = float(forecast_support.get("cross_asset_confirmation_score") or 0.0)
    trigger_pressure = float(forecast_support.get("trigger_pressure") or 0.0)
    threshold_state = _threshold_state(driver)
    if threshold_state == "breached" and confirmation < 0.4:
        return "breached but not yet confirmed"
    if trigger_pressure >= 0.66:
        return "rising trigger pressure"
    if confirmation >= 0.66 and scenario_support_strength in {"strong", "moderate"}:
        return "broad confirmation but still bounded"
    if confirmation >= 0.42 and scenario_support_strength in {"strong", "moderate"}:
        return "moderate path support"
    if scenario_support_strength == "strong":
        return "strong path support"
    if scenario_support_strength == "moderate":
        return "moderate path support"
    if str(_current_status(driver)) == "fading":
        return "fading path support"
    return "weak path support"


def _what_to_watch_next(driver: dict[str, Any]) -> str:
    forecast_support = dict(driver.get("forecast_support") or {})
    trigger_pressure = float(forecast_support.get("trigger_pressure") or 0.0)
    confirmation = float(forecast_support.get("cross_asset_confirmation_score") or 0.0)
    threshold_state = _threshold_state(driver)
    bucket = str(driver.get("primary_effect_bucket") or "market").strip().lower()
    if threshold_state == "breached" and confirmation < 0.4:
        return _watch_sentence(
            bucket=bucket,
            confirm="Look for other markets to confirm the break",
            fade="treat it as fading if the move slips back inside the range",
        )
    if bucket == "duration":
        return _watch_sentence(
            bucket=bucket,
            confirm="Look for the move to hold into the next session",
            fade="treat it as fading if yields fall back quickly",
        )
    if bucket == "inflation":
        return _watch_sentence(
            bucket=bucket,
            confirm="Look for confirmation from inflation and bond markets",
            fade="treat it as fading if the move reverses next session",
        )
    if bucket in {"credit", "liquidity"}:
        return _watch_sentence(
            bucket=bucket,
            confirm="Look for credit spreads and stock breadth to confirm",
            fade="treat it as fading if spreads retrace quickly",
        )
    if bucket == "dollar_fx":
        return _watch_sentence(
            bucket=bucket,
            confirm="Look for confirmation from the dollar and global stocks",
            fade="treat it as fading if the move reverses next session",
        )
    if bucket in {"growth", "market", "volatility"}:
        return _watch_sentence(
            bucket=bucket,
            confirm="Look for confirmation from breadth and market stress",
            fade="treat it as fading if the move quickly falls back",
        )
    if bucket == "energy":
        return _watch_sentence(
            bucket=bucket,
            confirm="Look for confirmation from oil and inflation markets",
            fade="treat it as fading if oil gives back the move next session",
        )
    if bucket == "real_assets":
        return _watch_sentence(
            bucket=bucket,
            confirm="Look for confirmation from real yields and the dollar",
            fade="treat it as fading if the move reverses next session",
        )
    if bucket == "policy":
        return _watch_sentence(
            bucket=bucket,
            confirm="Look for confirmation from the next session or release",
            fade="treat it as fading if markets retrace the move quickly",
        )
    if trigger_pressure >= 0.66:
        return _watch_sentence(
            bucket=bucket,
            confirm="Look for a clean break that holds",
            fade="treat it as fading if the move backs away quickly",
        )
    return _watch_sentence(
        bucket=bucket,
        confirm="Look for broader confirmation from related markets",
        fade="treat it as fading if the move does not hold",
    )


def _is_repetitive(left: str, right: str) -> bool:
    if left and right and (right.startswith(left) or left in right):
        return True
    left_tokens = _normalized_tokens(left)
    right_tokens = _normalized_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    overlap = len(left_tokens & right_tokens)
    return overlap / max(len(left_tokens), len(right_tokens)) >= 0.72


def _normalized_tokens(text: str) -> set[str]:
    cleaned = "".join(char.lower() if char.isalnum() else " " for char in str(text or ""))
    stop = {"the", "and", "for", "with", "that", "this", "into", "keep", "level", "until", "still", "current"}
    return {token for token in cleaned.split() if len(token) > 2 and token not in stop}


def _token_overlap(left: str, right: str) -> float:
    left_tokens = _normalized_tokens(left)
    right_tokens = _normalized_tokens(right)
    if not left_tokens or not right_tokens:
        return 0.0
    overlap = len(left_tokens & right_tokens)
    return overlap / max(len(left_tokens), len(right_tokens))


def _implementation_signature(driver: dict[str, Any]) -> str:
    parts = [
        str(driver.get("implementation_sensitivity") or "").strip(),
        " ".join(str(item).strip() for item in list(driver.get("affected_candidates") or []) if str(item).strip()),
        _joined_items(_humanized_sleeves(driver)),
    ]
    return " ".join(part for part in parts if part)


def _compact_text(text: str, *, max_words: int = 18, max_chars: int = 140) -> str | None:
    value = " ".join(str(text or "").strip().split())
    if not value:
        return None
    words = value.split()
    if len(words) > max_words:
        value = " ".join(words[:max_words]).rstrip(" ,.;:") + "."
    if len(value) > max_chars:
        value = value[: max_chars - 1].rstrip(" ,.;:") + "."
    return value


def _watch_clause(text: str) -> str:
    compact = _compact_text(text, max_words=12, max_chars=96)
    if not compact:
        return ""
    value = compact.strip().rstrip(".")
    lowered = value.lower()
    for prefix in (
        "watch whether ",
        "watch if ",
        "watch ",
        "recheck if ",
        "treat the current directional support as broken if ",
        "treat the current read as broken if ",
        "the brief changes if ",
        "the thesis changes if ",
    ):
        if lowered.startswith(prefix):
            value = value[len(prefix):]
            break
    if value:
        value = value[0].lower() + value[1:]
    return value


def _watch_sentence(*, bucket: str, confirm: str, fade: str) -> str:
    confirm_text = _compact_text(confirm, max_words=12, max_chars=96) or confirm
    fade_text = _compact_text(fade, max_words=12, max_chars=96) or fade
    return f"{confirm_text}; {fade_text}."


def _supporting_line(driver: dict[str, Any]) -> str:
    label = str(driver.get("label") or "Related line").strip()
    bucket = str(driver.get("primary_effect_bucket") or "market").strip().lower()
    threshold_state = _threshold_state(driver)
    confirmation = float(dict(driver.get("forecast_support") or {}).get("cross_asset_confirmation_score") or 0.0)
    if threshold_state == "breached" and confirmation < 0.4:
        return f"{label}: more convincing if other markets start to confirm."
    if bucket == "duration":
        return f"{label}: more convincing if yields keep holding the move."
    if bucket == "inflation":
        return f"{label}: more convincing if inflation pressure keeps building."
    if bucket in {"credit", "liquidity"}:
        return f"{label}: more convincing if spreads stay wide."
    if bucket == "dollar_fx":
        return f"{label}: more convincing if the dollar keeps holding the move."
    if bucket in {"growth", "market", "volatility"}:
        return f"{label}: more convincing if breadth keeps improving."
    if bucket == "energy":
        return f"{label}: more convincing if oil keeps holding the move."
    if bucket == "real_assets":
        return f"{label}: more convincing if real assets keep holding up."
    if bucket == "policy":
        return f"{label}: more convincing if the next session keeps pressure on rates."
    return f"{label}: more convincing if the move holds next session."
