from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


def _clean_text(value: Any) -> str | None:
    raw = str(value or "").strip()
    return raw or None


def _symbol_from_candidate(candidate_id: Any) -> str | None:
    raw = str(candidate_id or "").strip()
    if not raw:
        return None
    token = raw.removeprefix("candidate_instrument_").strip()
    return token.upper() if token else None


def _display_state(value: Any) -> str | None:
    raw = _clean_text(value)
    if not raw:
        return None
    if raw.startswith("candidate_instrument_"):
        return _symbol_from_candidate(raw)
    normalized = re.sub(r"[\s_]+", " ", raw).strip().lower()
    state_labels = {
        "support only": "market-supported review",
        "support-only": "market-supported review",
        "supportonly": "market-supported review",
        "support only review": "market-supported review",
        "support_only": "market-supported review",
        "cautious": "review only",
        "moderate": "ordinary review",
        "usable": "ordinary review",
        "strong": "higher priority review",
        "weak": "lower priority review",
        "evidence sufficient": "evidence base acceptable",
        "evidence blocker": "evidence gap",
    }
    return state_labels.get(normalized, raw.replace("_", " "))


def _sentence(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None
    text = re.sub(r"\s+", " ", text).strip()
    if text[-1] not in ".!?":
        text = f"{text}."
    return text


def _subject(symbol: str | None, sleeve_name: str | None = None) -> str:
    return symbol or sleeve_name or "This candidate"


def _sleeve_exposure(sleeve_name: str | None) -> str:
    return (sleeve_name or "the sleeve").strip()


def _lower_sleeve_exposure(sleeve_name: str | None) -> str:
    return _sleeve_exposure(sleeve_name).lower()


def _translate_investor_language(value: Any) -> str | None:
    text = _clean_text(value)
    if not text:
        return None

    targeted_replacements = [
        (r"\brecommendation remains review-only\b", "keep under review"),
        (r"\brecommendation admissibility is clear\b", "the candidate remains eligible for review"),
        (r"\bevidence blocker cleared\b", "evidence gap cleared"),
        (r"\brenewed evidence blocker\b", "renewed evidence gap"),
        (r"\bevidence sufficiency\b", "evidence support"),
    ]
    for pattern, replacement in targeted_replacements:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    phrase_replacements = [
        (r"\bsupport[_ -]?only\b", "market support was present, but not enough to justify deployment by itself"),
        (r"\bbounded support\b", "limited supporting evidence"),
        (r"\btiming context\b", "near term market conditions are informational only"),
        (r"\brecommendation authority\b", "reason to promote or deploy"),
        (r"\bevidence sufficient\b", "evidence base is acceptable"),
        (r"\bevidence blocker\b", "evidence was not strong enough to support deployment"),
        (r"\bold schema artifact\b", "stale or unusable timing artifact"),
        (r"\btiming fragile\b", "timing signal is not reliable enough for action"),
        (r"\bcautious\b", "keep under review, but do not deploy on this signal alone"),
    ]
    for pattern, replacement in phrase_replacements:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)

    return _sentence(text)


def _norm_for_compare(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()


_BANNED_VISIBLE_PATTERNS = [
    r"\bvisible decision state changed\b",
    r"\bsleeve question is whether\b",
    r"\binvestment question is whether\b",
    r"\brecommendation authority\b",
    r"\bbounded support\b",
    r"\btiming context\b",
    r"\bscore[- ]?band boundary\b",
    r"\bold schema artifact\b",
    r"\bcandidate row\b",
    r"\bquick brief\b",
    r"\buse the candidate row first\b",
    r"\bsupport[_ -]?only\b",
    r"\busable\b",
    r"\bstrong review\b",
    r"\bmoved from review only to ordinary review\b",
    r"\bnear term market conditions improved, but that only strengthens the review case\b",
]


_UI_INSTRUCTION_PATTERNS = [
    r"\bcandidate row\b",
    r"\bquick brief\b",
    r"\binvestment-case tab\b",
    r"\bopen the report\b",
    r"\buse the .* tab\b",
]


_ACTION_POSTURE_PATTERNS = [
    r"\bdeploy\b",
    r"\bdeployment\b",
    r"\bfirst call\b",
    r"\bfund\b",
    r"\bfunding\b",
    r"\bprepare\b",
    r"\bkeep .*review\b",
    r"\bno allocation change\b",
    r"\bdowngrade priority\b",
    r"\block deployment\b",
    r"\bmonitor only\b",
    r"\bdo not fund\b",
    r"\bdo not allocate\b",
    r"\bdo not add\b",
]

_GENERIC_TRIGGER_IDS = {
    "score_delta",
    "deployability_delta",
    "market_path",
    "timing_state",
    "source_state",
    "source_completion",
    "truth_confidence_delta",
    "sleeve_actionability",
}

_RAW_MOVEMENT_EVENT_TYPES = {
    "candidate_score_moved",
    "score_band_improved",
    "score_band_weakened",
    "recommendation_state_changed",
    "decision_changed",
    "leader_changed",
}

_MATERIAL_EVENT_TYPES = {
    "truth_change",
    "boundary_change",
    "blocker_opened",
    "blocker_cleared",
    "candidate_deployability_changed",
    "source_completion_changed",
    "truth_confidence_changed",
    "candidate_source_strengthened",
    "candidate_source_weakened",
    "source_integrity_changed",
    "document_support_changed",
    "index_scope_added",
    "quick_brief_evidence_added",
    "portfolio_drift_changed",
    "funding_path_changed",
    "sleeve_posture_changed",
}

_FULL_INVESTOR_MATERIALITY = {
    "material",
    "material_source_backed",
    "material_portfolio_backed",
    "historical_source_backed",
}

_FULL_INVESTOR_RENDER_MODES = {"full_investor", "full_investor_explanation"}
_AUDIT_RENDER_MODES = {"compact_audit", "grouped_audit", "hidden_audit"}


@dataclass(frozen=True)
class BlueprintChangeEvidencePacket:
    candidate_id: str | None
    ticker: str | None
    sleeve_id: str | None
    sleeve_label: str | None
    event_time: Any
    old_state: Any
    new_state: Any
    driver_kind: str
    driver_label: str
    driver_facts: tuple[str, ...]
    source_evidence: str | None
    source_provenance: tuple[str, ...]
    portfolio_evidence: str | None
    materiality_status: str
    materiality_reason: str
    closure_status: str
    is_current: bool | None
    event_age_hours: float | None
    source_scan_status: str | None
    render_mode: str
    missing_driver_reason: str | None


_SLEEVE_PROFILES: dict[str, dict[str, str]] = {
    "global equity core": {
        "role": "Core compounding exposure matters when broad developed equity remains the portfolio's main growth engine.",
        "upgrade_reason": "This sleeve is the core compounding engine, so a cleaner candidate can move closer to deployment when sleeve need, funding, and evidence line up.",
        "downgrade_reason": "A weaker read matters because core equity should only absorb capital when the candidate remains clean, broad, and well supported.",
        "next_action": "Compare against close global equity peers on benchmark fit, fee, liquidity, tracking, domicile, and evidence quality before changing allocation priority.",
        "reversal": "A weaker sleeve need, closed funding path, failed peer comparison, or deteriorating evidence would lower review priority.",
    },
    "global equity": {
        "role": "Core compounding exposure matters when broad developed equity remains the portfolio's main growth engine.",
        "upgrade_reason": "This sleeve is the core compounding engine, so a cleaner candidate can move closer to deployment when sleeve need, funding, and evidence line up.",
        "downgrade_reason": "A weaker read matters because core equity should only absorb capital when the candidate remains clean, broad, and well supported.",
        "next_action": "Compare against close global equity peers on benchmark fit, fee, liquidity, tracking, domicile, and evidence quality before changing allocation priority.",
        "reversal": "A weaker sleeve need, closed funding path, failed peer comparison, or deteriorating evidence would lower review priority.",
    },
    "developed ex us optional split": {
        "role": "Developed ex-US exposure is useful only when it improves non-US diversification versus the main global equity sleeve.",
        "upgrade_reason": "The optional split matters when non-US developed exposure adds useful diversification without weakening implementation quality.",
        "downgrade_reason": "A weaker read matters because this sleeve should not add complexity unless it clearly improves the global equity mix.",
        "next_action": "Compare against the main global equity line and other developed ex-US candidates before changing allocation priority.",
        "reversal": "A weaker diversification need, failed comparison versus global equity, or weaker implementation quality would lower review priority.",
    },
    "emerging markets": {
        "role": "Emerging markets exposure adds higher-cyclicality growth, so implementation quality and timing sensitivity matter more than headline exposure alone.",
        "upgrade_reason": "EM exposure is useful only if the candidate remains a clean, liquid, cost-efficient way to express the sleeve.",
        "downgrade_reason": "A weaker read matters because EM should not be funded when timing, liquidity, evidence, or peer comparison argues against action.",
        "next_action": "Compare against other emerging markets candidates on cost, tracking, liquidity, domicile, benchmark scope, and portfolio fit.",
        "reversal": "Renewed timing support, stronger sleeve need, cleaner evidence, or better peer standing could reopen the case for promotion.",
    },
    "china satellite": {
        "role": "China satellite exposure is high-risk and concentrated, so policy risk and evidence quality require a stricter threshold.",
        "upgrade_reason": "A stronger read only matters if policy risk, concentration, liquidity, and evidence support are good enough for a satellite allocation.",
        "downgrade_reason": "A weaker read matters because China-specific exposure should not advance without clear evidence and a deliberate portfolio role.",
        "next_action": "Review policy risk, concentration, liquidity, cost, and evidence quality before changing China satellite priority.",
        "reversal": "Cleaner evidence, stronger policy-risk support, and a better peer comparison could reopen the case for promotion.",
    },
    "ig bonds": {
        "role": "IG bond candidates matter when duration, credit quality, income, and ballast value improve portfolio resilience without adding unnecessary risk.",
        "upgrade_reason": "A stronger read matters if the candidate improves defensive ballast, yield quality, duration fit, or liquidity.",
        "downgrade_reason": "A weaker read matters because bond ballast should not add hidden credit, duration, or liquidity risk.",
        "next_action": "Review duration, credit quality, yield, liquidity, cost, and role versus cash before changing allocation priority.",
        "reversal": "Weaker ballast value, poorer duration fit, credit-quality concerns, or failed peer comparison would lower review priority.",
    },
    "cash and bills": {
        "role": "Cash and bills preserve liquidity, yield, optionality, and funding flexibility.",
        "upgrade_reason": "A stronger read matters when liquidity, yield, and funding optionality are more valuable than adding equity or duration risk.",
        "downgrade_reason": "A weaker read matters if the cash line no longer protects liquidity, yield, or funding flexibility well enough.",
        "next_action": "Review yield, liquidity, duration, currency, vehicle risk, and funding role before changing allocation priority.",
        "reversal": "Lower liquidity value, weaker yield, or a closed funding role would lower review priority.",
    },
    "real assets": {
        "role": "Real assets add inflation sensitivity and diversification, but the implementation must survive cyclicality and liquidity review.",
        "upgrade_reason": "A stronger read matters if inflation linkage or diversification improves without adding excessive implementation risk.",
        "downgrade_reason": "A weaker read matters because real assets should not advance when cyclicality, liquidity, or evidence weakens the role.",
        "next_action": "Review inflation linkage, cyclicality, liquidity, cost, and evidence before changing allocation priority.",
        "reversal": "Lower inflation relevance, failed peer comparison, or weaker implementation quality would lower review priority.",
    },
    "alternatives": {
        "role": "Alternatives should add a selective, non-redundant return source with clear risk control.",
        "upgrade_reason": "A stronger read matters only if the candidate adds diversification that is not already replicated elsewhere.",
        "downgrade_reason": "A weaker read matters because alternatives should not add complexity without a distinct and reliable portfolio role.",
        "next_action": "Compare against other diversifiers on return source, drawdown behavior, liquidity, cost, and mandate fit.",
        "reversal": "A redundant return source, weaker risk control, or failed peer comparison would lower review priority.",
    },
    "convex protection": {
        "role": "Convex protection candidates matter when the portfolio needs crisis response, drawdown protection, managed futures, tail-risk behavior, or trend following.",
        "upgrade_reason": "Managed futures or convex protection can matter when the portfolio needs crisis response and equity drawdown diversification, but the case still depends on implementation quality and peer comparison.",
        "downgrade_reason": "A weaker read matters because protection sleeves should only advance when reliability under stress, mandate fit, and implementation quality remain credible.",
        "next_action": "Compare against CAOS and other convex protection candidates on crisis behavior, cost, liquidity, tracking, and mandate fit.",
        "reversal": "Weaker crisis protection need, failed peer comparison, or deteriorating implementation quality would move it back to ordinary review.",
    },
}


def _contains_pattern(value: str | None, patterns: list[str]) -> bool:
    text = str(value or "")
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def _event_text(event: dict[str, Any]) -> str:
    return " ".join(
        str(part or "")
        for part in [
            event.get("event_type"),
            event.get("summary"),
            event.get("change_trigger"),
            event.get("reason_summary"),
            event.get("implication_summary"),
            event.get("portfolio_consequence"),
            event.get("next_action"),
            event.get("what_would_reverse"),
            event.get("previous_state"),
            event.get("current_state"),
        ]
    )


def _event_type(event: dict[str, Any]) -> str:
    return str(event.get("event_type") or "").strip().lower()


def _parse_utc(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _event_age_hours(value: Any) -> float | None:
    parsed = _parse_utc(value)
    if parsed is None:
        return None
    return round(max(0.0, (datetime.now(UTC) - parsed).total_seconds() / 3600.0), 2)


def _is_generic_trigger(value: Any) -> bool:
    raw = str(value or "").strip().lower()
    if not raw:
        return True
    return raw in _GENERIC_TRIGGER_IDS or bool(re.fullmatch(r"[a-z][a-z0-9_]+", raw))


def _has_specific_driver_text(value: Any) -> bool:
    text = _clean_text(value)
    if not text:
        return False
    if _is_generic_trigger(text):
        return False
    if _is_weak_explicit_text(text):
        return False
    normalized = _norm_for_compare(text)
    weak_fragments = [
        "does not expose",
        "driver unavailable",
        "no fresh source packet",
        "ledger records",
        "latest timing refresh",
        "changed near term market support",
    ]
    return not any(fragment in normalized for fragment in weak_fragments)


def _is_specific_evidence(value: Any) -> bool:
    text = _clean_text(value)
    if not text or not _has_specific_driver_text(text):
        return False
    normalized = _norm_for_compare(text)
    generic_fragments = [
        "timing improved",
        "timing support improved",
        "market path strengthened plus timing",
        "market path support improved",
        "candidate moved states",
        "review state changed",
        "score band changed",
        "candidate can compete better",
        "does not expose a specific driver",
    ]
    if any(fragment in normalized for fragment in generic_fragments):
        return False
    return bool(
        re.search(
            r"\b(target|below target|above target|funding|sgov|cash|evidence|blocker|policy|gate|breadth|liquidity|spread|duration|yield|tracking|domicile|peer|source|freshness|stale|implementation|sleeve)\b",
            text,
            flags=re.IGNORECASE,
        )
    )


def _driver_kind_for_event(event: dict[str, Any], *, category: str) -> str:
    text = _event_text(event).lower()
    event_type = _event_type(event)
    if event_type == "candidate_score_moved":
        return "raw_state_movement_only"
    if category == "timing":
        return "timing_signal"
    if category == "freshness_risk" or "freshness" in text or "stale" in text:
        return "source_freshness"
    if category == "blocker" or event_type in {"boundary_change", "blocker_opened", "blocker_cleared"}:
        return "policy_gate"
    if "funding" in text:
        return "funding_path"
    if "underweight" in text or "overweight" in text or "target" in text or category == "portfolio_drift":
        return "sleeve_drift"
    if "policy" in text or "gate" in text or "blocker" in text:
        return "policy_gate"
    if "evidence" in text or "document" in text or "source" in text or category == "source_evidence":
        return "evidence_blocker"
    if "market path" in text or "breadth" in text:
        return "market_path"
    if "portfolio fit" in text:
        return "portfolio_fit"
    if "implementation" in text or "tracking" in text or "liquidity" in text or "spread" in text:
        return "implementation_quality"
    if "peer" in text or "comparison" in text:
        return "peer_comparison"
    if event_type in _RAW_MOVEMENT_EVENT_TYPES or category == "decision":
        return "raw_state_movement_only"
    return "raw_state_movement_only"


def _render_mode_for(
    *,
    materiality_status: str,
    driver_kind: str,
    source_evidence: str | None,
    portfolio_evidence: str | None,
) -> str:
    if materiality_status == "suppressed_not_material":
        return "suppressed"
    if materiality_status in {"unresolved_driver_missing", "raw_movement_only"}:
        return "grouped_audit"
    if driver_kind == "raw_state_movement_only":
        return "grouped_audit"
    if materiality_status in _FULL_INVESTOR_MATERIALITY and (
        _is_specific_evidence(source_evidence) or _is_specific_evidence(portfolio_evidence)
    ):
        return "full_investor_explanation"
    return "grouped_audit"


def _materiality_class_for(*, materiality_status: str, render_mode: str, closure_status: str) -> str:
    if render_mode == "suppressed" or materiality_status == "suppressed_not_material":
        return "suppressed"
    if render_mode in _AUDIT_RENDER_MODES or materiality_status in {"unresolved_driver_missing", "raw_movement_only"}:
        return "audit_only"
    if materiality_status not in _FULL_INVESTOR_MATERIALITY:
        return "system_only"
    if closure_status == "open_actionable":
        return "investor_material"
    return "review_material"


def _driver_packet_from_packet(packet: BlueprintChangeEvidencePacket) -> dict[str, Any]:
    driver_type = {
        "evidence_blocker": "source",
        "source_freshness": "source",
        "market_path": "market",
        "timing_signal": "timing",
        "sleeve_drift": "portfolio",
        "funding_path": "portfolio",
        "policy_gate": "policy",
        "portfolio_fit": "portfolio",
        "implementation_quality": "score",
        "peer_comparison": "score",
        "raw_state_movement_only": "unknown",
    }.get(packet.driver_kind, "unknown")
    return {
        "driver_type": driver_type,
        "driver_name": packet.driver_label if packet.driver_label != "driver unavailable" else None,
        "driver_summary": packet.source_evidence or packet.portfolio_evidence or (packet.driver_facts[0] if packet.driver_facts else None),
        "source_ref": packet.source_provenance[0] if packet.source_provenance else None,
        "previous_value": packet.old_state,
        "current_value": packet.new_state,
        "threshold": None,
        "materiality": "audit" if packet.render_mode in _AUDIT_RENDER_MODES else ("high" if packet.closure_status == "open_actionable" else "medium"),
        "confidence": "low" if packet.render_mode in _AUDIT_RENDER_MODES else "medium",
        "preserved": packet.render_mode in _FULL_INVESTOR_RENDER_MODES and bool(packet.driver_facts),
    }


def _trigger_type_for_driver_kind(driver_kind: str) -> str:
    return {
        "evidence_blocker": "source",
        "source_freshness": "source",
        "market_path": "market",
        "timing_signal": "timing",
        "sleeve_drift": "portfolio",
        "funding_path": "portfolio",
        "policy_gate": "policy",
        "portfolio_fit": "portfolio",
        "implementation_quality": "score",
        "peer_comparison": "score",
        "raw_state_movement_only": "unknown",
    }.get(str(driver_kind or "").strip(), "unknown")


def _freshness_state_for_trigger(packet: BlueprintChangeEvidencePacket) -> str:
    raw = str(packet.source_scan_status or "").strip().lower()
    if raw in {"current", "stale", "last_good", "degraded_runtime", "unknown"}:
        return raw
    if raw in {"success", "fresh"}:
        return "current"
    if raw in {"degraded", "execution_failed_or_incomplete"}:
        return "degraded_runtime"
    return "unknown"


def _materiality_label_for_trigger(packet: BlueprintChangeEvidencePacket) -> str:
    if packet.render_mode in _AUDIT_RENDER_MODES:
        return "low"
    if packet.closure_status == "open_actionable":
        return "high"
    return "medium"


def _primary_trigger_from_packet(packet: BlueprintChangeEvidencePacket, *, display_label: str | None) -> dict[str, Any]:
    preserved = packet.render_mode in _FULL_INVESTOR_RENDER_MODES and bool(packet.driver_facts)
    label = (
        _clean_text(display_label)
        or packet.source_evidence
        or packet.portfolio_evidence
        or (packet.driver_facts[0] if packet.driver_facts else None)
        or packet.missing_driver_reason
    )
    return {
        "trigger_type": _trigger_type_for_driver_kind(packet.driver_kind),
        "driver_family": packet.driver_kind,
        "driver_name": packet.driver_label if packet.driver_label != "driver unavailable" else None,
        "display_label": label,
        "previous_value": packet.old_state,
        "current_value": packet.new_state,
        "change_value": None,
        "unit": None,
        "threshold": None,
        "observed_at": packet.event_time,
        "source_ref": packet.source_provenance[0] if packet.source_provenance else None,
        "freshness_state": _freshness_state_for_trigger(packet),
        "confidence": "low" if not preserved else ("high" if packet.closure_status == "open_actionable" else "medium"),
        "materiality": _materiality_label_for_trigger(packet),
        "preserved": preserved,
    }


def _candidate_impact_from_detail(
    packet: BlueprintChangeEvidencePacket,
    *,
    direction: str,
    detail_rows: dict[str, Any],
    score_delta: dict[str, float | int | None],
) -> dict[str, Any]:
    if packet.driver_kind in {"evidence_blocker", "source_freshness"}:
        affected_dimension = "source_confidence"
    elif packet.driver_kind == "timing_signal":
        affected_dimension = "timing"
    elif packet.driver_kind == "market_path":
        affected_dimension = "market_path"
    elif packet.driver_kind in {"sleeve_drift", "funding_path", "portfolio_fit"}:
        affected_dimension = "portfolio_fit"
    elif packet.driver_kind == "policy_gate":
        affected_dimension = "policy"
    elif packet.driver_kind == "raw_state_movement_only":
        affected_dimension = "recommendation"
    else:
        affected_dimension = "recommendation"

    if packet.driver_kind == "policy_gate" and _is_downgrade(direction):
        impact_direction = "blocked"
    elif packet.driver_kind == "policy_gate" and _is_upgrade(direction):
        impact_direction = "cleared"
    elif _is_upgrade(direction):
        impact_direction = "strengthened"
    elif _is_downgrade(direction):
        impact_direction = "weakened"
    else:
        impact_direction = "neutral"

    return {
        "affected_candidate_id": packet.candidate_id,
        "symbol": packet.ticker,
        "sleeve_id": packet.sleeve_id,
        "impact_direction": impact_direction,
        "affected_dimension": affected_dimension,
        "before_state": _display_state(packet.old_state),
        "after_state": _display_state(packet.new_state),
        "score_before": score_delta.get("from"),
        "score_after": score_delta.get("to"),
        "why_it_matters": detail_rows.get("reason"),
        "next_action": detail_rows.get("next_action"),
        "reversal_condition": detail_rows.get("reversal_condition"),
    }


def _profile_for_sleeve(sleeve_name: str | None) -> dict[str, str]:
    key = _lower_sleeve_exposure(sleeve_name)
    return _SLEEVE_PROFILES.get(
        key,
        {
            "role": f"{_sleeve_exposure(sleeve_name)} matters only if the candidate improves the sleeve job without weakening implementation quality.",
            "upgrade_reason": "A stronger read matters when the candidate improves sleeve fit, implementation quality, evidence, or peer standing.",
            "downgrade_reason": "A weaker read matters when sleeve fit, implementation quality, evidence, or peer standing no longer supports higher priority.",
            "next_action": "Compare against closest same-sleeve peers before changing allocation priority.",
            "reversal": "A weaker sleeve need, failed peer comparison, or deteriorating implementation quality would lower review priority.",
        },
    )


def _is_upgrade(direction: str) -> bool:
    return str(direction or "").strip().lower() == "upgrade"


def _is_downgrade(direction: str) -> bool:
    return str(direction or "").strip().lower() == "downgrade"


def _state_transition_phrase(previous_state: Any, current_state: Any) -> str | None:
    previous_label = _display_state(previous_state)
    current_label = _display_state(current_state)
    if previous_label and current_label:
        return f"{previous_label} to {current_label}"
    return previous_label or current_label


def _is_state_movement_only(value: str | None) -> bool:
    normalized = _norm_for_compare(value)
    if not normalized:
        return False
    return bool(re.fullmatch(r".{1,24} moved from .{1,60} to .{1,60}", normalized))


def _is_weak_explicit_text(value: str | None) -> bool:
    text = _clean_text(value)
    if not text:
        return True
    raw = str(text).strip()
    if re.fullmatch(r"[a-z][a-z0-9_]+", raw, flags=re.IGNORECASE):
        return True
    normalized = _norm_for_compare(text)
    if _contains_pattern(text, _BANNED_VISIBLE_PATTERNS + _UI_INSTRUCTION_PATTERNS):
        return True
    weak_fragments = [
        "moved from",
        "current decision read changed",
        "stored surface state",
        "score band",
        "overall standing",
        "can compete",
        "review burden",
        "question is whether",
    ]
    if any(fragment in normalized for fragment in weak_fragments):
        return True
    return False


def _action_posture_for(category: str, direction: str) -> str:
    if category == "blocker" and _is_downgrade(direction):
        return "Block deployment"
    if category == "blocker" and _is_upgrade(direction):
        return "Prepare for deployment review"
    if _is_upgrade(direction):
        return "No allocation change yet"
    if _is_downgrade(direction):
        return "Downgrade priority"
    if category == "freshness_risk":
        return "Monitor only"
    return "Keep under review"


def _category_driver_label(event: dict[str, Any], *, category: str, direction: str) -> str:
    event_type = _event_type(event)
    text = _event_text(event).lower()
    if "funding" in text or event_type == "funding_path_changed":
        return "funding path"
    if "underweight" in text or "overweight" in text or "target" in text or category == "portfolio_drift":
        return "sleeve position"
    if "evidence" in text or "source" in text or "document" in text or category == "source_evidence":
        return "evidence quality"
    if "blocker" in text or "gate" in text or category == "blocker":
        return "investment blocker"
    if "timing" in text or "market setup" in text or "market path" in text or category == "timing":
        return "near term market support"
    if "implementation" in text or "liquidity" in text or "tracking" in text or "fee" in text or "cost" in text:
        return "implementation quality"
    if "peer" in text or "compare" in text:
        return "peer comparison"
    if category == "freshness_risk":
        return "source freshness"
    if category == "decision" and _is_upgrade(direction):
        return "review priority"
    if category == "decision" and _is_downgrade(direction):
        return "review priority"
    return "stored decision read"


def _driver_is_unavailable(value: str | None) -> bool:
    normalized = _norm_for_compare(value)
    return normalized.startswith("the ledger records") or normalized.startswith("the available ledger records")


def _driver_fact_for_summary(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
) -> str:
    subject = _subject(symbol, sleeve_name)
    sleeve = _sleeve_exposure(sleeve_name)
    driver = _investment_trigger(
        event,
        category=category,
        direction=direction,
        symbol=symbol,
        sleeve_name=sleeve_name,
    )
    if category == "timing":
        if _driver_is_unavailable(driver):
            direction_label = "upgrade" if _is_upgrade(direction) else "downgrade" if _is_downgrade(direction) else "change"
            return f"The ledger records a timing {direction_label}, but does not expose the specific market driver."
        if _is_specific_evidence(driver):
            return driver
        verb = "improved" if _is_upgrade(direction) else "weakened" if _is_downgrade(direction) else "changed"
        return f"{sleeve} timing support {verb} after the latest timing refresh."
    if category == "decision" and _driver_is_unavailable(driver):
        direction_label = "increased" if _is_upgrade(direction) else "decreased" if _is_downgrade(direction) else "changed"
        return f"{sleeve} review priority {direction_label}, but the ledger does not expose a specific market or portfolio driver."
    if _driver_is_unavailable(driver):
        return driver
    return driver


def _role_phrase(sleeve_name: str | None) -> str:
    sleeve = _lower_sleeve_exposure(sleeve_name)
    if sleeve == "convex protection":
        return "crisis response, drawdown protection, managed futures exposure, and reliability under stress"
    if sleeve == "developed ex us optional split":
        return "non-US developed diversification versus the main global equity sleeve"
    if sleeve == "emerging markets":
        return "higher-cyclicality growth exposure, liquidity, implementation quality, and clean evidence"
    if sleeve == "china satellite":
        return "policy risk, concentration risk, liquidity, and a stricter evidence threshold"
    if sleeve == "ig bonds":
        return "duration, credit quality, income, liquidity, and ballast value"
    if sleeve == "cash and bills":
        return "liquidity, yield, optionality, and funding flexibility"
    if sleeve == "real assets":
        return "inflation sensitivity, diversification, cyclicality, and implementation quality"
    if sleeve == "alternatives":
        return "a non-redundant return source, risk control, and peer fit"
    if sleeve in {"global equity", "global equity core"}:
        return "core compounding exposure, funding priority, evidence quality, and broad developed equity fit"
    return "sleeve fit, implementation quality, evidence, and peer comparison"


def _specific_current_state_label(current_state: Any, fallback: str) -> str:
    label = _display_state(current_state)
    return label or fallback


def _driver_bundle(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
    current_state: Any,
) -> dict[str, str]:
    subject = _subject(symbol, sleeve_name)
    sleeve = _sleeve_exposure(sleeve_name)
    driver_sentence = _driver_fact_for_summary(
        event,
        category=category,
        direction=direction,
        symbol=symbol,
        sleeve_name=sleeve_name,
    )
    state_label = _specific_current_state_label(
        current_state,
        "higher priority review" if _is_upgrade(direction) else "lower priority review" if _is_downgrade(direction) else "review",
    )
    role_phrase = _role_phrase(sleeve_name)
    if category == "timing":
        if _is_upgrade(direction):
            implication = f"{subject} can move into {state_label} only if cost, tracking, liquidity, domicile, evidence, and sleeve fit remain acceptable."
            posture = "Keep it in review rather than treating timing alone as an allocation instruction."
        elif _is_downgrade(direction):
            implication = f"{subject} may remain a valid {sleeve} candidate, but timing no longer strengthens the case for deployment."
            posture = f"No deployment change. Keep {subject} under review and do not fund it on this signal alone."
        else:
            implication = f"{subject} remains a review candidate, but timing is only one input to the sleeve decision."
            posture = "No allocation change. Treat the timing read as background until sleeve need and evidence also support action."
    elif category == "decision":
        if _is_upgrade(direction):
            implication = f"{subject} deserves closer review for {role_phrase}."
            posture = "No allocation change yet; deployment still depends on peer comparison, implementation quality, evidence, and sleeve fit."
        elif _is_downgrade(direction):
            implication = f"{subject} should move lower in review priority until {role_phrase} strengthen again."
            posture = "Downgrade priority. Do not move it toward deployment until the investment case improves."
        else:
            implication = f"{subject}'s review priority changed inside {sleeve}."
            posture = "Keep under review until the investment driver is clear enough to act on."
    elif category == "source_evidence":
        if _is_upgrade(direction):
            implication = f"{subject} can be judged more on portfolio fit and implementation quality rather than being held back by source support."
            posture = "Prepare for deployment review only if funding, mandate fit, and peer comparison also hold up."
        elif _is_downgrade(direction):
            implication = f"{subject} needs stronger source support before it can sustain a firm peer comparison."
            posture = "Downgrade priority. Keep under review until evidence is strong enough to support action."
        else:
            implication = f"{subject}'s evidence base changed, which affects confidence in the investment read."
            posture = "Keep under review until the comparison is ready for action."
    elif category == "portfolio_drift":
        implication = f"{subject} becomes more relevant only if {sleeve} still has a real sleeve gap and funding path."
        posture = "Prepare for deployment review only if sleeve need and funding remain confirmed."
    elif category == "blocker":
        if _is_upgrade(direction):
            implication = f"{subject} can return to investment review inside {sleeve}."
            posture = "Prepare for deployment review only after evidence, implementation quality, and sleeve need also support action."
        else:
            implication = f"{subject} should not be treated as deployable while the blocker is active."
            posture = "Block deployment until the blocker clears and the candidate can be compared normally."
    elif category == "freshness_risk":
        implication = f"{subject}'s review confidence depends on whether the source base is fresh enough to trust."
        posture = "Monitor only. Do not promote the candidate until source freshness improves."
    else:
        implication = f"{subject}'s review status changed inside {sleeve}, but the investment driver is not fully exposed."
        posture = "Keep under review. Do not change allocation until the driver is clear."
    return {
        "driver_kind": category,
        "driver_sentence": driver_sentence,
        "candidate_implication": implication,
        "allocation_posture": posture,
    }


def _investment_trigger(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
) -> str:
    raw_trigger = event.get("change_trigger")
    explicit = _translate_investor_language(raw_trigger)
    if explicit and not _is_weak_explicit_text(raw_trigger) and not _is_weak_explicit_text(explicit):
        return explicit

    subject = _subject(symbol, sleeve_name)
    sleeve = _sleeve_exposure(sleeve_name)
    event_type = _event_type(event)
    driver = _category_driver_label(event, category=category, direction=direction)

    if category == "timing":
        verb = "strengthened" if _is_upgrade(direction) else "weakened" if _is_downgrade(direction) else "changed"
        return f"The latest timing refresh {verb} {sleeve} market support for {subject}."
    if event_type in {"score_band_improved", "score_band_weakened"} or category == "decision":
        if driver != "review priority":
            verb = "improved" if _is_upgrade(direction) else "weakened" if _is_downgrade(direction) else "changed"
            return f"{driver.capitalize()} {verb} enough to change {subject}'s review priority inside {sleeve}."
        direction_label = "upgrade" if _is_upgrade(direction) else "downgrade" if _is_downgrade(direction) else "change"
        return f"The ledger records a review priority {direction_label}, but does not expose a specific market or portfolio driver."
    if category == "source_evidence":
        verb = "improved" if _is_upgrade(direction) else "weakened" if _is_downgrade(direction) else "changed"
        return f"Evidence quality {verb} for {subject}."
    if category == "portfolio_drift":
        return f"Sleeve positioning or funding priority changed for {sleeve}."
    if category == "blocker":
        verb = "cleared" if _is_upgrade(direction) else "appeared" if _is_downgrade(direction) else "changed"
        return f"An investment blocker {verb} for {subject}."
    if category == "freshness_risk":
        return f"Source freshness changed for {subject}, which affects how much confidence the current read deserves."
    return f"The available ledger records a change for {subject}, but does not expose a specific market or portfolio driver."


def _summary_phrase_for_driver(trigger: str) -> str:
    text = trigger.strip()
    if text.lower().startswith("the ledger records"):
        return "the ledger changed its review priority without exposing the underlying driver"
    if text.lower().startswith("the available ledger"):
        return "the ledger recorded a change without exposing the underlying driver"
    return text[0].lower() + text[1:].rstrip(".") if text else "the change driver was unavailable"


def _sanitize_visible_text(value: Any) -> str | None:
    text = _translate_investor_language(value)
    if not text:
        return None
    replacements = [
        (r"\bvisible decision state changed\b", "review priority changed"),
        (r"\bThe sleeve question is whether\b", "The investment question is whether"),
        (r"\bscore[- ]?band boundary\b", "investment case weakens enough to lower review priority"),
        (r"\bcandidate row\b", "candidate review"),
        (r"\bquick brief\b", "investment note"),
        (r"\busable\b", "ordinary review"),
        (r"\bstrong review\b", "higher priority review"),
    ]
    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return _sentence(text)


def _visible_text_is_clean(value: str | None) -> bool:
    text = _clean_text(value)
    if not text:
        return False
    return not _contains_pattern(text, _BANNED_VISIBLE_PATTERNS)


def _number(value: Any) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return value
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    return int(parsed) if parsed.is_integer() else parsed


def _score_delta(event: dict[str, Any]) -> dict[str, float | int | None]:
    raw = event.get("score_delta")
    if isinstance(raw, dict):
        return {
            "from": _number(raw.get("from")),
            "to": _number(raw.get("to")),
        }
    return {"from": None, "to": None}


def _candidate_links(candidate_id: str | None, report_tab: str | None) -> dict[str, str | None]:
    if not candidate_id:
        return {
            "candidate_recommendation_href": None,
            "report_href": None,
        }
    tab = report_tab or "investment_case"
    return {
        "candidate_recommendation_href": f"?view=candidates&candidate={candidate_id}",
        "report_href": f"?view=candidates&candidate={candidate_id}&report={candidate_id}&report_tab={tab}",
    }


def _scope(event: dict[str, Any]) -> str:
    if event.get("candidate_id"):
        return "candidate"
    if event.get("sleeve_id"):
        return "sleeve"
    return "system" if str(event.get("category") or "") == "system" else "portfolio"


def _actionability(category: str, severity: str, requires_review: bool, direction: str) -> str:
    if requires_review or severity == "high":
        return "review"
    if category in {"decision", "blocker"} and direction in {"upgrade", "downgrade"}:
        return "review"
    if category in {"freshness_risk", "system"}:
        return "monitor"
    return "monitor" if severity == "medium" else "no_action"


def _confidence(event: dict[str, Any], category: str) -> str:
    if event.get("candidate_id") and category in {"decision", "blocker", "source_evidence"}:
        return "high"
    if category in {"system", "freshness_risk"}:
        return "medium"
    return "medium"


def _evidence_refs(event: dict[str, Any]) -> list[str]:
    refs: list[str] = []
    target = event.get("deep_link_target")
    if isinstance(target, dict):
        target_type = str(target.get("target_type") or "").strip()
        target_id = str(target.get("target_id") or "").strip()
        if target_type and target_id:
            refs.append(f"{target_type}:{target_id}")
    candidate_id = str(event.get("candidate_id") or "").strip()
    if candidate_id and not refs:
        refs.append(f"candidate_report:{candidate_id}")
    return refs


def _source_freshness(event: dict[str, Any], occurred_at: Any) -> dict[str, str | None]:
    raw = event.get("source_freshness")
    state = None
    latest_event_at = None
    if isinstance(raw, dict):
        state = _clean_text(raw.get("state"))
        latest_event_at = _clean_text(raw.get("latest_event_at"))
    category = str(event.get("category") or "").strip().lower()
    if not state:
        state = "stale" if category == "freshness_risk" else "unknown"
    return {
        "state": state,
        "latest_event_at": latest_event_at or _clean_text(occurred_at),
    }


def _driver_label(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
) -> str:
    event_type = _event_type(event)
    text = _event_text(event).lower()
    if category == "timing":
        if _has_specific_driver_text(event.get("change_trigger")):
            return "market path support"
        return "timing driver unavailable"
    if category == "portfolio_drift" or "underweight" in text or "funding" in text:
        if "funding" in text and ("underweight" in text or "target" in text):
            return "sleeve position + funding"
        if "funding" in text:
            return "funding path"
        return "sleeve position"
    if category == "source_evidence" or "evidence" in text or "source" in text or "document" in text:
        return "source evidence"
    if category == "blocker" or "blocker" in text or "gate" in text or event_type in {"boundary_change", "blocker_opened", "blocker_cleared"}:
        return "blocker state"
    if category == "decision":
        return "review priority"
    if category == "freshness_risk":
        return "source freshness"
    if _has_specific_driver_text(event.get("change_trigger")):
        return _category_driver_label(event, category=category, direction=direction)
    return "driver unavailable"


def _source_evidence_summary(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
) -> str:
    subject = _subject(symbol, sleeve_name)
    sleeve = _sleeve_exposure(sleeve_name)
    explicit_source = _sanitize_visible_text(event.get("source_evidence"))
    if explicit_source and _has_specific_driver_text(explicit_source):
        return explicit_source

    trigger = _sanitize_visible_text(event.get("change_trigger"))
    if trigger and _has_specific_driver_text(trigger):
        if category == "timing":
            return f"Source packet reports that {trigger[0].lower() + trigger[1:]}"
        return trigger

    event_type = _event_type(event)
    if event_type in {"truth_change", "source_completion_changed", "truth_confidence_changed", "candidate_source_strengthened", "candidate_source_weakened", "source_integrity_changed", "document_support_changed"}:
        verb = "improved" if _is_upgrade(direction) else "weakened" if _is_downgrade(direction) else "changed"
        return f"Fresh source coverage {verb} for {subject}."
    if event_type in {"index_scope_added", "quick_brief_evidence_added"}:
        return f"Fresh source scan added usable first-read evidence for {subject}."
    if category == "portfolio_drift":
        if "funding" in _event_text(event).lower():
            return f"Portfolio overlay changed the funding path or sleeve position for {sleeve}."
        return f"Portfolio overlay changed the sleeve position for {sleeve}."
    if category == "blocker":
        verb = "cleared" if _is_upgrade(direction) else "appeared" if _is_downgrade(direction) else "changed"
        return f"Policy or evidence review shows an investment blocker {verb} for {subject}."
    if category == "freshness_risk":
        return f"Source freshness changed enough to affect confidence in {subject}."
    if category == "timing":
        verb = "improved" if _is_upgrade(direction) else "weakened" if _is_downgrade(direction) else "changed"
        return f"Market path support {verb}, but the source packet does not expose a specific driver."
    return "No fresh source packet supports this movement."


def _materiality_assessment(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    source_evidence: str,
    trigger: str,
    portfolio_consequence: str,
) -> dict[str, str]:
    event_type = _event_type(event)
    has_specific_source = _has_specific_driver_text(source_evidence)
    has_specific_trigger = _has_specific_driver_text(trigger)
    has_action_posture = _contains_pattern(portfolio_consequence, _ACTION_POSTURE_PATTERNS)

    if event_type == "candidate_score_moved":
        return {
            "materiality_status": "raw_movement_only",
            "materiality_reason": "Raw score movement is not enough for an investor-facing change without a source, portfolio, evidence, funding, blocker, or policy driver.",
        }

    if event_type in _MATERIAL_EVENT_TYPES:
        materiality_status = (
            "material_portfolio_backed"
            if category == "portfolio_drift" or event_type in {"portfolio_drift_changed", "funding_path_changed", "sleeve_posture_changed"}
            else "material_source_backed"
        )
        return {
            "materiality_status": materiality_status,
            "materiality_reason": "The change is tied to source evidence, portfolio position, funding, blocker state, or deployment readiness.",
        }

    if category == "timing":
        if has_specific_source or has_specific_trigger:
            return {
                "materiality_status": "material_source_backed",
                "materiality_reason": "The timing change includes a source-backed market driver.",
            }
        return {
            "materiality_status": "unresolved_driver_missing",
            "materiality_reason": "The ledger records a timing movement but does not preserve the market driver needed to make it current investor evidence.",
        }

    if event_type in _RAW_MOVEMENT_EVENT_TYPES:
        if has_specific_source or has_specific_trigger:
            return {
                "materiality_status": "material_source_backed",
                "materiality_reason": "The review-priority change has a specific investment driver or portfolio consequence.",
            }
        return {
            "materiality_status": "raw_movement_only",
            "materiality_reason": "The ledger records review-state movement without a preserved investment driver.",
        }

    if category == "decision":
        if has_specific_source or has_specific_trigger or has_action_posture:
            return {
                "materiality_status": "material_portfolio_backed" if has_action_posture else "material_source_backed",
                "materiality_reason": "The review-priority change has a specific investment driver or portfolio consequence.",
            }
        return {
            "materiality_status": "unresolved_driver_missing",
            "materiality_reason": "The ledger records review-state movement without a preserved investment driver.",
        }

    if category in {"source_evidence", "portfolio_drift", "blocker", "freshness_risk"}:
        materiality_status = "material_portfolio_backed" if category == "portfolio_drift" else "material_source_backed"
        return {
            "materiality_status": materiality_status,
            "materiality_reason": "The change affects source confidence, sleeve position, blocker state, or freshness.",
        }

    if has_specific_source or has_specific_trigger:
        return {
            "materiality_status": "material_source_backed",
            "materiality_reason": "The change includes a specific evidence or investment driver.",
        }

    return {
        "materiality_status": "unresolved_driver_missing",
        "materiality_reason": "The ledger does not preserve enough driver detail for a current investor-facing change.",
    }


def _closure_status_for(
    *,
    materiality_status: str,
    category: str,
    severity: str | None,
    requires_review: bool | None,
    portfolio_consequence: str,
) -> str:
    if materiality_status == "suppressed_not_material":
        return "suppressed_not_material"
    if materiality_status in {"unresolved_driver_missing", "raw_movement_only"}:
        return "unresolved_driver_missing"
    if _contains_pattern(portfolio_consequence, [r"\bdeploy\b", r"\bfirst call\b", r"\bfund\b", r"\bblock deployment\b"]):
        return "open_actionable"
    if category in {"decision", "blocker", "portfolio_drift", "source_evidence"} or requires_review or severity == "high":
        return "open_review"
    return "closed_no_action"


def _detail_trigger(
    event: dict[str, Any],
    *,
    summary: str,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
    previous_state: Any,
    current_state: Any,
) -> str:
    return _investment_trigger(
        event,
        category=category,
        direction=direction,
        symbol=symbol,
        sleeve_name=sleeve_name,
    )


def _detail_reason(
    event: dict[str, Any],
    *,
    why_it_matters: str,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
) -> str:
    subject = _subject(symbol, sleeve_name)
    sleeve = _lower_sleeve_exposure(sleeve_name)
    profile = _profile_for_sleeve(sleeve_name)
    if category == "timing":
        if direction == "downgrade":
            return f"{subject} may still be a valid {sleeve} candidate, but near term market support no longer strengthens the case enough to promote it."
        if direction == "upgrade":
            return f"Timing support can raise review priority, but {subject} still needs to prove cost, tracking, liquidity, domicile, evidence quality, and sleeve fit before allocation priority changes."
        return f"Near term conditions changed for {subject}, but they should be treated as background rather than a standalone deployment signal."

    raw_reason = event.get("reason_summary")
    explicit = _sanitize_visible_text(raw_reason)
    if explicit and not _is_weak_explicit_text(raw_reason) and not _is_weak_explicit_text(explicit):
        return explicit
    sanitized_why = _sanitize_visible_text(why_it_matters)
    if sanitized_why and not _is_weak_explicit_text(why_it_matters) and not _is_weak_explicit_text(sanitized_why):
        return sanitized_why
    if category == "decision" and _is_upgrade(direction):
        return profile["upgrade_reason"]
    if category == "decision" and _is_downgrade(direction):
        return profile["downgrade_reason"]
    if category == "blocker":
        if _is_upgrade(direction):
            return f"{subject} can re-enter investment review only if the cleared blocker leaves enough evidence and implementation quality to compare it fairly."
        return f"{subject} should not move toward deployment while the blocker keeps the investment case unresolved."
    if category == "source_evidence":
        return f"Evidence quality matters because {subject} cannot support a firm preference unless the sources are strong enough to compare it against peers."
    if category == "portfolio_drift":
        return f"{profile['role']} Sleeve drift can change priority only when the candidate still passes implementation and peer review."
    if category == "freshness_risk":
        return f"Fresh information matters because stale source evidence should reduce confidence before the ETF is promoted or funded."
    return profile["role"]


def _detail_portfolio_consequence(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
) -> str:
    subject = _subject(symbol, sleeve_name)
    sleeve = _lower_sleeve_exposure(sleeve_name)
    if category == "timing":
        if direction == "downgrade":
            return f"No deployment change. Keep {sleeve} exposure under review, but do not fund {subject} today on this signal alone."
        if direction == "upgrade":
            return f"No allocation change yet. Near term support can raise review priority only if sleeve need, evidence, and implementation quality also support action."
        return f"No portfolio change by itself. Use this timing read as background for {subject}, not as a standalone instruction."

    explicit = _sanitize_visible_text(event.get("portfolio_consequence"))
    if explicit and not _is_weak_explicit_text(explicit) and _contains_pattern(explicit, _ACTION_POSTURE_PATTERNS):
        return explicit
    if category == "portfolio_drift":
        return f"Prepare for deployment review only if {sleeve} still needs capital and the funding path remains confirmed."
    if category == "blocker":
        if _is_upgrade(direction):
            return f"Prepare for deployment review only after the cleared blocker is matched with clean evidence, implementation quality, and sleeve need."
        return f"Block deployment. Keep {subject} out of allocation consideration until the blocker clears."
    if category == "source_evidence":
        if _is_upgrade(direction):
            return f"Prepare for deployment review if the evidence base is now strong enough to support peer comparison."
        return f"Downgrade priority. Keep under review until evidence is strong enough to support a firm preference."
    if category == "decision":
        if _is_upgrade(direction):
            return f"No allocation change yet. Raise review priority inside {_sleeve_exposure(sleeve_name)} before considering deployment."
        if _is_downgrade(direction):
            return f"Downgrade priority. Keep {subject} under review, but do not allocate until the investment case strengthens."
    if category == "freshness_risk":
        return f"Monitor only. Do not treat {subject} as ready for promotion until source freshness improves."
    return "Keep under review. Do not change allocation until the investment driver is clear."


def _detail_next_action(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    candidate_id: str | None,
    symbol: str | None,
    sleeve_name: str | None,
) -> str:
    subject = _subject(symbol, sleeve_name)
    sleeve = _lower_sleeve_exposure(sleeve_name)
    profile = _profile_for_sleeve(sleeve_name)
    if category == "timing":
        peer_scope = f"other {sleeve} candidates" if sleeve != "the sleeve" else "same-sleeve peers"
        return f"Continue review and compare {subject} against {peer_scope} on cost, tracking, liquidity, domicile, and portfolio fit."

    raw_next_action = event.get("next_action")
    explicit = _sanitize_visible_text(raw_next_action)
    if explicit and not _contains_pattern(raw_next_action, _UI_INSTRUCTION_PATTERNS) and not _contains_pattern(explicit, _UI_INSTRUCTION_PATTERNS):
        return explicit
    if category == "decision":
        return profile["next_action"]
    if category == "blocker":
        if _is_upgrade(direction):
            return f"Recheck evidence quality, implementation fit, and same-sleeve peers before treating {subject} as deployable."
        return f"Do not deploy until the blocker clears and the candidate can be compared on implementation quality and sleeve fit."
    if category == "source_evidence":
        return f"Review source quality, document support, and peer comparison before changing allocation priority."
    if category == "portfolio_drift":
        return f"Prepare deployment only if {sleeve} remains off target and the funding path is confirmed."
    if category == "freshness_risk":
        return f"Refresh or verify source evidence before changing {subject}'s review priority."
    if candidate_id:
        return f"Compare against closest peers before changing allocation priority."
    return "Monitor the change until the investment driver is clear enough to act on."


def _detail_reversal(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
) -> str:
    subject = _subject(symbol, sleeve_name)
    sleeve = _lower_sleeve_exposure(sleeve_name)
    profile = _profile_for_sleeve(sleeve_name)
    if category == "timing":
        if direction == "downgrade":
            return f"Renewed timing support, a material {sleeve} sleeve underweight, or a confirmed funding path could reopen the case for promotion."
        if direction == "upgrade":
            return f"Weaker timing support, no sleeve need, or unresolved evidence gaps would keep {subject} from moving closer to deployment."
        return f"A clearer timing signal or a return to the prior support state would soften this change."

    raw_reversal = event.get("what_would_reverse")
    explicit = _sanitize_visible_text(raw_reversal)
    if explicit and not _contains_pattern(raw_reversal, [r"score[- ]?band boundary"]) and not _is_weak_explicit_text(explicit):
        return explicit
    if category == "decision":
        return profile["reversal"]
    if category == "blocker":
        if _is_upgrade(direction):
            return f"A renewed blocker, weaker evidence, or failed peer comparison would remove the promotion case."
        return f"Clearing the blocker and restoring enough evidence quality would reopen investment review."
    if category == "source_evidence":
        return f"Stale source evidence, weaker document support, or unresolved conflicts would block promotion."
    if category == "portfolio_drift":
        return f"A closed funding path or sleeve recovery would remove the deployment case."
    if category == "freshness_risk":
        return f"A fresh source update with no unresolved evidence gaps would restore review confidence."
    return f"A weaker sleeve need, failed peer comparison, or deteriorating implementation quality would lower review priority."


def _detail_summary(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
    previous_state: Any,
    current_state: Any,
    fallback_summary: str,
) -> str:
    subject = _subject(symbol, sleeve_name)
    sleeve = _lower_sleeve_exposure(sleeve_name)
    sleeve_display = _sleeve_exposure(sleeve_name)
    profile = _profile_for_sleeve(sleeve_name)
    event_context = " ".join(
        str(part or "")
        for part in [
            event.get("summary"),
            event.get("implication_summary"),
            event.get("reason_summary"),
            event.get("portfolio_consequence"),
        ]
    ).lower()
    evidence_is_acceptable = bool(
        re.search(r"evidence[^.]{0,80}\b(clean|sufficient|acceptable|complete)\b", event_context)
        or re.search(r"source[^.]{0,80}\b(clean|complete)\b", event_context)
    )
    bundle = _driver_bundle(
        event,
        category=category,
        direction=direction,
        symbol=symbol,
        sleeve_name=sleeve_name,
        current_state=current_state,
    )
    driver_sentence = bundle["driver_sentence"].rstrip(".")

    if category == "timing":
        if direction == "downgrade":
            evidence_clause = (
                "The evidence base remains acceptable, but "
                if evidence_is_acceptable
                else ""
            )
            return (
                f"{driver_sentence}. {evidence_clause}{bundle['candidate_implication']} "
                f"{bundle['allocation_posture']}"
            )
        if direction == "upgrade":
            return (
                f"{driver_sentence}, but no deployment case is confirmed yet. "
                f"{bundle['candidate_implication']} {bundle['allocation_posture']}"
            )
        return (
            f"{driver_sentence}. {bundle['candidate_implication']} {bundle['allocation_posture']}"
        )

    if category == "source_evidence":
        if _is_upgrade(direction):
            return (
                f"{driver_sentence}. {bundle['candidate_implication']} {bundle['allocation_posture']}"
            )
        if _is_downgrade(direction):
            return (
                f"{driver_sentence}. {bundle['candidate_implication']} {bundle['allocation_posture']}"
            )
        return (
            f"{driver_sentence}. {bundle['candidate_implication']} {bundle['allocation_posture']}"
        )
    if category == "portfolio_drift":
        return (
            f"{driver_sentence}. {bundle['candidate_implication']} {bundle['allocation_posture']}"
        )
    if category == "blocker":
        if _is_upgrade(direction):
            return (
                f"{driver_sentence}. {bundle['candidate_implication']} {bundle['allocation_posture']}"
            )
        if _is_downgrade(direction):
            return (
                f"{driver_sentence}. {bundle['candidate_implication']} {bundle['allocation_posture']}"
            )
        return (
            f"{driver_sentence}. {bundle['candidate_implication']} {bundle['allocation_posture']}"
        )
    if category == "decision":
        if _is_upgrade(direction):
            return (
                f"{driver_sentence}. {bundle['candidate_implication']} {bundle['allocation_posture']}"
            )
        if _is_downgrade(direction):
            return (
                f"{driver_sentence}. {bundle['candidate_implication']} {bundle['allocation_posture']}"
            )
        return (
            f"{driver_sentence}. {profile['role']} Review implementation quality and closest peers before changing allocation priority."
        )

    translated = _sanitize_visible_text(fallback_summary)
    if translated and _visible_text_is_clean(translated) and not _is_weak_explicit_text(translated):
        return translated
    return f"{subject} changed review status inside {sleeve_display}, but the ledger does not expose a specific investment driver. Treat this as a review priority change, not a deployment signal."


def _role_fallback(
    role: str,
    *,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
) -> str:
    subject = _subject(symbol, sleeve_name)
    sleeve = _lower_sleeve_exposure(sleeve_name)
    profile = _profile_for_sleeve(sleeve_name)
    if role == "trigger":
        if category == "timing":
            return f"The latest timing refresh changed near term market support for {subject}."
        if category == "portfolio_drift":
            return f"Sleeve positioning or funding priority changed for {sleeve}."
        if category == "source_evidence":
            return f"Evidence quality changed for {subject}."
        if category == "decision":
            return f"The ledger records a review priority change, but does not expose a specific market or portfolio driver."
        return f"The available ledger records a change for {subject}, but does not expose a specific market or portfolio driver."
    if role == "reason":
        if category == "timing":
            return f"Near term market conditions can influence when to review {subject}, but they cannot replace sleeve fit, cost, or evidence."
        if category == "portfolio_drift":
            return "Portfolio drift can change priority only when sleeve need and funding are both clear."
        if category == "source_evidence":
            return "Evidence quality controls whether the comparison can support an investable preference."
        return profile["upgrade_reason"] if _is_upgrade(direction) else profile["downgrade_reason"] if _is_downgrade(direction) else profile["role"]
    if role == "source_evidence":
        if category == "timing":
            return "Market path support changed, but the source packet does not expose a specific driver."
        if category == "source_evidence":
            return f"Source coverage changed for {subject}."
        if category == "portfolio_drift":
            return f"Portfolio overlay changed sleeve position or funding priority for {sleeve}."
        if category == "blocker":
            return f"Policy or evidence review changed blocker status for {subject}."
        return "No fresh source packet supports this movement."
    if role == "portfolio_consequence":
        if category == "timing" and direction == "downgrade":
            return f"Keep {subject} on review; do not deploy on this weaker timing read alone."
        if _is_downgrade(direction):
            return f"Downgrade priority. Keep {subject} under review until the investment case strengthens."
        return "No allocation change yet. Prepare only if sleeve need, funding path, and evidence also support action."
    if role == "next_action":
        if category == "timing":
            return f"Compare {subject} with same-sleeve peers if action is still plausible."
        return profile["next_action"]
    if role == "reversal_condition":
        if category == "timing":
            return "A stronger timing read plus a real sleeve need could reopen the case."
        return profile["reversal"]
    return "Review this change in context before acting."


def _row_fails_quality(
    role: str,
    value: str | None,
    *,
    transition_text: str | None,
    summary: str,
) -> bool:
    text = _clean_text(value)
    if not text:
        return True
    normalized = _norm_for_compare(text)
    if _contains_pattern(text, _BANNED_VISIBLE_PATTERNS):
        return True
    if role == "trigger":
        if _is_state_movement_only(text):
            return True
        if transition_text and _norm_for_compare(transition_text) == normalized:
            return True
        if re.search(r"\bmoved from\b", text, flags=re.IGNORECASE) and not re.search(
            r"\bledger|driver|market|portfolio|evidence|timing|funding|implementation|peer|source|blocker",
            text,
            flags=re.IGNORECASE,
        ):
            return True
    if role == "source_evidence":
        if _is_state_movement_only(text):
            return True
        if _contains_pattern(text, _UI_INSTRUCTION_PATTERNS):
            return True
    if role == "reason":
        if "sleeve question is whether" in normalized:
            return True
        if _norm_for_compare(summary) == normalized:
            return True
    if role == "portfolio_consequence":
        if not _contains_pattern(text, _ACTION_POSTURE_PATTERNS):
            return True
    if role == "next_action":
        if _contains_pattern(text, _UI_INSTRUCTION_PATTERNS):
            return True
    if role == "reversal_condition":
        if "score band" in normalized and not re.search(
            r"\binvestment|sleeve|peer|implementation|evidence|timing|funding|blocker|source\b",
            text,
            flags=re.IGNORECASE,
        ):
            return True
    return False


def _distinct_detail_rows(
    rows: dict[str, str],
    *,
    summary: str,
    transition_text: str | None,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
) -> dict[str, str]:
    distinct: dict[str, str] = {}
    seen = {_norm_for_compare(summary)}
    for role, raw_value in rows.items():
        value = _sanitize_visible_text(raw_value) or _role_fallback(
            role,
            category=category,
            direction=direction,
            symbol=symbol,
            sleeve_name=sleeve_name,
        )
        normalized = _norm_for_compare(value)
        if (
            not normalized
            or normalized in seen
            or _row_fails_quality(role, value, transition_text=transition_text, summary=summary)
        ):
            value = _role_fallback(
                role,
                category=category,
                direction=direction,
                symbol=symbol,
                sleeve_name=sleeve_name,
            )
            normalized = _norm_for_compare(value)
        if normalized in seen:
            value = _role_fallback(
                role,
                category=category,
                direction=direction,
                symbol=symbol,
                sleeve_name=sleeve_name,
            )
            normalized = _norm_for_compare(value)
        distinct[role] = value
        seen.add(normalized)
    return distinct


def _build_change_evidence_packet(
    event: dict[str, Any],
    *,
    category: str,
    direction: str,
    symbol: str | None,
    sleeve_name: str | None,
    source_evidence: str | None,
    trigger: str | None,
    portfolio_consequence: str | None,
    materiality: dict[str, str],
    closure_status: str,
    age_hours: float | None,
    evidence_refs: list[str],
) -> BlueprintChangeEvidencePacket:
    candidate_id = _clean_text(event.get("candidate_id"))
    sleeve_id = _clean_text(event.get("sleeve_id"))
    driver_kind = _driver_kind_for_event(event, category=category)
    portfolio_evidence = portfolio_consequence if category == "portfolio_drift" and _is_specific_evidence(portfolio_consequence) else None
    driver_facts = tuple(
        fact
        for fact in [source_evidence, trigger, portfolio_evidence]
        if _is_specific_evidence(fact)
    )
    render_mode = _render_mode_for(
        materiality_status=materiality["materiality_status"],
        driver_kind=driver_kind,
        source_evidence=source_evidence,
        portfolio_evidence=portfolio_evidence,
    )
    missing_driver_reason = None
    if render_mode in _AUDIT_RENDER_MODES:
        missing_driver_reason = (
            "The prior ledger records a review state change, but the source driver was not preserved."
            if materiality["materiality_status"] in {"unresolved_driver_missing", "raw_movement_only"}
            else "The change lacks enough source or portfolio evidence for a full investor explanation."
        )
    return BlueprintChangeEvidencePacket(
        candidate_id=candidate_id,
        ticker=symbol,
        sleeve_id=sleeve_id,
        sleeve_label=sleeve_name,
        event_time=event.get("changed_at_utc"),
        old_state=event.get("previous_state"),
        new_state=event.get("current_state"),
        driver_kind=driver_kind,
        driver_label=_driver_label(
            event,
            category=category,
            direction=direction,
            symbol=symbol,
            sleeve_name=sleeve_name,
        ),
        driver_facts=driver_facts,
        source_evidence=source_evidence,
        source_provenance=tuple(evidence_refs),
        portfolio_evidence=portfolio_evidence,
        materiality_status=materiality["materiality_status"],
        materiality_reason=materiality["materiality_reason"],
        closure_status=closure_status,
        is_current=None,
        event_age_hours=age_hours,
        source_scan_status=_clean_text(event.get("source_scan_status")),
        render_mode=render_mode,
        missing_driver_reason=missing_driver_reason,
    )


def _compact_audit_summary(packet: BlueprintChangeEvidencePacket) -> str:
    return (
        "Historical review movement. The prior ledger records a review state change, "
        "but the source driver was not preserved. Treat this as audit context, not an investment signal."
    )


def _build_compact_audit_detail(
    *,
    packet: BlueprintChangeEvidencePacket,
    event: dict[str, Any],
    symbol: str | None,
    evidence_refs: list[str],
    report_tab: str | None,
) -> dict[str, Any]:
    candidate_id = packet.candidate_id
    materiality_class = _materiality_class_for(
        materiality_status=packet.materiality_status,
        render_mode=packet.render_mode,
        closure_status=packet.closure_status,
    )
    original_transition = _state_transition_phrase(packet.old_state, packet.new_state)
    audit_detail = {
        "audit_summary": _compact_audit_summary(packet),
        "missing_driver_reason": packet.missing_driver_reason,
        "original_event_type": _clean_text(event.get("event_type")),
        "original_transition": original_transition,
        "grouped_count": None,
    }
    score_delta = _score_delta(event)
    return {
        "event_id": _clean_text(event.get("event_id")) or "",
        "summary": _compact_audit_summary(packet),
        "state_transition": {
            "from": _display_state(packet.old_state),
            "to": _display_state(packet.new_state),
        },
        "driver_kind": packet.driver_kind,
        "driver_label": "Driver unavailable",
        "trigger": None,
        "source_evidence": None,
        "reason": None,
        "portfolio_consequence": None,
        "next_action": None,
        "reversal_condition": None,
        "reversal_conditions": None,
        "closure_status": packet.closure_status,
        "materiality_status": packet.materiality_status,
        "materiality_reason": packet.materiality_reason,
        "materiality_class": materiality_class,
        "render_mode": "grouped_audit",
        "driver_packet": _driver_packet_from_packet(packet),
        "primary_trigger": _primary_trigger_from_packet(packet, display_label=None),
        "candidate_impact": _candidate_impact_from_detail(
            packet,
            direction=str(event.get("direction") or "").strip(),
            detail_rows={
                "reason": None,
                "next_action": None,
                "reversal_condition": None,
            },
            score_delta=score_delta,
        ),
        "audit_detail": audit_detail,
        "missing_driver_reason": packet.missing_driver_reason,
        "is_current": False,
        "event_age_hours": packet.event_age_hours,
        "source_scan_status": packet.source_scan_status,
        "score_delta": score_delta,
        "affected_candidate": {
            "candidate_id": candidate_id,
            "symbol": symbol,
            "sleeve_id": packet.sleeve_id,
        },
        "evidence_refs": evidence_refs,
        "source_freshness": _source_freshness(event, packet.event_time),
        "links": _candidate_links(candidate_id, report_tab),
    }


def _full_investor_detail_fails_quality(detail: dict[str, Any], packet: BlueprintChangeEvidencePacket) -> bool:
    if packet.render_mode not in _FULL_INVESTOR_RENDER_MODES:
        return True
    if packet.materiality_status not in _FULL_INVESTOR_MATERIALITY:
        return True
    if packet.driver_kind == "raw_state_movement_only":
        return True
    summary = _clean_text(detail.get("summary"))
    trigger = _clean_text(detail.get("trigger"))
    source_evidence = _clean_text(detail.get("source_evidence"))
    portfolio_consequence = _clean_text(detail.get("portfolio_consequence"))
    next_action = _clean_text(detail.get("next_action"))
    reversal = _clean_text(detail.get("reversal_condition") or detail.get("reversal_conditions"))
    transition_text = _state_transition_phrase(packet.old_state, packet.new_state)
    visible_text = " ".join(
        text or ""
        for text in [summary, trigger, source_evidence, detail.get("reason"), portfolio_consequence, next_action, reversal]
    )
    if not summary or _is_state_movement_only(summary) or re.match(r"^[A-Z0-9. -]{2,12}\s+moved\b", summary):
        return True
    if not trigger or _is_state_movement_only(trigger) or (transition_text and _norm_for_compare(trigger) == _norm_for_compare(transition_text)):
        return True
    if not _is_specific_evidence(source_evidence):
        return True
    if not portfolio_consequence or not _contains_pattern(portfolio_consequence, _ACTION_POSTURE_PATTERNS):
        return True
    if not next_action or _contains_pattern(next_action, _UI_INSTRUCTION_PATTERNS):
        return True
    if not reversal or ("score band" in _norm_for_compare(reversal) and not re.search(r"\binvestment|sleeve|peer|implementation|evidence|timing|funding|blocker|source\b", reversal, flags=re.IGNORECASE)):
        return True
    return _contains_pattern(visible_text, _BANNED_VISIBLE_PATTERNS)


def _change_detail(
    event: dict[str, Any],
    *,
    surface_id: str,
    category: str,
    direction: str,
    severity: str,
    requires_review: bool,
    symbol: str | None,
    summary: str,
    why_it_matters: str,
    evidence_refs: list[str],
) -> dict[str, Any]:
    candidate_id = _clean_text(event.get("candidate_id"))
    sleeve_id = _clean_text(event.get("sleeve_id"))
    sleeve_name = _clean_text(event.get("sleeve_name"))
    occurred_at = event.get("changed_at_utc")
    previous_state = event.get("previous_state")
    current_state = event.get("current_state")
    report_tab = _clean_text(event.get("report_tab")) or "investment_case"
    detail_summary = _detail_summary(
        event,
        category=category,
        direction=direction,
        symbol=symbol,
        sleeve_name=sleeve_name,
        previous_state=previous_state,
        current_state=current_state,
        fallback_summary=summary,
    )
    source_evidence = _source_evidence_summary(
        event,
        category=category,
        direction=direction,
        symbol=symbol,
        sleeve_name=sleeve_name,
    )
    rows = _distinct_detail_rows(
        {
            "trigger": _detail_trigger(
                event,
                summary=summary,
                category=category,
                direction=direction,
                symbol=symbol,
                sleeve_name=sleeve_name,
                previous_state=previous_state,
                current_state=current_state,
            ),
            "source_evidence": source_evidence,
            "reason": _detail_reason(
                event,
                why_it_matters=why_it_matters,
                category=category,
                direction=direction,
                symbol=symbol,
                sleeve_name=sleeve_name,
            ),
            "portfolio_consequence": _detail_portfolio_consequence(
                event,
                category=category,
                direction=direction,
                symbol=symbol,
                sleeve_name=sleeve_name,
            ),
            "next_action": _detail_next_action(
                event,
                category=category,
                direction=direction,
                candidate_id=candidate_id,
                symbol=symbol,
                sleeve_name=sleeve_name,
            ),
            "reversal_condition": _detail_reversal(
                event,
                category=category,
                direction=direction,
                symbol=symbol,
                sleeve_name=sleeve_name,
            ),
        },
        summary=detail_summary,
        transition_text=_state_transition_phrase(previous_state, current_state),
        category=category,
        direction=direction,
        symbol=symbol,
        sleeve_name=sleeve_name,
    )
    materiality = _materiality_assessment(
        event,
        category=category,
        direction=direction,
        source_evidence=rows["source_evidence"],
        trigger=rows["trigger"],
        portfolio_consequence=rows["portfolio_consequence"],
    )
    closure_status = _closure_status_for(
        materiality_status=materiality["materiality_status"],
        category=category,
        severity=severity,
        requires_review=requires_review,
        portfolio_consequence=rows["portfolio_consequence"],
    )
    age_hours = _event_age_hours(occurred_at)
    packet = _build_change_evidence_packet(
        event,
        category=category,
        direction=direction,
        symbol=symbol,
        sleeve_name=sleeve_name,
        source_evidence=rows["source_evidence"],
        trigger=rows["trigger"],
        portfolio_consequence=rows["portfolio_consequence"],
        materiality=materiality,
        closure_status=closure_status,
        age_hours=age_hours,
        evidence_refs=evidence_refs,
    )
    if packet.render_mode in _AUDIT_RENDER_MODES:
        return _build_compact_audit_detail(
            packet=packet,
            event={**event, "category": category},
            symbol=symbol,
            evidence_refs=evidence_refs,
        report_tab=report_tab,
    )

    score_delta = _score_delta(event)
    detail = {
        "event_id": _clean_text(event.get("event_id")) or "",
        "summary": detail_summary,
        "state_transition": {
            "from": _display_state(previous_state),
            "to": _display_state(current_state),
        },
        "driver_kind": packet.driver_kind,
        "driver_label": _driver_label(
            event,
            category=category,
            direction=direction,
            symbol=symbol,
            sleeve_name=sleeve_name,
        ),
        "trigger": rows["trigger"],
        "source_evidence": rows["source_evidence"],
        "reason": rows["reason"],
        "portfolio_consequence": rows["portfolio_consequence"],
        "next_action": rows["next_action"],
        "reversal_condition": rows["reversal_condition"],
        "reversal_conditions": rows["reversal_condition"],
        "closure_status": closure_status,
        "materiality_status": materiality["materiality_status"],
        "materiality_reason": materiality["materiality_reason"],
        "materiality_class": _materiality_class_for(
            materiality_status=materiality["materiality_status"],
            render_mode=packet.render_mode,
            closure_status=closure_status,
        ),
        "render_mode": packet.render_mode,
        "driver_packet": _driver_packet_from_packet(packet),
        "primary_trigger": _primary_trigger_from_packet(packet, display_label=rows["trigger"]),
        "candidate_impact": _candidate_impact_from_detail(
            packet,
            direction=direction,
            detail_rows=rows,
            score_delta=score_delta,
        ),
        "audit_detail": None,
        "missing_driver_reason": packet.missing_driver_reason,
        "is_current": None,
        "event_age_hours": age_hours,
        "source_scan_status": _clean_text(event.get("source_scan_status")),
        "score_delta": score_delta,
        "affected_candidate": {
            "candidate_id": candidate_id,
            "symbol": symbol,
            "sleeve_id": sleeve_id,
        },
        "evidence_refs": evidence_refs,
        "source_freshness": _source_freshness({**event, "category": category}, occurred_at),
        "links": _candidate_links(candidate_id, report_tab),
    }
    if _full_investor_detail_fails_quality(detail, packet):
        fallback_packet = BlueprintChangeEvidencePacket(
            **{
                **packet.__dict__,
                "render_mode": "grouped_audit",
                "materiality_status": "unresolved_driver_missing"
                if packet.materiality_status in _FULL_INVESTOR_MATERIALITY
                else packet.materiality_status,
                "closure_status": "unresolved_driver_missing",
                "missing_driver_reason": "The change did not pass the source-backed explanation quality gate.",
            }
        )
        return _build_compact_audit_detail(
            packet=fallback_packet,
            event={**event, "category": category},
            symbol=symbol,
            evidence_refs=evidence_refs,
            report_tab=report_tab,
        )
    return detail


def build_decision_read_fields(
    event: dict[str, Any],
    *,
    surface_id: str,
    category: str,
    direction: str,
    severity: str,
    requires_review: bool,
) -> dict[str, Any]:
    occurred_at = event.get("changed_at_utc")
    previous_state = event.get("previous_state")
    current_state = event.get("current_state")
    summary = event.get("summary") or event.get("implication_summary") or "Change recorded."
    why_it_matters = event.get("implication_summary") or event.get("portfolio_consequence") or summary
    symbol = event.get("symbol") or _symbol_from_candidate(event.get("candidate_id"))
    evidence_refs = _evidence_refs(event)
    state_transition = (
        {"from": _display_state(previous_state), "to": _display_state(current_state)}
        if previous_state is not None or current_state is not None
        else None
    )
    detail = _change_detail(
        event,
        surface_id=surface_id,
        category=category,
        direction=direction,
        severity=severity,
        requires_review=requires_review,
        symbol=symbol,
        summary=summary,
        why_it_matters=why_it_matters,
        evidence_refs=evidence_refs,
    )
    return {
        "occurred_at": occurred_at,
        "effective_at": occurred_at,
        "surface": surface_id,
        "category": category,
        "severity": severity,
        "actionability": _actionability(category, severity, requires_review, direction),
        "scope": _scope({**event, "category": category}),
        "confidence": _confidence(event, category),
        "freshness_state": None,
        "symbol": symbol,
        "title": summary,
        "state_transition": state_transition,
        "score_delta": event.get("score_delta"),
        "market_context": event.get("market_context"),
        "portfolio_context": event.get("portfolio_context"),
        "why_it_matters": detail["summary"],
        "next_step": detail["next_action"],
        "evidence_refs": evidence_refs,
        "source_freshness": event.get("source_freshness"),
        "render_mode": detail.get("render_mode"),
        "materiality_status": detail.get("materiality_status"),
        "materiality_class": detail.get("materiality_class"),
        "materiality_reason": detail.get("materiality_reason"),
        "closure_status": detail.get("closure_status"),
        "driver_packet": detail.get("driver_packet"),
        "primary_trigger": detail.get("primary_trigger"),
        "candidate_impact": detail.get("candidate_impact"),
        "audit_detail": detail.get("audit_detail"),
        "missing_driver_reason": detail.get("missing_driver_reason"),
        "change_detail": detail,
    }
