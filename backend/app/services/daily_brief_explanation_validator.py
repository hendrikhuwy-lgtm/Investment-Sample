from __future__ import annotations

import re
from typing import Any


DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
NUMBER_RE = re.compile(r"(?<!\w)(?:\+|-)?\d+(?:\.\d+)?(?:th)?(?!\w)")
FORBIDDEN_PHRASE_MAP = {
    "stress-transmission evidence first": "near-term stress signal",
    "target-sleeve proxy": "target sleeve relevance is used because live holdings are unavailable",
    "support family": "supporting signal group",
    "evidence basis": "current evidence used",
    "current enough": "recent enough",
    "policy dependence reduces authority": "policy assumptions limit how strongly this can be framed",
    "benchmark-relative interpretation becomes less clean": "benchmark comparison is less clear under current conditions",
    "routing unavailable": "holdings-specific routing is unavailable today",
    "optional holdings effect": "holdings context",
}
REQUIRED_KEYS = [
    "signal",
    "meaning",
    "investment_implication",
    "boundary",
    "review_action",
    "analyst_synthesis",
    "system_relevance",
    "scenario_if_worsens",
    "scenario_if_stabilizes",
    "scenario_if_reverses",
    "strengthen_read",
    "weaken_read",
    "top_strip_summary",
    "top_strip_implication",
    "top_strip_review",
    "top_strip_boundary",
]
INTERNAL_VOCAB = {
    "ontology",
    "framework vocabulary",
    "support family",
    "target-sleeve proxy",
    "policy dependence reduces authority",
}


def _allowed_numbers(fact_pack: dict[str, Any]) -> set[str]:
    allowed: set[str] = set()
    raw_metrics = dict(fact_pack.get("raw_metrics") or {})
    metric_value = raw_metrics.get("metric_value")
    try:
        if metric_value not in {None, ""}:
            numeric = float(metric_value)
            allowed.add(f"{numeric:+.2f}")
            allowed.add(f"{numeric:.2f}")
            allowed.add(f"{numeric:.0f}")
    except Exception:
        pass
    for key in ("metric_delta", "percentile_value"):
        value = raw_metrics.get(key)
        if value is None:
            continue
        try:
            numeric = float(value)
            allowed.add(f"{numeric:+.2f}")
            allowed.add(f"{numeric:.2f}")
            allowed.add(f"{numeric:.0f}")
        except Exception:
            pass
    for item in list(fact_pack.get("evidence_items") or []):
        text = str(dict(item).get("value_text") or "")
        for match in NUMBER_RE.findall(text):
            allowed.add(match)
    for date in _allowed_dates(fact_pack):
        for piece in str(date).split("-"):
            if piece:
                allowed.add(piece)
    return allowed


def _allowed_dates(fact_pack: dict[str, Any]) -> set[str]:
    allowed = set()
    if fact_pack.get("observed_date"):
        allowed.add(str(fact_pack.get("observed_date")))
    for item in list(fact_pack.get("evidence_items") or []):
        observed = dict(item).get("observed_date")
        if observed:
            allowed.add(str(observed))
    return allowed


def _soft_cleanup(parts: dict[str, str]) -> dict[str, str]:
    cleaned = {}
    for key, value in parts.items():
        text = " ".join(str(value or "").split()).strip()
        for bad, replacement in FORBIDDEN_PHRASE_MAP.items():
            text = re.sub(re.escape(bad), replacement, text, flags=re.IGNORECASE)
        cleaned[key] = text
    return cleaned


def _normalize_text(text: str) -> set[str]:
    normalized = re.sub(r"[^a-z0-9\s]", " ", str(text or "").lower())
    return {token for token in normalized.split() if token and token not in {"the", "and", "for", "with", "this", "that", "into", "from", "will", "remain", "today"}}


def _too_similar(left: str, right: str) -> bool:
    a = _normalize_text(left)
    b = _normalize_text(right)
    if not a or not b:
        return False
    overlap = len(a & b) / max(1, min(len(a), len(b)))
    return overlap >= 0.8


def validate_signal_explanation(
    fact_pack: dict[str, Any],
    schema: dict[str, Any],
    generated: dict[str, str],
    *,
    strict: bool = True,
) -> dict[str, Any]:
    if any(not str(generated.get(key) or "").strip() for key in REQUIRED_KEYS):
        return {"status": "fail_fallback_to_template", "reason": "missing_required_parts", "parts": generated}
    parts = _soft_cleanup(generated)
    text_blob = " ".join(parts.values())
    allowed_dates = _allowed_dates(fact_pack)
    for date in DATE_RE.findall(text_blob):
        if date not in allowed_dates:
            return {"status": "fail_fallback_to_template", "reason": f"unsupported_date:{date}", "parts": parts}
    allowed_numbers = _allowed_numbers(fact_pack)
    for number in NUMBER_RE.findall(text_blob):
        if number.endswith("th"):
            continue
        if allowed_numbers and number not in allowed_numbers and number not in {"5"}:
            return {"status": "fail_fallback_to_template", "reason": f"unsupported_number:{number}", "parts": parts}
    if "holdings" in text_blob.lower() and str(fact_pack.get("holdings_grounding_mode") or "") != "live_holding_grounded":
        if "unavailable" not in text_blob.lower() and "sleeve-level" not in text_blob.lower() and strict:
            return {"status": "fail_fallback_to_template", "reason": "unsupported_holdings_claim", "parts": parts}
    forbidden = list(dict(schema.get("forbidden_claims") or {}).get("forbidden_phrases") or [])
    for phrase in forbidden:
        if phrase and phrase.lower() in text_blob.lower():
            return {"status": "fail_fallback_to_template", "reason": f"forbidden_phrase:{phrase}", "parts": parts}
    for phrase in INTERNAL_VOCAB:
        if phrase in text_blob.lower():
            return {"status": "fail_fallback_to_template", "reason": f"internal_vocabulary:{phrase}", "parts": parts}
    if _too_similar(parts["meaning"], parts["investment_implication"]):
        return {"status": "fail_fallback_to_template", "reason": "meaning_implication_duplicate", "parts": parts}
    expanded_tokens = _normalize_text(parts["analyst_synthesis"]) | _normalize_text(parts["system_relevance"])
    collapsed_tokens = _normalize_text(parts["meaning"]) | _normalize_text(parts["investment_implication"])
    if expanded_tokens and collapsed_tokens and len(expanded_tokens - collapsed_tokens) < 2:
        return {"status": "fail_fallback_to_template", "reason": "expanded_not_additive", "parts": parts}
    freshness_state = str(fact_pack.get("freshness_state") or "").lower()
    if freshness_state == "stale_excluded" and (parts["top_strip_summary"] or parts["top_strip_implication"]):
        return {"status": "fail_fallback_to_template", "reason": "stale_excluded_promoted", "parts": parts}
    if "boundary" not in parts["boundary"].lower() and not parts["boundary"].strip().endswith("."):
        return {"status": "fail_fallback_to_template", "reason": "missing_boundary_statement", "parts": parts}
    soft = parts != generated
    return {"status": "pass_with_soft_cleanup" if soft else "pass", "reason": "ok", "parts": parts}
