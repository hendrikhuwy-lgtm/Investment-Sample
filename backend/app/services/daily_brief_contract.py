from __future__ import annotations

from typing import Any


DAILY_BRIEF_CONTRACT_VERSION = "daily_brief_synthesis_v3"

REQUIRED_SYNTHESIS_FIELDS = (
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
)


def _missing_fields(parts: dict[str, Any] | None, required: tuple[str, ...] = REQUIRED_SYNTHESIS_FIELDS) -> list[str]:
    payload = dict(parts or {})
    return [field for field in required if not str(payload.get(field) or "").strip()]


def build_reader_contract_snapshot(reader_payload: dict[str, Any]) -> dict[str, Any]:
    top_developments = [dict(item) for item in list(reader_payload.get("top_developments") or []) if isinstance(item, dict)]
    top_cards = []
    for item in top_developments[:3]:
        top_cards.append(
            {
                "metric_code": str(item.get("metric_code") or ""),
                "title": str(item.get("title") or ""),
                "freshness_reason_code": str(item.get("freshness_reason_code") or ""),
                "investor_explanation": dict(item.get("investor_explanation") or {}),
            }
        )
    return {
        "brief_contract_version": DAILY_BRIEF_CONTRACT_VERSION,
        "generated_at": reader_payload.get("generated_at"),
        "top_strip": dict((reader_payload.get("daily_conclusion_block") or {}).get("investor_explanation") or {}),
        "top_developments": top_cards,
        "what_deserves_review_now": dict(reader_payload.get("what_deserves_review_now") or {}),
        "what_stays_in_background": dict(reader_payload.get("what_stays_in_background") or {}),
    }


def verify_reader_contract_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    top_strip = dict(snapshot.get("top_strip") or {})
    top_developments = [dict(item) for item in list(snapshot.get("top_developments") or []) if isinstance(item, dict)]
    review_now = dict(snapshot.get("what_deserves_review_now") or {})
    background = dict(snapshot.get("what_stays_in_background") or {})
    failures: list[str] = []

    if str(snapshot.get("brief_contract_version") or "") != DAILY_BRIEF_CONTRACT_VERSION:
        failures.append("contract_version_mismatch")

    top_strip_missing = _missing_fields(top_strip, ("top_strip_summary", "top_strip_implication", "top_strip_review", "top_strip_boundary"))
    if top_strip_missing:
        failures.append(f"missing_top_strip_fields:{','.join(top_strip_missing)}")

    if not top_developments:
        failures.append("missing_top_developments")
    else:
        missing_expanded = _missing_fields(dict(top_developments[0].get("investor_explanation") or {}))
        if missing_expanded:
            failures.append(f"missing_expanded_fields:{','.join(missing_expanded)}")

    if not str(review_now.get("summary") or "").strip():
        failures.append("missing_review_now_summary")
    if not str(background.get("summary") or "").strip():
        failures.append("missing_background_summary")

    return {
        "status": "ok" if not failures else "failed_contract_compliance",
        "contract_version": DAILY_BRIEF_CONTRACT_VERSION,
        "failures": failures,
        "required_field_count": len(REQUIRED_SYNTHESIS_FIELDS),
        "top_development_count": len(top_developments),
    }

