from __future__ import annotations

from typing import Any


def build_footnote(template_key: str, evidence_item: dict[str, Any], fact_pack: dict[str, Any]) -> str:
    key = str(template_key or "observed_reading")
    observed_date = str(evidence_item.get("observed_date") or fact_pack.get("observed_date") or "the latest observation")
    evidence_type = str(evidence_item.get("evidence_type") or "direct")
    freshness = str(evidence_item.get("freshness_state") or fact_pack.get("freshness_state") or "unknown")
    if key == "observed_reading":
        return f"This is the market reading used in the brief, dated {observed_date}."
    if key == "recent_change":
        return "This shows whether the move is still building, fading, or stabilizing across the recent observation window."
    if key == "high_percentile":
        return "A high percentile means the reading is elevated relative to its own recent range, which increases the chance that the move is decision-relevant rather than background noise."
    if key == "percentile_general":
        return "Percentile compares the reading with its own recent history to show how unusual the current level is."
    if key == "latest_available":
        return f"This is the latest available reading. It is still being used because the issue remains unresolved even though the data is not same-day."
    if key == "sleeve_proxy_only":
        return "This is sleeve-level relevance only. It is not a confirmed holdings-level effect because live holdings are unavailable or not directly mappable."
    if key == "holdings_unavailable":
        return "Holdings-specific effects cannot be confirmed today, so the brief is relying on sleeve relevance instead."
    if key == "holdings_direct":
        return "This evidence is grounded in current mapped holdings rather than only target-sleeve design."
    if key == "benchmark_context_only":
        return "This is benchmark context. It explains what the benchmark comparison adds here, not a separate investment conclusion."
    if key == "local_context_relevance":
        return "This local or regional context matters because it changes how a Singapore-based investor should read the main signal."
    if freshness in {"stale", "lagged"}:
        return f"This evidence is lagged relative to today, but it remains relevant because the condition is still unresolved."
    if evidence_type == "benchmark":
        return "This evidence belongs to the benchmark layer rather than a direct sleeve or holdings effect."
    return f"This is supporting evidence used in the current brief, dated {observed_date}."


def build_signal_evidence_block(fact_pack: dict[str, Any], *, enable_footnotes: bool = True) -> dict[str, Any]:
    bullets: list[dict[str, Any]] = []
    footnotes: list[dict[str, Any]] = []
    for index, item in enumerate(list(fact_pack.get("evidence_items") or []), start=1):
        evidence = dict(item)
        observed_date = str(evidence.get("observed_date") or "").strip()
        bullet_text = str(evidence.get("value_text") or "").strip()
        if observed_date and observed_date not in bullet_text:
            bullet_text = f"{bullet_text}. Date used: {observed_date}." if bullet_text else f"Date used: {observed_date}."
        bullet = {
            "evidence_id": evidence.get("evidence_id"),
            "label": evidence.get("label"),
            "text": bullet_text,
            "footnote_number": index if enable_footnotes else None,
        }
        bullets.append(bullet)
        if enable_footnotes:
            footnotes.append(
                {
                    "number": index,
                    "text": build_footnote(str(evidence.get("footnote_template_key") or "observed_reading"), evidence, fact_pack),
                }
            )
    return {
        "heading": "Evidence",
        "bullets": bullets,
        "footnotes": footnotes,
    }
