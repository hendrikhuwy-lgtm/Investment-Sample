from __future__ import annotations

import json
from typing import Any


TOP_STRIP_KEYS = (
    "signal",
    "top_strip_summary",
    "top_strip_implication",
    "top_strip_review",
    "top_strip_boundary",
)

CARD_KEYS = (
    "signal",
    "meaning",
    "investment_implication",
    "boundary",
    "review_action",
)

EXPANDED_KEYS = (
    "analyst_synthesis",
    "system_relevance",
    "scenario_if_worsens",
    "scenario_if_stabilizes",
    "scenario_if_reverses",
    "strengthen_read",
    "weaken_read",
)


def _base_signal_payload(fact_pack: dict[str, Any]) -> dict[str, Any]:
    return {
        "signal": {
            "id": fact_pack.get("signal_id"),
            "title": fact_pack.get("signal_title"),
            "family": fact_pack.get("signal_family"),
            "type": fact_pack.get("signal_type"),
            "role": fact_pack.get("signal_role"),
            "observation_date": fact_pack.get("observation_date"),
            "freshness_state": fact_pack.get("freshness_state"),
            "freshness_reason_code": fact_pack.get("freshness_reason_code"),
            "lag_reason": fact_pack.get("lag_reason"),
            "confidence_level": fact_pack.get("confidence_level"),
            "grounding_type": fact_pack.get("grounding_type"),
            "holdings_grounding_mode": fact_pack.get("holdings_grounding_mode"),
            "affected_sleeves": list(fact_pack.get("affected_sleeves") or [])[:4],
            "affected_markets": list(fact_pack.get("affected_markets") or [])[:4],
            "raw_metrics": dict(fact_pack.get("raw_metrics") or {}),
        },
    }


def build_top_strip_prompt_payload(fact_pack: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    return {
        **_base_signal_payload(fact_pack),
        "purpose": "top_strip_synthesis",
        "required_keys": list(TOP_STRIP_KEYS),
        "summary_facts": list(fact_pack.get("signal_summary_facts") or [])[:4],
        "implication_facts": list(fact_pack.get("likely_investment_implication_facts") or [])[:4],
        "boundary_facts": list(fact_pack.get("boundary_facts") or [])[:3],
        "review_action_facts": list(fact_pack.get("review_action_facts") or [])[:3],
        "constraints": {
            "task": "Write only the investor top-strip summary, implication, review line, and boundary line.",
            "brevity": "very concise",
            "tone": "serious, investor-facing, fast to scan",
            "forbidden_claims": dict(schema.get("forbidden_claims") or {}),
        },
    }


def build_card_prompt_payload(fact_pack: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    return {
        **_base_signal_payload(fact_pack),
        "purpose": "card_synthesis",
        "required_keys": list(CARD_KEYS),
        "signal_summary_facts": list(fact_pack.get("signal_summary_facts") or [])[:4],
        "why_it_matters_facts": list(fact_pack.get("why_it_matters_facts") or [])[:4],
        "implication_facts": list(fact_pack.get("likely_investment_implication_facts") or [])[:4],
        "boundary_facts": list(fact_pack.get("boundary_facts") or [])[:3],
        "review_action_facts": list(fact_pack.get("review_action_facts") or [])[:3],
        "constraints": {
            "task": "Write collapsed card synthesis. Meaning and investment implication must not restate each other.",
            "brevity": "concise",
            "tone": "investor-facing and analytical",
            "forbidden_claims": dict(schema.get("forbidden_claims") or {}),
        },
    }


def build_expanded_prompt_payload(fact_pack: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    return {
        **_base_signal_payload(fact_pack),
        "purpose": "expanded_synthesis",
        "required_keys": list(EXPANDED_KEYS),
        "signal_summary_facts": list(fact_pack.get("signal_summary_facts") or [])[:4],
        "why_it_matters_facts": list(fact_pack.get("why_it_matters_facts") or [])[:4],
        "implication_facts": list(fact_pack.get("likely_investment_implication_facts") or [])[:4],
        "benchmark_support_facts": list(fact_pack.get("benchmark_support_facts") or [])[:3],
        "holdings_support_facts": list(fact_pack.get("holdings_support_facts") or [])[:3],
        "uncertainty_facts": list(fact_pack.get("uncertainty_facts") or [])[:4],
        "scenario_branch_facts": dict(fact_pack.get("scenario_branch_facts") or {}),
        "strengthen_read_facts": list(fact_pack.get("strengthen_read_facts") or [])[:3],
        "weaken_read_facts": list(fact_pack.get("weaken_read_facts") or [])[:3],
        "constraints": {
            "task": "Write additive deep-read analysis. Explain mechanism and scenario branches without inventing evidence.",
            "brevity": "moderate",
            "tone": "institutional but plain language",
            "forbidden_claims": dict(schema.get("forbidden_claims") or {}),
        },
    }


def build_prompt_family_bundle(fact_pack: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    return {
        "top_strip": build_top_strip_prompt_payload(fact_pack, schema),
        "card": build_card_prompt_payload(fact_pack, schema),
        "expanded": build_expanded_prompt_payload(fact_pack, schema),
    }


def build_system_prompt() -> str:
    return """You are the synthesis writer for an investor-facing Daily Brief.

Rules:
- Output valid JSON only.
- Use the required keys exactly.
- Use only approved facts from the prompt payload.
- Do not invent any number, date, sleeve, metric, provider, source, holding, or benchmark claim.
- Do not convert sleeve relevance into holdings relevance unless holdings_grounding_mode is live_holding_grounded.
- Use plain investor language and keep an institutional tone.
- Top-strip fields must be sharp and fast to scan.
- Meaning must describe what the signal says about conditions now.
- Investment implication must describe what changes for pacing, review urgency, risk tolerance, or sleeve monitoring.
- Boundary must say what this does not mean yet.
- Expanded synthesis must add analysis and scenario branches, not restate the short fields.
- Do not use internal framework vocabulary or ontology terms.
"""


def build_combined_prompt_payload(fact_pack: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
    families = build_prompt_family_bundle(fact_pack, schema)
    return {
        "required_keys": [
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
        ],
        "families": families,
    }


def build_user_prompt(fact_pack: dict[str, Any], schema: dict[str, Any]) -> str:
    return json.dumps(build_combined_prompt_payload(fact_pack, schema), ensure_ascii=False)


def build_brief_policy_prompt_constraints(
    *,
    brief_policy_mode: str | None = None,
    portfolio_mapping_class: str | None = None,
) -> dict[str, Any]:
    normalized_mode = str(brief_policy_mode or "").strip().lower() or None
    normalized_mapping = str(portfolio_mapping_class or "").strip().lower() or None
    blocked_claims: list[str] = []
    caution_lines: list[str] = []
    if normalized_mapping in {"macro_only", "target_proxy", "sleeve_proxy", "sleeve-proxy"}:
        blocked_claims.append("Do not convert sleeve or macro context into direct holdings language.")
        caution_lines.append("Portfolio consequence must remain sleeve-level or market-level unless direct holdings grounding exists.")
    if normalized_mode in {"monitoring_only", "monitor_only", "bounded"}:
        blocked_claims.append("Do not escalate bounded monitoring context into direct action language.")
        caution_lines.append("Keep the tone analytical and conditional because the run is in bounded monitoring mode.")
    return {
        "brief_policy_mode": normalized_mode,
        "portfolio_mapping_class": normalized_mapping,
        "blocked_claims": blocked_claims,
        "caution_lines": caution_lines,
        "guidance_strength": (
            "bounded"
            if normalized_mode in {"monitoring_only", "monitor_only", "bounded"}
            or normalized_mapping in {"macro_only", "target_proxy", "sleeve_proxy", "sleeve-proxy"}
            else "normal"
        ),
    }
