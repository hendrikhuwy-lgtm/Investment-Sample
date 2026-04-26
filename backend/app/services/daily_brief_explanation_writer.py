from __future__ import annotations

import json
import os
import re
import time
from typing import Any
from urllib import error, request
from urllib.parse import urljoin

from app.config import Settings
from app.services.daily_brief_explanation_formatter import format_signal_explanation
from app.services.daily_brief_prompt_builders import build_system_prompt, build_user_prompt

EXPECTED_KEYS = {
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
}


def _extract_json_response(raw: dict[str, Any]) -> dict[str, str] | None:
    try:
        for item in raw.get("output", []):
            for content in item.get("content", []):
                if content.get("type") == "output_text":
                    value = json.loads(content.get("text") or "{}")
                    return value if isinstance(value, dict) else None
    except Exception:
        return None
    return None


def _extract_first_json_object(text: str) -> dict[str, str] | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    try:
        value = json.loads(raw)
        return value if isinstance(value, dict) else None
    except json.JSONDecodeError:
        pass
    match = re.search(r"\{.*\}", raw, flags=re.DOTALL)
    if not match:
        return None
    try:
        value = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _compact_payload(fact_pack: dict[str, Any], schema: dict[str, Any]) -> dict[str, Any]:
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
            "affected_sleeves": list(fact_pack.get("affected_sleeves") or [])[:4],
            "affected_markets": list(fact_pack.get("affected_markets") or [])[:4],
            "raw_metrics": dict(fact_pack.get("raw_metrics") or {}),
            "confidence_level": fact_pack.get("confidence_level"),
            "confidence_basis": fact_pack.get("confidence_basis"),
            "grounding_type": fact_pack.get("grounding_type"),
            "holdings_grounding_mode": fact_pack.get("holdings_grounding_mode"),
            "evidence_classification": fact_pack.get("evidence_classification"),
        },
        "facts": {
            "signal_summary_facts": list(fact_pack.get("signal_summary_facts") or [])[:4],
            "why_it_matters_facts": list(fact_pack.get("why_it_matters_facts") or [])[:4],
            "likely_investment_implication_facts": list(fact_pack.get("likely_investment_implication_facts") or [])[:4],
            "boundary_facts": list(fact_pack.get("boundary_facts") or [])[:3],
            "review_action_facts": list(fact_pack.get("review_action_facts") or [])[:3],
            "benchmark_support_facts": list(fact_pack.get("benchmark_support_facts") or [])[:3],
            "holdings_support_facts": list(fact_pack.get("holdings_support_facts") or [])[:3],
            "uncertainty_facts": list(fact_pack.get("uncertainty_facts") or [])[:4],
            "scenario_branch_facts": dict(fact_pack.get("scenario_branch_facts") or {}),
            "strengthen_read_facts": list(fact_pack.get("strengthen_read_facts") or [])[:3],
            "weaken_read_facts": list(fact_pack.get("weaken_read_facts") or [])[:3],
        },
        "schema": {
            "top_strip_spec": dict(schema.get("top_strip_spec") or {}),
            "collapsed_card_spec": dict(schema.get("collapsed_card_spec") or {}),
            "expanded_analysis_spec": dict(schema.get("expanded_analysis_spec") or {}),
            "prompt_constraints": dict(schema.get("prompt_constraints") or {}),
            "forbidden_claims": dict(schema.get("forbidden_claims") or {}),
        },
    }


def _result(
    parts: dict[str, str],
    *,
    mode: str,
    attempted: bool,
    succeeded: bool,
    reason: str,
    latency_ms: int,
    provider: str,
    model: str,
    validator_ready: bool = True,
    section_name: str = "unknown_section",
    priority: str = "normal",
    attempt_count: int = 1,
) -> dict[str, Any]:
    return {
        "mode": mode,
        "parts": parts,
        "execution": {
            "provider": provider,
            "model": model,
            "section_name": section_name,
            "priority": priority,
            "llm_attempted": attempted,
            "llm_succeeded": succeeded,
            "validator_ready": validator_ready,
            "fallback_used": mode == "deterministic_template_only",
            "failure_reason": reason if not succeeded else "",
            "latency_ms": latency_ms,
            "attempt_count": attempt_count,
        },
    }


def _post_with_reason(url: str, body: dict[str, Any], headers: dict[str, str], timeout: int) -> tuple[dict[str, Any] | None, str | None]:
    req = request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8")), None
    except error.HTTPError as exc:
        return None, f"http_{exc.code}"
    except error.URLError:
        return None, "connection_error"
    except TimeoutError:
        return None, "timeout"
    except json.JSONDecodeError:
        return None, "parse_error"


def _call_openai(base_url: str, model: str, payload: dict[str, Any], *, timeout: int) -> tuple[dict[str, str] | None, str | None]:
    api_key = os.getenv("OPENAI_API_KEY", "").strip()
    if not api_key:
        return None, "missing_api_key"
    body = {
        "model": model,
        "input": [
            {"role": "system", "content": build_system_prompt()},
            {"role": "user", "content": build_user_prompt(payload["fact_pack"], payload["schema"])},
        ],
        "text": {
            "format": {
                "type": "json_schema",
                "name": "daily_brief_explanation",
                "schema": {
                    "type": "object",
                    "properties": {key: {"type": "string"} for key in EXPECTED_KEYS},
                    "required": sorted(EXPECTED_KEYS),
                    "additionalProperties": False,
                },
            }
        },
    }
    raw, error_reason = _post_with_reason(
        urljoin(base_url.rstrip("/") + "/", "v1/responses"),
        body,
        {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        timeout=timeout,
    )
    if not raw:
        return None, error_reason or "no_response"
    parsed = _extract_json_response(raw)
    return (parsed, None) if parsed else (None, "parse_error")


def _call_ollama(base_url: str, model: str, payload: dict[str, Any], *, timeout: int) -> tuple[dict[str, str] | None, str | None]:
    body = {
        "model": model,
        "prompt": f"{build_system_prompt()}\nRespond with one JSON object only.\n\nPayload:\n{build_user_prompt(payload['fact_pack'], payload['schema'])}",
        "format": "json",
        "stream": False,
        "options": {
            "temperature": 0.1,
            "num_predict": 700,
        },
    }
    raw, error_reason = _post_with_reason(
        urljoin(base_url.rstrip("/") + "/", "api/generate"),
        body,
        {
            "Content-Type": "application/json",
        },
        timeout=timeout,
    )
    if not raw:
        return None, error_reason or "no_response"
    parsed = _extract_first_json_object(str(raw.get("response") or ""))
    if not parsed:
        return None, "parse_error"
    missing = EXPECTED_KEYS - set(parsed)
    if missing:
        return None, "missing_required_keys"
    return {key: str(parsed.get(key) or "") for key in EXPECTED_KEYS}, None


def _call_provider(settings: Settings, payload: dict[str, Any]) -> tuple[dict[str, str] | None, str | None]:
    provider = str(settings.daily_brief_llm_provider or "openai").strip().lower()
    base_url = str(settings.daily_brief_llm_base_url or "").strip()
    timeout = int(settings.daily_brief_llm_timeout_seconds or 30)
    if provider == "ollama":
        if not base_url:
            return None, "missing_base_url"
        return _call_ollama(base_url, settings.daily_brief_llm_model, payload, timeout=timeout)
    openai_base = base_url or "https://api.openai.com"
    return _call_openai(openai_base, settings.daily_brief_llm_model, payload, timeout=timeout)


def write_signal_explanation(
    settings: Settings,
    fact_pack: dict[str, Any],
    schema: dict[str, Any],
    *,
    section_name: str = "unknown_section",
    priority: str = "normal",
    policy_constraints: dict[str, Any] | None = None,
) -> dict[str, Any]:
    deterministic = format_signal_explanation(fact_pack, schema)
    mode = str(settings.daily_brief_explanation_mode or "deterministic_template_only")
    provider = str(settings.daily_brief_llm_provider or "ollama").strip().lower()
    model = str(settings.daily_brief_llm_model or "")
    if mode != "llm_rewrite_validated":
        return _result(
            deterministic,
            mode="deterministic_template_only",
            attempted=False,
            succeeded=False,
            reason="llm_mode_disabled",
            latency_ms=0,
            provider=provider,
            model=model,
            section_name=section_name,
            priority=priority,
        )
    payload = {
        "fact_pack": fact_pack,
        "schema": schema,
        "compact_payload": _compact_payload(fact_pack, schema),
        "policy_constraints": dict(policy_constraints or {}),
    }
    started = time.perf_counter()
    llm_parts, error_reason = _call_provider(settings, payload)
    attempt_count = 1
    retryable_reasons = {
        "timeout",
        "connection_error",
        "parse_error",
        "missing_required_keys",
        "http_500",
        "http_502",
        "http_503",
        "http_504",
    }
    if not llm_parts and priority in {"top", "high", "critical"} and str(error_reason or "") in retryable_reasons:
        attempt_count += 1
        llm_parts, error_reason = _call_provider(settings, payload)
    latency_ms = int(round((time.perf_counter() - started) * 1000))
    if not llm_parts:
        return _result(
            deterministic,
            mode="deterministic_template_only",
            attempted=True,
            succeeded=False,
            reason=error_reason or "provider_failed",
            latency_ms=latency_ms,
            provider=provider,
            model=model,
            section_name=section_name,
            priority=priority,
            attempt_count=attempt_count,
        )
    return _result(
        {key: str(llm_parts.get(key) or "") for key in EXPECTED_KEYS},
        mode=f"llm_rewrite_validated:{provider}",
        attempted=True,
        succeeded=True,
        reason="",
        latency_ms=latency_ms,
        provider=provider,
        model=model,
        section_name=section_name,
        priority=priority,
        attempt_count=attempt_count,
    )
