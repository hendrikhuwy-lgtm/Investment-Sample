from __future__ import annotations

from app.config import Settings
from app.services.daily_brief_explanation_formatter import format_signal_explanation
from app.services.daily_brief_explanation_validator import validate_signal_explanation
from app.services.daily_brief_explanation_writer import write_signal_explanation


def _sample_fact_pack() -> dict:
    return {
        "signal_title": "EM volatility",
        "signal_family": "volatility",
        "signal_type": "cross_asset_stress",
        "signal_role": "dominant_signal",
        "affected_markets": ["emerging_markets", "china"],
        "affected_sleeves": ["emerging_markets", "china_satellite"],
        "raw_metrics": {
            "metric_name": "EM volatility",
            "metric_value": "33.37",
            "metric_delta": 6.02,
            "delta_window": 5,
            "percentile_value": 88.0,
        },
        "metric_name": "EM volatility",
        "metric_value": "33.37",
        "observation_date": "2026-03-09",
        "observed_date": "2026-03-09",
        "freshness_state": "latest_available",
        "freshness_reason_code": "latest_available_source_lag",
        "lag_reason": "source publication cadence is slower than the Daily Brief run cadence",
        "investment_implication_class": "risk pressure",
        "review_action_class": "review_deployment_pacing",
        "boundary_class": "not_long_term_sleeve_rejection",
        "holdings_grounding_mode": "target_proxy",
        "evidence_classification": "sleeve_inferred",
        "confidence_level": "medium",
        "confidence_basis": "fresh enough but benchmark-relative confirmation is incomplete",
        "signal_summary_facts": [
            "EM volatility moved sharply higher over the recent observation window",
        ],
        "why_it_matters_facts": [
            "Rising EM volatility usually means risk tolerance is tightening across more fragile markets",
        ],
        "likely_investment_implication_facts": [
            "Deployment pacing should stay cautious while stress remains elevated",
        ],
        "boundary_facts": [
            "This does not invalidate the long-term EM sleeve role.",
        ],
        "review_action_facts": [
            "Review whether EM pacing should remain conservative.",
        ],
        "benchmark_support_facts": [
            "Benchmark proxy pressure is also elevated.",
        ],
        "holdings_support_facts": [
            "Live holdings are unavailable, so the signal should stay at sleeve level.",
        ],
        "uncertainty_facts": [
            "Benchmark-relative confirmation remains incomplete.",
        ],
        "scenario_branch_facts": {
            "if_worsens": "If this worsens, EM risk should be treated more defensively.",
            "if_stabilizes": "If this stabilizes, current caution may stay contained.",
            "if_reverses": "If this reverses, the current pressure can fall back into background monitoring.",
        },
        "strengthen_read_facts": [
            "A second stress signal confirming the move would strengthen this read.",
        ],
        "weaken_read_facts": [
            "A reversal in volatility and spreads would weaken this read.",
        ],
        "forbidden_claims": {"no_holdings_level_claim": True},
    }


def _sample_schema() -> dict:
    return {
        "output_contract": "signal_synthesis_v2",
        "signal_sentence": {"metric": "EM volatility", "value": "33.37", "date": "2026-03-09", "change": "+6.02 over 5 observations"},
        "top_strip_spec": {"task": "Write one concise investor summary line."},
        "collapsed_card_spec": {"meaning_must_describe": "what the signal says now"},
        "expanded_analysis_spec": {"analyst_synthesis_must_add": "why this matters now"},
        "prompt_constraints": {"plain_investor_language": True},
        "forbidden_claims": {"no_holdings_level_claim": True},
    }


def test_openai_mode_without_api_key_falls_back_deterministically(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    settings = Settings(
        daily_brief_explanation_mode="llm_rewrite_validated",
        daily_brief_llm_provider="openai",
        daily_brief_llm_model="gpt-5-mini",
    )

    result = write_signal_explanation(settings, _sample_fact_pack(), _sample_schema())

    assert result["mode"] == "deterministic_template_only"
    assert result["execution"]["provider"] == "openai"
    assert result["execution"]["llm_attempted"] is True
    assert result["execution"]["llm_succeeded"] is False
    assert result["execution"]["fallback_used"] is True
    assert result["execution"]["failure_reason"] == "missing_api_key"
    assert set(result["parts"]) == {
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


def test_default_settings_prefer_ollama_without_openai_key(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("IA_DAILY_BRIEF_LLM_PROVIDER", raising=False)
    monkeypatch.delenv("IA_DAILY_BRIEF_LLM_MODEL", raising=False)
    monkeypatch.delenv("IA_DAILY_BRIEF_LLM_BASE_URL", raising=False)

    settings = Settings.from_env()

    assert settings.daily_brief_llm_provider == "ollama"
    assert settings.daily_brief_llm_model == "llama3.1:8b"
    assert settings.daily_brief_llm_base_url == "http://127.0.0.1:11434"


def test_ollama_mode_uses_local_provider_without_openai_key(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    def fake_post(url: str, body: dict, headers: dict, timeout: int = 20) -> tuple[dict, None]:
        assert url == "http://127.0.0.1:11434/api/generate"
        assert body["model"] == "llama3.1:8b"
        return (
            {
                "response": (
                    '{"signal":"EM volatility is elevated on 2026-03-09.",'
                    '"meaning":"Near-term EM stress is higher.",'
                    '"investment_implication":"Deployment pacing should stay cautious.",'
                    '"boundary":"This does not invalidate the long-term EM sleeve role.",'
                    '"review_action":"Review whether EM pacing should remain conservative.",'
                    '"analyst_synthesis":"EM stress is rising fast enough to matter now, even though the read is still sleeve-level rather than holdings-specific.",'
                    '"system_relevance":"This matters most for EM and China sleeves because it raises the bar for near-term deployment confidence.",'
                    '"scenario_if_worsens":"If this worsens, EM risk should be treated more defensively.",'
                    '"scenario_if_stabilizes":"If this stabilizes, current caution may stay contained.",'
                    '"scenario_if_reverses":"If this reverses, the pressure can move back into background monitoring.",'
                    '"strengthen_read":"A second stress signal confirming the move would strengthen this read.",'
                    '"weaken_read":"A reversal in volatility and spreads would weaken this read.",'
                    '"top_strip_summary":"EM volatility is the clearest market stress signal in today\\u2019s brief.",'
                    '"top_strip_implication":"The current implication is slower EM deployment, not a thesis break.",'
                    '"top_strip_review":"Review whether EM pacing should stay cautious.",'
                    '"top_strip_boundary":"This still does not invalidate the long-term EM sleeve role."}'
                )
            },
            None,
        )

    monkeypatch.setattr(
        "app.services.daily_brief_explanation_writer._post_with_reason",
        fake_post,
    )
    settings = Settings(
        daily_brief_explanation_mode="llm_rewrite_validated",
        daily_brief_llm_provider="ollama",
        daily_brief_llm_base_url="http://127.0.0.1:11434",
        daily_brief_llm_model="llama3.1:8b",
    )

    result = write_signal_explanation(settings, _sample_fact_pack(), _sample_schema())

    assert result["mode"] == "llm_rewrite_validated:ollama"
    assert result["parts"]["meaning"] == "Near-term EM stress is higher."
    assert result["parts"]["analyst_synthesis"].startswith("EM stress is rising")
    assert result["execution"]["section_name"] == "unknown_section"
    assert result["execution"]["attempt_count"] == 1
    assert result["execution"]["llm_attempted"] is True
    assert result["execution"]["llm_succeeded"] is True
    assert result["execution"]["fallback_used"] is False


def test_high_priority_retry_records_attempt_count(monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    calls = {"count": 0}

    def flaky_post(url: str, body: dict, headers: dict, timeout: int = 20) -> tuple[dict | None, str | None]:
        calls["count"] += 1
        if calls["count"] == 1:
            return None, "timeout"
        return (
            {
                "response": (
                    '{"signal":"EM volatility is elevated on 2026-03-09.",'
                    '"meaning":"Near-term EM stress is higher.",'
                    '"investment_implication":"Deployment pacing should stay cautious.",'
                    '"boundary":"This does not invalidate the long-term EM sleeve role.",'
                    '"review_action":"Review whether EM pacing should remain conservative.",'
                    '"analyst_synthesis":"EM stress is rising fast enough to matter now, even though the read is still sleeve-level rather than holdings-specific.",'
                    '"system_relevance":"This matters most for EM and China sleeves because it raises the bar for near-term deployment confidence.",'
                    '"scenario_if_worsens":"If this worsens, EM risk should be treated more defensively.",'
                    '"scenario_if_stabilizes":"If this stabilizes, current caution may stay contained.",'
                    '"scenario_if_reverses":"If this reverses, the pressure can move back into background monitoring.",'
                    '"strengthen_read":"A second stress signal confirming the move would strengthen this read.",'
                    '"weaken_read":"A reversal in volatility and spreads would weaken this read.",'
                    '"top_strip_summary":"EM volatility is the clearest market stress signal in today\\u2019s brief.",'
                    '"top_strip_implication":"The current implication is slower EM deployment, not a thesis break.",'
                    '"top_strip_review":"Review whether EM pacing should stay cautious.",'
                    '"top_strip_boundary":"This still does not invalidate the long-term EM sleeve role."}'
                )
            },
            None,
        )

    monkeypatch.setattr(
        "app.services.daily_brief_explanation_writer._post_with_reason",
        flaky_post,
    )
    settings = Settings(
        daily_brief_explanation_mode="llm_rewrite_validated",
        daily_brief_llm_provider="ollama",
        daily_brief_llm_base_url="http://127.0.0.1:11434",
        daily_brief_llm_model="llama3.1:8b",
    )

    result = write_signal_explanation(
        settings,
        _sample_fact_pack(),
        _sample_schema(),
        section_name="top_developments",
        priority="top",
    )

    assert result["mode"] == "llm_rewrite_validated:ollama"
    assert result["execution"]["attempt_count"] == 2
    assert result["execution"]["section_name"] == "top_developments"
    assert result["execution"]["llm_succeeded"] is True
    assert result["execution"]["fallback_used"] is False


def test_deterministic_template_emits_expanded_keys() -> None:
    parts = format_signal_explanation(_sample_fact_pack(), _sample_schema())
    assert parts["analyst_synthesis"]
    assert parts["system_relevance"]
    assert parts["scenario_if_worsens"]
    assert parts["top_strip_summary"]


def test_validator_rejects_duplicate_meaning_and_implication() -> None:
    generated = format_signal_explanation(_sample_fact_pack(), _sample_schema())
    generated["investment_implication"] = generated["meaning"]

    result = validate_signal_explanation(_sample_fact_pack(), _sample_schema(), generated)

    assert result["status"] == "fail_fallback_to_template"
    assert result["reason"] == "meaning_implication_duplicate"
