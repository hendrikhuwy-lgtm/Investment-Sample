from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.env_loader import load_local_env


DEFAULT_MCP_PRIORITY_SERVERS = ",".join(
    [
        "ai.auteng/docs",
        "ai.auteng/mcp",
        "ai.com.mcp/contabo",
        "ai.com.mcp/registry",
        "ai.com.mcp/openai-tools",
        # Removed: ai.com.mcp/petstore (test fixture, not production data)
        # Removed: ai.com.mcp/lenny-rachitsky-podcast (subjective commentary, noise)
    ]
)
DEFAULT_DB_RELATIVE_PATH = "storage/investment_agent.sqlite3"
REPO_ROOT = Path(__file__).resolve().parents[2]
load_local_env(REPO_ROOT)
CANONICAL_DB_PATH = REPO_ROOT / DEFAULT_DB_RELATIVE_PATH
LEGACY_DB_PATH = REPO_ROOT / "backend" / "storage" / "investment_agent.sqlite3"
VERSION_CONTRACT_PATH = REPO_ROOT / "ops" / "version.json"
DEFAULT_VERSION_CONTRACT: dict[str, Any] = {
    "build_id": "dev",
    "api_version": 1,
    "backend_version": "0.1.0",
    "frontend_version": "0.1.0",
}


def get_repo_root() -> Path:
    return REPO_ROOT


def load_version_contract() -> dict[str, Any]:
    payload = dict(DEFAULT_VERSION_CONTRACT)
    path = VERSION_CONTRACT_PATH
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                payload.update(raw)
        except json.JSONDecodeError:
            pass

    try:
        payload["api_version"] = int(payload.get("api_version", 1))
    except Exception:  # noqa: BLE001
        payload["api_version"] = 1
    payload["build_id"] = str(payload.get("build_id", "dev"))
    payload["backend_version"] = str(payload.get("backend_version", "0.1.0"))
    payload["frontend_version"] = str(payload.get("frontend_version", "0.1.0"))
    return payload


def get_db_path(settings: Settings | None = None) -> Path:
    raw_env = os.getenv("IA_DB_PATH", "").strip()
    if raw_env:
        path = Path(raw_env).expanduser()
    elif settings is not None and settings.db_path != DEFAULT_DB_RELATIVE_PATH:
        path = Path(settings.db_path).expanduser()
    else:
        path = CANONICAL_DB_PATH

    if not path.is_absolute():
        path = REPO_ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def get_legacy_db_path() -> Path:
    return LEGACY_DB_PATH


@dataclass(frozen=True)
class Settings:
    app_name: str = "investment-agent"
    db_path: str = DEFAULT_DB_RELATIVE_PATH
    smtp_host: str = "smtp.example.com"
    smtp_port: int = 587
    smtp_user: str = ""
    smtp_password: str = ""
    alert_from: str = "alerts@example.com"
    alert_to: str = "investor@example.com"
    web_timeout_seconds: int = 12
    critical_email_cooldown_minutes: int = 30
    mcp_timeout_seconds: int = 4
    mcp_max_retries: int = 1
    mcp_live_required: bool = True
    mcp_min_success_ratio: float = 0.70
    mcp_min_success_count: int = 10
    mcp_connect_timeout_seconds: int = 8
    mcp_read_timeout_seconds: int = 12
    mcp_max_workers: int = 12
    mcp_server_budget_seconds: float = 15.0
    http_proxy: str = ""
    https_proxy: str = ""
    no_proxy: str = ""
    proxy_mode: str = "auto"
    mcp_registry_snapshot_path: str = "mcp/registry_snapshot.json"
    mcp_priority_mode: str = "prioritized_only"
    mcp_priority_servers: str = DEFAULT_MCP_PRIORITY_SERVERS
    mcp_connectors_candidates_path: str = "mcp/connectors/financial_intelligence_candidates.json"
    mcp_registry_priorities_path: str = "mcp/connectors/current_registry_priorities.json"
    refresh_live_cache_on_brief: bool = True
    daily_brief_mcp_live_required: bool = True
    platform_auto_refresh_daily_log: bool = True
    platform_daily_log_max_age_hours: int = 24
    build_id: str = "dev"
    git_sha: str = ""
    api_version: int = 1
    tls_ca_bundle: str = ""
    tls_verify: bool = True
    auto_daily_brief_enabled: bool = True
    auto_daily_brief_interval_minutes: int = 15
    auto_daily_brief_run_hour_china: int = 8
    auto_daily_brief_second_run_hour_china: int = 20
    auto_daily_brief_send_email: bool = False
    auto_daily_brief_force_cache_only: bool = False
    auto_daily_brief_bootstrap_on_dashboard: bool = True
    daily_brief_require_approval_before_send: bool = False
    daily_brief_default_mode: str = "daily"
    daily_brief_default_audience: str = "pm"
    daily_brief_ack_open_tracking: bool = False
    daily_brief_force_send_on_stale_critical_data: bool = False
    daily_brief_require_portfolio: bool = False
    daily_brief_allow_target_proxy: bool = True
    daily_brief_max_portfolio_age_hours: int = 72
    daily_brief_explanation_mode: str = "deterministic_template_only"
    daily_brief_fallback_mode: str = "deterministic"
    daily_brief_validate_strict: bool = True
    daily_brief_enable_footnotes: bool = True
    daily_brief_enable_deep_rewrite: bool = False
    daily_brief_max_sentence_length: int = 220
    daily_brief_forbidden_phrase_check: bool = True
    daily_brief_dominant_max_lag_days: int = 2
    daily_brief_support_max_lag_days: int = 4
    daily_brief_background_max_lag_days: int = 14
    daily_brief_exclude_stale_background: bool = True
    daily_brief_demote_stale_support: bool = True
    daily_brief_max_fallback_rate: float = 0.40
    daily_brief_warn_on_degraded_llm_run: bool = True
    daily_brief_fail_if_top_sections_fallback_exceed_threshold: bool = False
    daily_brief_llm_parallelism: int = 2
    daily_brief_llm_timeout_seconds: int = 30
    daily_brief_llm_provider: str = "ollama"
    daily_brief_llm_base_url: str = "http://127.0.0.1:11434"
    daily_brief_llm_model: str = "llama3.1:8b"
    blueprint_candidate_explanation_mode: str = "deterministic_template_only"
    blueprint_candidate_fallback_mode: str = "deterministic"
    blueprint_candidate_validate_strict: bool = True
    blueprint_candidate_llm_timeout_seconds: int = 20
    blueprint_candidate_llm_provider: str = "ollama"
    blueprint_candidate_llm_base_url: str = "http://127.0.0.1:11434"
    blueprint_candidate_llm_model: str = "llama3.1:8b"
    blueprint_factsheet_max_age_days: int = 120
    blueprint_candidate_metadata_auto_refresh: bool = True
    blueprint_quality_performance_auto_refresh: bool = False
    blueprint_allow_legacy_inline_fallback: bool = False
    blueprint_auto_refresh_enabled: bool = True
    blueprint_auto_refresh_interval_minutes: int = 360
    blueprint_refresh_stale_after_hours: int = 30
    blueprint_profile_type: str = "hnwi_sg"
    blueprint_concentration_warning_buffer: float = 0.90
    blueprint_macro_freshness_hours: int = 48
    blueprint_holdings_freshness_days: int = 120
    blueprint_liquidity_proxy_freshness_days: int = 30
    blueprint_citation_health_max_age_days: int = 7
    blueprint_data_fallback_max_age_hours: int = 72
    blueprint_citation_hash_max_bytes: int = 5_000_000
    blueprint_max_unknown_dimensions: int = 1
    blueprint_max_unknown_weight_share: float = 0.20
    blueprint_require_liquidity_dimension: bool = True
    blueprint_allow_design_only_mode: bool = True
    blueprint_market_path_enabled: bool = True
    blueprint_kronos_enabled: bool = True
    blueprint_market_path_ui_enabled: bool = True
    blueprint_market_scheduler_enabled: bool = True
    blueprint_market_series_refresh_seconds: int = 86_400
    blueprint_market_forecast_refresh_seconds: int = 86_400
    blueprint_market_identity_audit_seconds: int = 604_800

    @staticmethod
    def from_env() -> "Settings":
        daily_brief_llm_provider = (
            os.getenv("IA_DAILY_BRIEF_LLM_PROVIDER", "ollama").strip().lower() or "ollama"
        )
        daily_brief_llm_model = os.getenv("IA_DAILY_BRIEF_LLM_MODEL", "").strip()
        if not daily_brief_llm_model:
            daily_brief_llm_model = "llama3.1:8b" if daily_brief_llm_provider == "ollama" else "gpt-5-mini"
        daily_brief_llm_base_url = os.getenv("IA_DAILY_BRIEF_LLM_BASE_URL", "").strip()
        if not daily_brief_llm_base_url and daily_brief_llm_provider == "ollama":
            daily_brief_llm_base_url = "http://127.0.0.1:11434"
        blueprint_candidate_llm_provider = (
            os.getenv("IA_BLUEPRINT_CANDIDATE_LLM_PROVIDER", "ollama").strip().lower() or "ollama"
        )
        blueprint_candidate_llm_model = os.getenv("IA_BLUEPRINT_CANDIDATE_LLM_MODEL", "").strip()
        if not blueprint_candidate_llm_model:
            blueprint_candidate_llm_model = "llama3.1:8b" if blueprint_candidate_llm_provider == "ollama" else "gpt-5-mini"
        blueprint_candidate_llm_base_url = os.getenv("IA_BLUEPRINT_CANDIDATE_LLM_BASE_URL", "").strip()
        if not blueprint_candidate_llm_base_url and blueprint_candidate_llm_provider == "ollama":
            blueprint_candidate_llm_base_url = "http://127.0.0.1:11434"
        return Settings(
            db_path=os.getenv("IA_DB_PATH", DEFAULT_DB_RELATIVE_PATH),
            smtp_host=os.getenv("IA_SMTP_HOST", "smtp.example.com"),
            smtp_port=int(os.getenv("IA_SMTP_PORT", "587")),
            smtp_user=os.getenv("IA_SMTP_USER", ""),
            smtp_password=os.getenv("IA_SMTP_PASSWORD", ""),
            alert_from=os.getenv("IA_ALERT_FROM", "alerts@example.com"),
            alert_to=os.getenv("IA_ALERT_TO", "investor@example.com"),
            web_timeout_seconds=int(os.getenv("IA_WEB_TIMEOUT_SECONDS", "12")),
            critical_email_cooldown_minutes=int(
                os.getenv("IA_CRITICAL_EMAIL_COOLDOWN_MINUTES", "30")
            ),
            mcp_timeout_seconds=int(os.getenv("IA_MCP_TIMEOUT_SECONDS", "4")),
            mcp_max_retries=int(os.getenv("IA_MCP_MAX_RETRIES", "1")),
            mcp_live_required=os.getenv("IA_MCP_LIVE_REQUIRED", "1")
            in {"1", "true", "TRUE", "yes", "YES"},
            mcp_min_success_ratio=float(os.getenv("IA_MCP_MIN_SUCCESS_RATIO", "0.70")),
            mcp_min_success_count=int(os.getenv("IA_MCP_MIN_SUCCESS_COUNT", "10")),
            mcp_connect_timeout_seconds=int(os.getenv("IA_MCP_CONNECT_TIMEOUT_SECONDS", "8")),
            mcp_read_timeout_seconds=int(os.getenv("IA_MCP_READ_TIMEOUT_SECONDS", "12")),
            mcp_max_workers=int(os.getenv("IA_MCP_MAX_WORKERS", "12")),
            mcp_server_budget_seconds=float(os.getenv("IA_MCP_SERVER_BUDGET_SECONDS", "15")),
            http_proxy=os.getenv(
                "IA_HTTP_PROXY",
                os.getenv("HTTP_PROXY", os.getenv("http_proxy", "")),
            ),
            https_proxy=os.getenv(
                "IA_HTTPS_PROXY",
                os.getenv("HTTPS_PROXY", os.getenv("https_proxy", "")),
            ),
            no_proxy=os.getenv(
                "IA_NO_PROXY",
                os.getenv("NO_PROXY", os.getenv("no_proxy", "")),
            ),
            proxy_mode=os.getenv("IA_PROXY_MODE", "auto"),
            mcp_registry_snapshot_path=os.getenv(
                "IA_MCP_REGISTRY_SNAPSHOT_PATH", "mcp/registry_snapshot.json"
            ),
            mcp_priority_mode=os.getenv("IA_MCP_PRIORITY_MODE", "prioritized_only").strip().lower(),
            mcp_priority_servers=os.getenv(
                "IA_MCP_PRIORITY_SERVERS",
                DEFAULT_MCP_PRIORITY_SERVERS,
            ),
            mcp_connectors_candidates_path=os.getenv(
                "IA_MCP_CONNECTORS_CANDIDATES_PATH",
                "mcp/connectors/financial_intelligence_candidates.json",
            ),
            mcp_registry_priorities_path=os.getenv(
                "IA_MCP_REGISTRY_PRIORITIES_PATH",
                "mcp/connectors/current_registry_priorities.json",
            ),
            refresh_live_cache_on_brief=os.getenv("IA_REFRESH_LIVE_CACHE_ON_BRIEF", "1")
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_mcp_live_required=os.getenv("IA_DAILY_BRIEF_MCP_LIVE_REQUIRED", "1")
            in {"1", "true", "TRUE", "yes", "YES"},
            platform_auto_refresh_daily_log=os.getenv("IA_PLATFORM_AUTO_REFRESH_DAILY_LOG", "1")
            in {"1", "true", "TRUE", "yes", "YES"},
            platform_daily_log_max_age_hours=int(
                os.getenv("IA_PLATFORM_DAILY_LOG_MAX_AGE_HOURS", "24")
            ),
            build_id=os.getenv("IA_BUILD_ID", "dev"),
            git_sha=os.getenv("IA_GIT_SHA", ""),
            api_version=int(os.getenv("IA_API_VERSION", "1")),
            tls_ca_bundle=os.getenv("IA_TLS_CA_BUNDLE", os.getenv("SSL_CERT_FILE", "")),
            tls_verify=os.getenv("IA_TLS_VERIFY", "1")
            in {"1", "true", "TRUE", "yes", "YES"},
            auto_daily_brief_enabled=os.getenv("IA_AUTO_DAILY_BRIEF_ENABLED", "1")
            in {"1", "true", "TRUE", "yes", "YES"},
            auto_daily_brief_interval_minutes=max(
                5, int(os.getenv("IA_AUTO_DAILY_BRIEF_INTERVAL_MINUTES", "15"))
            ),
            auto_daily_brief_run_hour_china=min(
                23, max(0, int(os.getenv("IA_AUTO_DAILY_BRIEF_RUN_HOUR_CHINA", "8")))
            ),
            auto_daily_brief_second_run_hour_china=min(
                23, max(0, int(os.getenv("IA_AUTO_DAILY_BRIEF_SECOND_RUN_HOUR_CHINA", "20")))
            ),
            auto_daily_brief_send_email=os.getenv("IA_AUTO_DAILY_BRIEF_SEND_EMAIL", "0")
            in {"1", "true", "TRUE", "yes", "YES"},
            auto_daily_brief_force_cache_only=os.getenv("IA_AUTO_DAILY_BRIEF_FORCE_CACHE_ONLY", "0")
            in {"1", "true", "TRUE", "yes", "YES"},
            auto_daily_brief_bootstrap_on_dashboard=os.getenv(
                "IA_AUTO_DAILY_BRIEF_BOOTSTRAP_ON_DASHBOARD", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_require_approval_before_send=os.getenv(
                "IA_DAILY_BRIEF_REQUIRE_APPROVAL_BEFORE_SEND", "0"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_default_mode=(
                os.getenv("IA_DAILY_BRIEF_DEFAULT_MODE", "daily").strip().lower() or "daily"
            ),
            daily_brief_default_audience=(
                os.getenv("IA_DAILY_BRIEF_DEFAULT_AUDIENCE", "pm").strip().lower() or "pm"
            ),
            daily_brief_ack_open_tracking=os.getenv("IA_DAILY_BRIEF_ACK_OPEN_TRACKING", "0")
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_force_send_on_stale_critical_data=os.getenv(
                "IA_DAILY_BRIEF_FORCE_SEND_ON_STALE_CRITICAL_DATA", "0"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_require_portfolio=os.getenv("IA_DAILY_BRIEF_REQUIRE_PORTFOLIO", "0")
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_allow_target_proxy=os.getenv("IA_DAILY_BRIEF_ALLOW_TARGET_PROXY", "1")
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_max_portfolio_age_hours=max(
                1, int(os.getenv("IA_DAILY_BRIEF_MAX_PORTFOLIO_AGE_HOURS", "72"))
            ),
            daily_brief_explanation_mode=(
                os.getenv("IA_DAILY_BRIEF_EXPLANATION_MODE", "deterministic_template_only").strip().lower()
                or "deterministic_template_only"
            ),
            daily_brief_fallback_mode=(
                os.getenv("IA_DAILY_BRIEF_FALLBACK_MODE", "deterministic").strip().lower()
                or "deterministic"
            ),
            daily_brief_validate_strict=os.getenv("IA_DAILY_BRIEF_VALIDATE_STRICT", "1")
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_enable_footnotes=os.getenv("IA_DAILY_BRIEF_ENABLE_FOOTNOTES", "1")
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_enable_deep_rewrite=os.getenv("IA_DAILY_BRIEF_ENABLE_DEEP_REWRITE", "0")
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_max_sentence_length=max(
                80, int(os.getenv("IA_DAILY_BRIEF_MAX_SENTENCE_LENGTH", "220"))
            ),
            daily_brief_forbidden_phrase_check=os.getenv(
                "IA_DAILY_BRIEF_FORBIDDEN_PHRASE_CHECK", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_dominant_max_lag_days=max(
                1, int(os.getenv("IA_DAILY_BRIEF_DOMINANT_MAX_LAG_DAYS", "2"))
            ),
            daily_brief_support_max_lag_days=max(
                1, int(os.getenv("IA_DAILY_BRIEF_SUPPORT_MAX_LAG_DAYS", "4"))
            ),
            daily_brief_background_max_lag_days=max(
                1, int(os.getenv("IA_DAILY_BRIEF_BACKGROUND_MAX_LAG_DAYS", "14"))
            ),
            daily_brief_exclude_stale_background=os.getenv(
                "IA_DAILY_BRIEF_EXCLUDE_STALE_BACKGROUND", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_demote_stale_support=os.getenv(
                "IA_DAILY_BRIEF_DEMOTE_STALE_SUPPORT", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_max_fallback_rate=min(
                1.0, max(0.0, float(os.getenv("IA_DAILY_BRIEF_MAX_FALLBACK_RATE", "0.40")))
            ),
            daily_brief_warn_on_degraded_llm_run=os.getenv(
                "IA_DAILY_BRIEF_WARN_ON_DEGRADED_LLM_RUN", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_fail_if_top_sections_fallback_exceed_threshold=os.getenv(
                "IA_DAILY_BRIEF_FAIL_IF_TOP_SECTIONS_FALLBACK_EXCEED_THRESHOLD", "0"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            daily_brief_llm_parallelism=max(
                1, int(os.getenv("IA_DAILY_BRIEF_LLM_PARALLELISM", "2"))
            ),
            daily_brief_llm_timeout_seconds=max(
                5, int(os.getenv("IA_DAILY_BRIEF_LLM_TIMEOUT_SECONDS", "30"))
            ),
            daily_brief_llm_provider=daily_brief_llm_provider,
            daily_brief_llm_base_url=daily_brief_llm_base_url,
            daily_brief_llm_model=daily_brief_llm_model,
            blueprint_candidate_explanation_mode=(
                os.getenv("IA_BLUEPRINT_CANDIDATE_EXPLANATION_MODE", "deterministic_template_only").strip().lower()
                or "deterministic_template_only"
            ),
            blueprint_candidate_fallback_mode=(
                os.getenv("IA_BLUEPRINT_CANDIDATE_FALLBACK_MODE", "deterministic").strip().lower()
                or "deterministic"
            ),
            blueprint_candidate_validate_strict=os.getenv(
                "IA_BLUEPRINT_CANDIDATE_VALIDATE_STRICT",
                "1",
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_candidate_llm_timeout_seconds=max(
                5, int(os.getenv("IA_BLUEPRINT_CANDIDATE_LLM_TIMEOUT_SECONDS", "20"))
            ),
            blueprint_candidate_llm_provider=blueprint_candidate_llm_provider,
            blueprint_candidate_llm_base_url=blueprint_candidate_llm_base_url,
            blueprint_candidate_llm_model=blueprint_candidate_llm_model,
            blueprint_factsheet_max_age_days=max(
                1, int(os.getenv("IA_BLUEPRINT_FACTSHEET_MAX_AGE_DAYS", "120"))
            ),
            blueprint_candidate_metadata_auto_refresh=os.getenv(
                "IA_BLUEPRINT_CANDIDATE_METADATA_AUTO_REFRESH", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_quality_performance_auto_refresh=os.getenv(
                "IA_BLUEPRINT_QUALITY_PERFORMANCE_AUTO_REFRESH", "0"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_allow_legacy_inline_fallback=os.getenv(
                "IA_BLUEPRINT_ALLOW_LEGACY_INLINE_FALLBACK", "0"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_auto_refresh_enabled=os.getenv(
                "IA_BLUEPRINT_AUTO_REFRESH_ENABLED", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_auto_refresh_interval_minutes=max(
                15, int(os.getenv("IA_BLUEPRINT_AUTO_REFRESH_INTERVAL_MINUTES", "360"))
            ),
            blueprint_refresh_stale_after_hours=max(
                6, int(os.getenv("IA_BLUEPRINT_REFRESH_STALE_AFTER_HOURS", "30"))
            ),
            blueprint_profile_type=os.getenv("IA_BLUEPRINT_PROFILE_TYPE", "hnwi_sg").strip().lower() or "hnwi_sg",
            blueprint_concentration_warning_buffer=float(
                os.getenv("IA_BLUEPRINT_CONCENTRATION_WARNING_BUFFER", "0.90")
            ),
            blueprint_macro_freshness_hours=max(
                1, int(os.getenv("IA_BLUEPRINT_MACRO_FRESHNESS_HOURS", "48"))
            ),
            blueprint_holdings_freshness_days=max(
                1, int(os.getenv("IA_BLUEPRINT_HOLDINGS_FRESHNESS_DAYS", "120"))
            ),
            blueprint_liquidity_proxy_freshness_days=max(
                1, int(os.getenv("IA_BLUEPRINT_LIQUIDITY_PROXY_FRESHNESS_DAYS", "30"))
            ),
            blueprint_citation_health_max_age_days=max(
                1, int(os.getenv("IA_BLUEPRINT_CITATION_HEALTH_MAX_AGE_DAYS", "7"))
            ),
            blueprint_data_fallback_max_age_hours=max(
                1, int(os.getenv("IA_BLUEPRINT_DATA_FALLBACK_MAX_AGE_HOURS", "72"))
            ),
            blueprint_citation_hash_max_bytes=max(
                1024, int(os.getenv("IA_BLUEPRINT_CITATION_HASH_MAX_BYTES", "5000000"))
            ),
            blueprint_max_unknown_dimensions=max(
                0, int(os.getenv("IA_BLUEPRINT_MAX_UNKNOWN_DIMENSIONS", "1"))
            ),
            blueprint_max_unknown_weight_share=float(
                os.getenv("IA_BLUEPRINT_MAX_UNKNOWN_WEIGHT_SHARE", "0.20")
            ),
            blueprint_require_liquidity_dimension=os.getenv(
                "IA_BLUEPRINT_REQUIRE_LIQUIDITY_DIMENSION", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_allow_design_only_mode=os.getenv(
                "IA_BLUEPRINT_ALLOW_DESIGN_ONLY_MODE", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_market_path_enabled=os.getenv(
                "IA_BLUEPRINT_MARKET_PATH_ENABLED", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_kronos_enabled=os.getenv(
                "IA_BLUEPRINT_KRONOS_ENABLED", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_market_path_ui_enabled=os.getenv(
                "IA_BLUEPRINT_MARKET_PATH_UI_ENABLED", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_market_scheduler_enabled=os.getenv(
                "IA_BLUEPRINT_MARKET_SCHEDULER_ENABLED", "1"
            )
            in {"1", "true", "TRUE", "yes", "YES"},
            blueprint_market_series_refresh_seconds=max(
                300, int(os.getenv("IA_BLUEPRINT_MARKET_SERIES_REFRESH_SECONDS", "86400"))
            ),
            blueprint_market_forecast_refresh_seconds=max(
                300, int(os.getenv("IA_BLUEPRINT_MARKET_FORECAST_REFRESH_SECONDS", "86400"))
            ),
            blueprint_market_identity_audit_seconds=max(
                3600, int(os.getenv("IA_BLUEPRINT_MARKET_IDENTITY_AUDIT_SECONDS", "604800"))
            ),
        )

    def resolved_db_path(self, project_root: Path) -> Path:
        _ = project_root
        return get_db_path(settings=self)

    def prioritized_mcp_server_set(self) -> set[str]:
        values = [item.strip() for item in self.mcp_priority_servers.split(",") if item.strip()]
        return set(values)


def get_settings() -> Settings:
    return Settings.from_env()
