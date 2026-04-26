from __future__ import annotations

from datetime import UTC, datetime
import logging
import random
import sqlite3
import sys
import threading
from pathlib import Path
from typing import Any

import numpy as np

from app.config import get_db_path, get_repo_root, get_settings
from app.services.blueprint_benchmark_registry import canonical_benchmark_full_name, resolve_benchmark_assignment
from app.v2.blueprint_market.kronos_input_adapter import build_kronos_input
from app.v2.blueprint_market.market_identity import ensure_candidate_market_identities, resolve_market_identity, set_forecast_driving_series
from app.v2.blueprint_market.series_refresh_service import (
    check_candidate_series_freshness,
    list_blueprint_market_candidates,
    refresh_candidate_series,
)
from app.v2.blueprint_market.series_store import (
    ensure_blueprint_market_tables,
    latest_forecast_artifact,
    latest_series_version,
    load_price_series,
    persist_forecast_artifact,
    record_forecast_run,
)


logger = logging.getLogger(__name__)

_INTERVAL = "1day"
_HORIZON = 21
_MAX_CONTEXT = 400
_OBSERVED_POINTS = 120
_MIN_HISTORY_BARS = 260
_MODEL_NAME = "kronos"
_MODEL_VERSION = "NeoQuasar/Kronos-base"
_MODEL_TOKENIZER = "NeoQuasar/Kronos-Tokenizer-base"
_SERVICE_NAME = "blueprint_kronos_market_setup"
_WRAPPER_CLASS = "KronosPredictor"
_RUNTIME_ENGINE = "python_kronos"
_SUPPORT_SEMANTICS_VERSION = "2026-04-20-truth-soft-degrade-v1"
_HARD_STALE_DAYS = 45
_CAUTION_STALE_DAYS = 7
_HARD_MISSING_RATIO = 0.15
_SOFT_STALE_DAYS = 1
_SAMPLE_PATH_COUNT = 16
_SAMPLE_SEED_BASE = 17_291
_SAMPLE_TEMPERATURE = 1.0
_SAMPLE_TOP_P = 0.9
_RUNTIME_CACHE: dict[str, Any] = {"predictor": None, "load_failed": None}
_PREDICTOR_LOCK = threading.RLock()
_CANDIDATE_BUILD_LOCKS: dict[str, threading.RLock] = {}
_CANDIDATE_BUILD_LOCKS_LOCK = threading.RLock()
_USEFULNESS_ORDER = {
    "suppressed": 0,
    "unstable": 1,
    "usable_with_caution": 2,
    "usable": 3,
    "strong": 4,
}
_PROXY_CAUTION_ONLY_SLEEVES = {
    "global_equity_core",
    "developed_ex_us_optional",
    "emerging_markets",
    "china_satellite",
    "cash_bills",
    "real_assets",
    "alternatives",
}
_PROXY_STRONG_ALLOWED_SLEEVES = {"ig_bonds"}
_CONVEX_SLEEVES = {"convex"}
_TIMING_LABELS = {
    "timing_ready": "Timing ready",
    "timing_review": "Timing review",
    "timing_fragile": "Timing fragile",
    "timing_constrained": "Timing constrained",
    "timing_unavailable": "Timing unavailable",
}
_SCOPE_BY_BENCHMARK_KEY = {
    "SP500": ("us_large_cap", "broad U.S. large-cap equity"),
    "MSCI_WORLD": ("developed_markets_equity", "broad developed markets"),
    "FTSE_DEV_WORLD": ("developed_markets_equity", "broad developed markets"),
    "FTSE_ALL_WORLD": ("all_world_equity", "all-world equity"),
    "MSCI_ACWI": ("all_world_equity", "all-world equity"),
    "MSCI_EM_IMI": ("emerging_markets_equity", "emerging-markets equity"),
    "FTSE_EM": ("emerging_markets_equity", "emerging-markets equity"),
    "MSCI_CHINA": ("china_equity", "China equity"),
    "GLOBAL_AGG_BOND": ("investment_grade_bonds", "investment-grade bonds"),
    "GLOBAL_AGG_BOND_HDG": ("investment_grade_bonds", "investment-grade bonds"),
    "SGD_GOV_BOND": ("investment_grade_bonds", "Singapore government bonds"),
    "SHORT_TBILL": ("short_duration_treasury", "short-duration cash alternatives"),
    "GOLD": ("gold", "gold"),
    "BROAD_COMMODITIES": ("broad_commodities", "broad commodities"),
    "GLOBAL_REITS": ("global_reits", "global REITs"),
}
_SCOPE_BY_SYMBOL = {
    "SPY": ("us_large_cap", "broad U.S. large-cap equity"),
    "IVV": ("us_large_cap", "broad U.S. large-cap equity"),
    "VOO": ("us_large_cap", "broad U.S. large-cap equity"),
    "CSPX": ("us_large_cap", "broad U.S. large-cap equity"),
    "ACWI": ("all_world_equity", "all-world equity"),
    "VWRA": ("all_world_equity", "all-world equity"),
    "VWRL": ("all_world_equity", "all-world equity"),
    "URTH": ("developed_markets_equity", "broad developed markets"),
    "IWDA": ("developed_markets_equity", "broad developed markets"),
    "EEM": ("emerging_markets_equity", "emerging-markets equity"),
    "MCHI": ("china_equity", "China equity"),
    "GLD": ("gold", "gold"),
    "SGLN": ("gold", "gold"),
    "BNDW": ("investment_grade_bonds", "investment-grade bonds"),
    "AGGU": ("investment_grade_bonds", "investment-grade bonds"),
    "MBH.SI": ("investment_grade_bonds", "Singapore government bonds"),
    "A35": ("investment_grade_bonds", "Singapore government bonds"),
    "SHV": ("short_duration_treasury", "short-duration cash alternatives"),
    "BIL": ("short_duration_treasury", "short-duration cash alternatives"),
    "SGOV": ("short_duration_treasury", "short-duration cash alternatives"),
    "DBC": ("broad_commodities", "broad commodities"),
    "DBMF": ("managed_futures", "managed futures"),
}


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _candidate_build_lock(candidate_id: str) -> threading.RLock:
    normalized = str(candidate_id or "").strip() or "unknown_candidate"
    with _CANDIDATE_BUILD_LOCKS_LOCK:
        lock = _CANDIDATE_BUILD_LOCKS.get(normalized)
        if lock is None:
            lock = threading.RLock()
            _CANDIDATE_BUILD_LOCKS[normalized] = lock
        return lock


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _feature_enabled() -> bool:
    settings = get_settings()
    return bool(getattr(settings, "blueprint_market_path_enabled", False))


def _ui_enabled() -> bool:
    settings = get_settings()
    return bool(getattr(settings, "blueprint_market_path_ui_enabled", False))


def _kronos_enabled() -> bool:
    settings = get_settings()
    return bool(getattr(settings, "blueprint_kronos_enabled", False))


def _point(timestamp: str, value: float) -> dict[str, Any]:
    return {"timestamp": str(timestamp), "value": round(float(value), 4)}


def _provider_source_for_identity(identity: dict[str, Any] | None) -> str:
    if str((identity or {}).get("series_role") or "") == "approved_proxy":
        return "twelve_data+approved_proxy"
    return "twelve_data"


def _provider_name(provider_source: str | None) -> str:
    raw = str(provider_source or "").strip()
    if not raw:
        return "twelve_data"
    return raw.split("+", 1)[0]


def _timing_label(timing_state: str) -> str:
    return _TIMING_LABELS.get(str(timing_state or ""), "Timing not assessed")


def _timing_state_for_suppression(suppression_reason: str | None) -> tuple[str, list[str]]:
    reason = str(suppression_reason or "market_setup_unavailable").strip() or "market_setup_unavailable"
    if reason in {"symbol_mapping_failed", "invalid_calendar", "unknown_model_identity", "scope_mapping_failed"}:
        return "timing_unavailable", [reason]
    if reason == "model_execution_failed":
        return "timing_constrained", ["forecast_runtime_failed"]
    return "timing_constrained", [reason]


def _timing_state_from_market_path(
    *,
    usefulness_label: str,
    market_setup_state: str,
    path_quality_label: str | None,
    candidate_fragility_label: str | None,
    threshold_drift_direction: str | None,
    uses_proxy_series: bool,
    series_quality_summary: dict[str, Any] | None,
) -> tuple[str, list[str]]:
    state = str(market_setup_state or "").strip()
    usefulness = str(usefulness_label or "").strip()
    path_quality = str(path_quality_label or "").strip()
    fragility = str(candidate_fragility_label or "").strip()
    drift = str(threshold_drift_direction or "").strip()
    summary = dict(series_quality_summary or {})
    reasons: list[str] = []
    if state == "unavailable" or usefulness == "suppressed":
        return "timing_unavailable", ["market_setup_unavailable"]
    if state in {"stale", "degraded"}:
        return "timing_constrained", ["proxy_series_stale" if state == "stale" else "market_setup_unavailable"]
    if uses_proxy_series:
        stale_days = int(summary.get("stale_days") or 0)
        reasons.append("proxy_series_fresh_and_approved" if stale_days <= _CAUTION_STALE_DAYS else "proxy_series_stale")
    else:
        reasons.append("direct_series_current_and_usable")
    if drift == "toward_weakening":
        reasons.append("threshold_drift_weakening")
    if fragility in {"fragile", "acute"}:
        reasons.append("path_fragility_current")
    if path_quality == "noisy":
        reasons.append("path_noisy_but_usable")
    if usefulness == "unstable" and ("path_fragility_current" in reasons or "threshold_drift_weakening" in reasons):
        return "timing_fragile", sorted(set(reasons))
    if (
        usefulness == "strong"
        and state == "direct_usable"
        and path_quality in {"clean", "balanced"}
        and fragility in {"resilient", "watchful"}
    ):
        return "timing_ready", sorted(set(reasons))
    return "timing_review", sorted(set(reasons))


def _driving_symbol(identity: dict[str, Any] | None) -> str | None:
    for key in ("provider_symbol", "symbol"):
        value = str((identity or {}).get(key) or "").strip().upper()
        if value:
            return value
    return None


def _proxy_symbol(identity: dict[str, Any] | None) -> str | None:
    relationship = str((identity or {}).get("proxy_relationship") or "").strip()
    if relationship.startswith("benchmark_proxy:"):
        value = relationship.partition(":")[2].strip().upper()
        if value:
            return value
    return None


def _proxy_reason(identity: dict[str, Any] | None) -> str | None:
    if str((identity or {}).get("series_role") or "").strip() != "approved_proxy":
        return None
    return "direct_series_unavailable_or_ineligible"


def _route_state(identity: dict[str, Any] | None, quality_flags: list[str] | None = None) -> str:
    flags = set(list(quality_flags or []))
    if "last_good_artifact_served" in flags:
        return "last_good_artifact"
    if str((identity or {}).get("series_role") or "").strip() == "approved_proxy":
        return "approved_proxy"
    if identity:
        return "direct"
    return "unavailable"


def _freshness_state(
    *,
    series_quality_summary: dict[str, Any] | None,
    quality_flags: list[str] | None = None,
) -> str:
    flags = set(list(quality_flags or []))
    if "last_good_artifact_served" in flags:
        return "last_good"
    summary = dict(series_quality_summary or {})
    stale_days = int(summary.get("stale_days") or 0)
    if stale_days <= _SOFT_STALE_DAYS:
        return "fresh"
    if stale_days <= _HARD_STALE_DAYS:
        return "stale"
    return "untrusted"


def _market_setup_state(
    *,
    support: dict[str, Any] | None,
    series_quality_summary: dict[str, Any] | None,
    quality_flags: list[str] | None = None,
) -> str:
    data = dict(support or {})
    summary = dict(series_quality_summary or {})
    flags = set(list(quality_flags or []))
    suppression_reason = str(data.get("suppression_reason") or "").strip()
    if suppression_reason in {
        "symbol_mapping_failed",
        "invalid_calendar",
        "unknown_model_identity",
        "scope_mapping_failed",
    }:
        return "unavailable"
    freshness_state = str(data.get("freshness_state") or _freshness_state(series_quality_summary=summary, quality_flags=quality_flags)).strip()
    if freshness_state == "untrusted":
        return "unavailable"
    if freshness_state in {"last_good", "stale"}:
        return "stale"
    if suppression_reason:
        return "degraded"
    if data.get("liquidity_feature_mode") == "price_only":
        return "degraded"
    if "sampling_degraded" in flags:
        return "degraded"
    if bool(summary.get("uses_proxy_series")):
        return "proxy_usable"
    return "direct_usable"


def _confidence_label(
    *,
    series_quality_summary: dict[str, Any] | None,
    usefulness_label: str,
    market_setup_state: str,
    uncertainty_width: float | None = None,
    path_count: int | None = None,
) -> str:
    if market_setup_state == "unavailable":
        return "unavailable"
    summary = dict(series_quality_summary or {})
    stale_days = int(summary.get("stale_days") or 0)
    missing_ratio = float(summary.get("missing_bar_ratio") or 0.0)
    uses_proxy = bool(summary.get("uses_proxy_series"))
    if market_setup_state in {"stale", "degraded"}:
        return "low"
    if uses_proxy or stale_days > _SOFT_STALE_DAYS or missing_ratio > 0.05:
        return "medium" if usefulness_label == "strong" and (uncertainty_width or 1.0) <= 0.06 else "low"
    if (path_count or 0) < _SAMPLE_PATH_COUNT:
        return "low"
    if usefulness_label == "strong" and (uncertainty_width or 1.0) <= 0.05:
        return "high"
    if usefulness_label in {"strong", "usable"} and (uncertainty_width or 1.0) <= 0.1:
        return "medium"
    return "low"


def _sample_seed_list(*, count: int, base: int = _SAMPLE_SEED_BASE) -> list[int]:
    return [int(base + index) for index in range(max(0, count))]


def _set_runtime_seed(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    try:
        import torch

        torch.manual_seed(seed)
        if torch.cuda.is_available():
            torch.cuda.manual_seed_all(seed)
    except Exception:
        pass


def _representative_path_index(values: np.ndarray, quantile: float) -> int:
    if values.size <= 1:
        return 0
    target = float(np.quantile(values, quantile))
    return int(np.argmin(np.abs(values - target)))


def _point_series(timestamps: list[str], values: np.ndarray) -> list[dict[str, Any]]:
    return [_point(timestamp, value) for timestamp, value in zip(timestamps, values.tolist(), strict=False)]


def _scope_from_assignment(
    *,
    benchmark_key: str | None,
    benchmark_label: str | None,
    symbol: str | None,
) -> tuple[str | None, str | None]:
    key = str(benchmark_key or "").strip().upper()
    if key in _SCOPE_BY_BENCHMARK_KEY:
        return _SCOPE_BY_BENCHMARK_KEY[key]
    normalized_symbol = str(symbol or "").strip().upper()
    if normalized_symbol in _SCOPE_BY_SYMBOL:
        return _SCOPE_BY_SYMBOL[normalized_symbol]
    label = canonical_benchmark_full_name(key, benchmark_label)
    if label:
        text = str(label).strip().lower()
        if text:
            return text.replace(" ", "_").replace("-", "_"), text
    return None, None


def _resolve_scope_manifest(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
    identity: dict[str, Any],
) -> tuple[str | None, str | None]:
    candidate_rows = list_blueprint_market_candidates(conn, candidate_id=candidate_id)
    candidate_row = dict(candidate_rows[0]) if candidate_rows else {"symbol": identity.get("symbol"), "sleeve_key": None}
    sleeve_key = str(candidate_row.get("sleeve_key") or "").strip()
    assignment = resolve_benchmark_assignment(conn, candidate=candidate_row, sleeve_key=sleeve_key)
    symbol = _driving_symbol(identity)
    scope = _scope_from_assignment(
        benchmark_key=str(dict(assignment or {}).get("benchmark_key") or "").strip() or None,
        benchmark_label=str(dict(assignment or {}).get("benchmark_label") or "").strip() or None,
        symbol=symbol,
    )
    if all(scope):
        return scope
    proxy_symbol = _proxy_symbol(identity)
    if proxy_symbol:
        proxy_scope = _scope_from_assignment(benchmark_key=None, benchmark_label=None, symbol=proxy_symbol)
        if all(proxy_scope):
            return proxy_scope
    return _scope_from_assignment(
        benchmark_key=None,
        benchmark_label=None,
        symbol=str(candidate_row.get("symbol") or symbol or "").strip().upper() or None,
    )


def _choose_identity_with_series(
    conn: sqlite3.Connection,
    *,
    candidate_id: str,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    identities = ensure_candidate_market_identities(conn, candidate_id)
    direct_identity = next(
        (
            item
            for item in identities
            if str(item.get("series_role") or "") == "direct"
            and str(item.get("forecast_eligibility") or "") == "eligible"
        ),
        None,
    )
    direct_rows: list[dict[str, Any]] = []
    if direct_identity is not None:
        direct_rows = load_price_series(
            conn,
            candidate_id=str(direct_identity["candidate_id"]),
            series_role=str(direct_identity["series_role"]),
            interval=str(direct_identity.get("primary_interval") or _INTERVAL),
            ascending=True,
        )
        if direct_rows:
            direct_quality = dict(
                (
                    check_candidate_series_freshness(
                        conn,
                        candidate_id=str(direct_identity["candidate_id"]),
                        series_role=str(direct_identity["series_role"]),
                    )
                    or {}
                ).get("series_quality_summary")
                or {}
            )
            if _hard_failure_reason(direct_quality, row_count=len(direct_rows)) is None:
                set_forecast_driving_series(
                    conn,
                    candidate_id=str(direct_identity["candidate_id"]),
                    series_role=str(direct_identity["series_role"]),
                    interval=str(direct_identity.get("primary_interval") or _INTERVAL),
                )
                return direct_identity, direct_rows
    proxy_identity = next(
        (
            item
            for item in identities
            if str(item.get("series_role") or "") == "approved_proxy"
            and str(item.get("forecast_eligibility") or "") == "eligible"
        ),
        None,
    )
    proxy_rows: list[dict[str, Any]] = []
    if proxy_identity is not None:
        proxy_rows = load_price_series(
            conn,
            candidate_id=str(proxy_identity["candidate_id"]),
            series_role=str(proxy_identity["series_role"]),
            interval=str(proxy_identity.get("primary_interval") or _INTERVAL),
            ascending=True,
        )
        if proxy_rows:
            proxy_quality = dict(
                (
                    check_candidate_series_freshness(
                        conn,
                        candidate_id=str(proxy_identity["candidate_id"]),
                        series_role=str(proxy_identity["series_role"]),
                    )
                    or {}
                ).get("series_quality_summary")
                or {}
            )
            if _hard_failure_reason(proxy_quality, row_count=len(proxy_rows)) is None:
                set_forecast_driving_series(
                    conn,
                    candidate_id=str(proxy_identity["candidate_id"]),
                    series_role=str(proxy_identity["series_role"]),
                    interval=str(proxy_identity.get("primary_interval") or _INTERVAL),
                )
                return proxy_identity, proxy_rows
    if direct_identity is not None and direct_rows:
        set_forecast_driving_series(
            conn,
            candidate_id=str(direct_identity["candidate_id"]),
            series_role=str(direct_identity["series_role"]),
            interval=str(direct_identity.get("primary_interval") or _INTERVAL),
        )
        return direct_identity, direct_rows
    if proxy_identity is not None and proxy_rows:
        set_forecast_driving_series(
            conn,
            candidate_id=str(proxy_identity["candidate_id"]),
            series_role=str(proxy_identity["series_role"]),
            interval=str(proxy_identity.get("primary_interval") or _INTERVAL),
        )
        return proxy_identity, proxy_rows
    return direct_identity or proxy_identity or resolve_market_identity(conn, candidate_id, allow_proxy=True), direct_rows or proxy_rows


def _candidate_sleeve_keys(conn: sqlite3.Connection, candidate_id: str) -> set[str]:
    return {
        str(item.get("sleeve_key") or "").strip()
        for item in list_blueprint_market_candidates(conn, candidate_id=candidate_id)
        if str(item.get("sleeve_key") or "").strip()
    }


def _demote_usefulness(current: str, cap: str) -> str:
    current_rank = _USEFULNESS_ORDER.get(str(current), 0)
    cap_rank = _USEFULNESS_ORDER.get(str(cap), 0)
    if current_rank <= cap_rank:
        return current
    for label, rank in _USEFULNESS_ORDER.items():
        if rank == cap_rank:
            return label
    return current


def _proxy_strong_allowed(
    *,
    uses_proxy_series: bool,
    sleeve_keys: set[str],
    path_quality_label: str,
    candidate_fragility_label: str,
    threshold_drift_direction: str,
    scenario_takeaways: dict[str, Any],
) -> bool:
    if not uses_proxy_series:
        return False
    if not sleeve_keys or any(sleeve not in _PROXY_STRONG_ALLOWED_SLEEVES for sleeve in sleeve_keys):
        return False
    if path_quality_label not in {"clean", "balanced"}:
        return False
    if candidate_fragility_label not in {"resilient", "watchful"}:
        return False
    if threshold_drift_direction == "toward_weakening":
        return False
    if bool((scenario_takeaways or {}).get("stress_breaks_candidate_support")):
        return False
    return True


def _proxy_cap_for_sleeves(
    *,
    uses_proxy_series: bool,
    sleeve_keys: set[str],
    path_quality_label: str,
    candidate_fragility_label: str,
    threshold_drift_direction: str,
    scenario_takeaways: dict[str, Any],
) -> str | None:
    if not uses_proxy_series or not sleeve_keys:
        return None
    if any(sleeve in _PROXY_STRONG_ALLOWED_SLEEVES for sleeve in sleeve_keys):
        return None
    if not bool(sleeve_keys & _PROXY_CAUTION_ONLY_SLEEVES):
        return None
    if (
        path_quality_label in {"clean", "balanced"}
        and candidate_fragility_label in {"resilient", "watchful"}
        and threshold_drift_direction != "toward_weakening"
        and not bool((scenario_takeaways or {}).get("favorable_case_is_narrow"))
        and not bool((scenario_takeaways or {}).get("stress_breaks_candidate_support"))
    ):
        return "usable"
    return "usable_with_caution"


def _recalibrate_usefulness_label(
    *,
    base_usefulness_label: str,
    series_quality_summary: dict[str, Any],
    sleeve_keys: set[str],
    path_quality_label: str,
    candidate_fragility_label: str,
    threshold_drift_direction: str,
    volatility_outlook: str,
    scenario_takeaways: dict[str, Any],
) -> tuple[str, list[str]]:
    label = str(base_usefulness_label or "suppressed")
    quality_flags: list[str] = []
    uses_proxy_series = bool(series_quality_summary.get("uses_proxy_series"))
    direct_convex_exception = (not uses_proxy_series) and bool(sleeve_keys) and sleeve_keys.issubset(_CONVEX_SLEEVES)
    proxy_strong_allowed = _proxy_strong_allowed(
        uses_proxy_series=uses_proxy_series,
        sleeve_keys=sleeve_keys,
        path_quality_label=path_quality_label,
        candidate_fragility_label=candidate_fragility_label,
        threshold_drift_direction=threshold_drift_direction,
        scenario_takeaways=scenario_takeaways,
    )
    proxy_cap = _proxy_cap_for_sleeves(
        uses_proxy_series=uses_proxy_series,
        sleeve_keys=sleeve_keys,
        path_quality_label=path_quality_label,
        candidate_fragility_label=candidate_fragility_label,
        threshold_drift_direction=threshold_drift_direction,
        scenario_takeaways=scenario_takeaways,
    )
    if proxy_cap is not None and label == "strong":
        label = _demote_usefulness(label, proxy_cap)
        quality_flags.append("proxy_strength_capped")
    if label == "strong" and path_quality_label == "noisy" and candidate_fragility_label == "fragile":
        target = "usable"
        if proxy_cap == "usable_with_caution":
            target = proxy_cap
        label = _demote_usefulness(label, target)
        quality_flags.append("strong_demoted_noisy_fragile")
    narrow_support = bool((scenario_takeaways or {}).get("favorable_case_is_narrow"))
    stress_breaks = bool((scenario_takeaways or {}).get("stress_breaks_candidate_support"))
    downside_not_contained = not bool((scenario_takeaways or {}).get("downside_damage_is_contained", True))
    instability_score = 0
    if path_quality_label == "fragile":
        instability_score += 2
    elif path_quality_label == "noisy":
        instability_score += 1
    if candidate_fragility_label == "acute":
        instability_score += 2
    elif candidate_fragility_label == "fragile":
        instability_score += 1
    if threshold_drift_direction == "toward_weakening":
        instability_score += 1
    if volatility_outlook == "elevated":
        instability_score += 1
    if narrow_support:
        instability_score += 1
    if stress_breaks:
        instability_score += 1
    if downside_not_contained:
        instability_score += 1
    if direct_convex_exception:
        instability_score = max(0, instability_score - 2)
    if label in {"strong", "usable", "usable_with_caution"} and instability_score >= 4:
        label = "unstable"
        quality_flags.append("support_unstable")
    elif proxy_cap == "usable_with_caution" and label == "usable":
        label = _demote_usefulness(label, proxy_cap)
        quality_flags.append("proxy_strength_capped")
    if uses_proxy_series and threshold_drift_direction == "toward_weakening" and label in {"usable", "usable_with_caution"}:
        quality_flags.append("proxy_drift_caution")
    return label, quality_flags


def _empty_support(*, candidate_id: str, provider_source: str | None, suppression_reason: str, eligibility_state: str) -> dict[str, Any]:
    usefulness_label = "suppressed"
    provider = _provider_name(provider_source)
    freshness_state = "unavailable"
    market_setup_state = "unavailable"
    timing_state, timing_reasons = _timing_state_for_suppression(suppression_reason)
    return {
        "candidate_id": candidate_id,
        "eligibility_state": eligibility_state,
        "usefulness_label": usefulness_label,
        "suppression_reason": suppression_reason,
        "market_setup_state": market_setup_state,
        "freshness_state": freshness_state,
        "observed_series": [],
        "projected_series": [],
        "input_timestamps": [],
        "output_timestamps": [],
        "uncertainty_band": None,
        "volatility_outlook": None,
        "path_stability": None,
        "path_quality_label": None,
        "path_quality_score": None,
        "candidate_fragility_label": None,
        "candidate_fragility_score": None,
        "threshold_map": [],
        "strengthening_threshold": None,
        "weakening_threshold": None,
        "current_distance_to_strengthening": None,
        "current_distance_to_weakening": None,
        "threshold_drift_direction": None,
        "scenario_summary": [],
        "scenario_takeaways": None,
        "candidate_implication": "Market path support is not active enough to change the current candidate read.",
        "timing_state": timing_state,
        "timing_label": _timing_label(timing_state),
        "timing_reasons": timing_reasons,
        "timing_artifact_valid": False,
        "timing_artifact_schema_status": "unavailable",
        "market_path_case_family": _market_path_case_family(usefulness_label=usefulness_label, uses_proxy_series=False),
        "market_path_objective": _market_path_objective(usefulness_label),
        "market_path_case_note": "Market-path support is currently suppressed.",
        "support_provenance": {
            "runtime_canonical_provider": "twelve_data",
            "history_provider_name": None,
            "series_role": None,
            "uses_proxy_series": False,
            "recovered_by_secondary_provider": False,
            "last_good_artifact_served": False,
            "provider_source": provider_source,
            "direct_support_unavailable_reason": suppression_reason,
        },
        "truth_manifest": {
            "model_family": _MODEL_NAME,
            "checkpoint": _MODEL_VERSION,
            "service_name": _SERVICE_NAME,
            "provider": provider,
            "driving_symbol": None,
            "driving_series_role": None,
            "forecast_horizon": _HORIZON,
            "forecast_interval": _INTERVAL,
            "freshness_state": freshness_state,
        },
        "model_family": _MODEL_NAME,
        "checkpoint": _MODEL_VERSION,
        "tokenizer": _MODEL_TOKENIZER,
        "wrapper_class": _WRAPPER_CLASS,
        "service_name": _SERVICE_NAME,
        "runtime_engine": _RUNTIME_ENGINE,
        "provider": provider,
        "driving_symbol": None,
        "driving_series_role": None,
        "proxy_symbol": None,
        "proxy_reason": None,
        "liquidity_feature_mode": "unknown",
        "volume_available": False,
        "amount_available": False,
        "sampling_summary": {
            "sampling_mode": "unavailable",
            "temperature": None,
            "top_p": None,
            "seed_policy": "none",
            "sample_path_count": 0,
            "summary_method": "unavailable",
        },
        "threshold_context": {
            "summary": "Threshold context is unavailable.",
            "nearest_threshold_id": None,
            "drift_direction": None,
            "strengthening": None,
            "weakening": None,
            "stress": None,
        },
        "scenario_endpoint_summary": [],
        "generated_at": _now_iso(),
        "provider_source": provider_source,
        "forecast_horizon": _HORIZON,
        "forecast_interval": _INTERVAL,
        "quality_flags": [suppression_reason],
        "series_quality_summary": None,
        "model_metadata": {
            "model_name": _MODEL_NAME,
            "model_version": _MODEL_VERSION,
            "support_semantics_version": _SUPPORT_SEMANTICS_VERSION,
        },
    }


def _hard_failure_reason(series_quality_summary: dict[str, Any], *, row_count: int) -> tuple[str, str] | None:
    quality_label = str(series_quality_summary.get("quality_label") or "")
    stale_days = int(series_quality_summary.get("stale_days") or 0)
    missing_ratio = float(series_quality_summary.get("missing_bar_ratio") or 0.0)
    if row_count < _MIN_HISTORY_BARS:
        return ("insufficient_history", "insufficient_history")
    if quality_label == "broken" or missing_ratio > _HARD_MISSING_RATIO:
        return ("quality_degraded", "degraded")
    if stale_days > _HARD_STALE_DAYS:
        return ("stale_series", "stale")
    return None


def _usefulness_label(
    *,
    series_quality_summary: dict[str, Any],
    uncertainty_width: float,
    path_stability: str,
    volatility_outlook: str,
) -> str:
    failure = _hard_failure_reason(series_quality_summary, row_count=int(series_quality_summary.get("bars_present") or 0))
    if failure is not None:
        return "suppressed"
    score = 0
    stale_days = int(series_quality_summary.get("stale_days") or 0)
    missing_ratio = float(series_quality_summary.get("missing_bar_ratio") or 0.0)
    quality_label = str(series_quality_summary.get("quality_label") or "")
    uses_proxy_series = bool(series_quality_summary.get("uses_proxy_series"))
    if uncertainty_width <= 0.035:
        score += 2
    elif uncertainty_width <= 0.08:
        score += 1
    elif uncertainty_width > 0.14:
        score -= 2
    if path_stability == "stable":
        score += 2
    elif path_stability == "balanced":
        score += 1
    else:
        score -= 2
    if volatility_outlook == "contained":
        score += 1
    elif volatility_outlook == "elevated":
        score -= 1
    if stale_days <= 2:
        score += 1
    elif stale_days > _CAUTION_STALE_DAYS:
        score -= 1
    if missing_ratio <= 0.02:
        score += 1
    elif missing_ratio > 0.08:
        score -= 1
    if quality_label == "degraded":
        score -= 1
    elif quality_label == "watch":
        score += 0
    if uses_proxy_series:
        score -= 1
    if score >= 5:
        return "strong"
    if score >= 2:
        return "usable"
    if score >= 0:
        return "usable_with_caution"
    return "unstable"


def _path_stability_label(uncertainty_width: float) -> str:
    if uncertainty_width <= 0.04:
        return "stable"
    if uncertainty_width <= 0.1:
        return "balanced"
    return "fragile"


def _volatility_outlook(projected_returns: np.ndarray, historical_returns: np.ndarray) -> str:
    projected_std = float(np.std(projected_returns)) if projected_returns.size else 0.0
    historical_std = float(np.std(historical_returns)) if historical_returns.size else 0.0
    if historical_std <= 0:
        return "unresolved"
    ratio = projected_std / historical_std
    if ratio >= 1.15:
        return "elevated"
    if ratio <= 0.85:
        return "contained"
    return "stable"


def _candidate_implication(
    *,
    usefulness_label: str,
    path_stability: str,
    volatility_outlook: str,
    threshold_map: list[dict[str, Any]],
    uses_proxy_series: bool,
) -> str:
    if usefulness_label in {"suppressed", "unstable", "weak"}:
        return "Market path support is too weak to change the current candidate view."
    downside = next((item for item in threshold_map if item.get("threshold_id") == "downside_case"), None)
    stress = next((item for item in threshold_map if item.get("threshold_id") == "stress_case"), None)
    relation = str((downside or {}).get("relation") or "")
    proxy_note = " Proxy series is being used, so keep the signal secondary." if uses_proxy_series else ""
    if usefulness_label in {"strong", "usable"} and relation == "below" and volatility_outlook == "contained" and path_stability == "stable":
        return f"Projected downside remains contained enough to leave the current candidate read unchanged.{proxy_note}"
    if stress and str(stress.get("relation") or "") == "above":
        return f"Stress-path support still sits above the current anchor, which keeps the market-path read supportive but bounded.{proxy_note}"
    if usefulness_label == "usable_with_caution":
        return f"Projected path is usable with caution, so keep decision truth and implementation evidence ahead of it.{proxy_note}"
    return f"Projected path remains usable as bounded scenario support, but keep implementation truth and evidence quality ahead of it.{proxy_note}"


def _find_threshold(threshold_map: list[dict[str, Any]], threshold_id: str) -> dict[str, Any] | None:
    return next((item for item in threshold_map if str(item.get("threshold_id") or "") == threshold_id), None)


def _delta_pct(threshold: dict[str, Any] | None) -> float | None:
    if threshold is None:
        return None
    value = threshold.get("delta_pct")
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _path_quality_semantics(
    *,
    series_quality_summary: dict[str, Any],
    threshold_map: list[dict[str, Any]],
    uncertainty_width: float,
    path_stability: str,
    volatility_outlook: str,
) -> tuple[str, float]:
    downside = _find_threshold(threshold_map, "downside_case")
    stress = _find_threshold(threshold_map, "stress_case")
    score = 82.0
    score -= min(35.0, uncertainty_width * 240.0)
    score -= min(16.0, float(series_quality_summary.get("stale_days") or 0) * 2.0)
    score -= min(20.0, float(series_quality_summary.get("missing_bar_ratio") or 0.0) * 180.0)
    if bool(series_quality_summary.get("uses_proxy_series")):
        score -= 8.0
    if volatility_outlook == "elevated":
        score -= 12.0
    elif volatility_outlook == "contained":
        score += 4.0
    if path_stability == "stable":
        score += 6.0
    elif path_stability == "fragile":
        score -= 10.0
    if str((downside or {}).get("relation") or "") == "above":
        score += 8.0
    elif str((downside or {}).get("relation") or "") == "below":
        score -= 6.0
    if str((stress or {}).get("relation") or "") == "above":
        score += 5.0
    elif str((stress or {}).get("relation") or "") == "below":
        score -= 12.0
    bounded = max(0.0, min(100.0, score))
    if bounded >= 78.0:
        return "clean", round(bounded, 1)
    if bounded >= 62.0:
        return "balanced", round(bounded, 1)
    if bounded >= 45.0:
        return "noisy", round(bounded, 1)
    return "fragile", round(bounded, 1)


def _candidate_fragility_semantics(
    *,
    series_quality_summary: dict[str, Any],
    threshold_map: list[dict[str, Any]],
    uncertainty_width: float,
    path_stability: str,
    volatility_outlook: str,
) -> tuple[str, float]:
    downside = _find_threshold(threshold_map, "downside_case")
    stress = _find_threshold(threshold_map, "stress_case")
    score = 22.0
    score += min(36.0, uncertainty_width * 260.0)
    score += min(14.0, float(series_quality_summary.get("stale_days") or 0) * 1.6)
    score += min(18.0, float(series_quality_summary.get("missing_bar_ratio") or 0.0) * 140.0)
    if bool(series_quality_summary.get("uses_proxy_series")):
        score += 8.0
    if volatility_outlook == "elevated":
        score += 12.0
    elif volatility_outlook == "contained":
        score -= 4.0
    if path_stability == "stable":
        score -= 10.0
    elif path_stability == "fragile":
        score += 12.0
    if str((downside or {}).get("relation") or "") == "below":
        score += 10.0
    if str((stress or {}).get("relation") or "") == "below":
        score += 16.0
    bounded = max(0.0, min(100.0, score))
    if bounded <= 28.0:
        return "resilient", round(bounded, 1)
    if bounded <= 48.0:
        return "watchful", round(bounded, 1)
    if bounded <= 68.0:
        return "fragile", round(bounded, 1)
    return "acute", round(bounded, 1)


def _threshold_drift_direction(
    *,
    base_threshold: dict[str, Any] | None,
    downside_threshold: dict[str, Any] | None,
    stress_threshold: dict[str, Any] | None,
    path_stability: str,
    volatility_outlook: str,
) -> str:
    base_delta = _delta_pct(base_threshold) or 0.0
    downside_delta = _delta_pct(downside_threshold) or 0.0
    stress_delta = _delta_pct(stress_threshold) or 0.0
    if base_delta >= 2.5 and downside_delta >= -2.0 and path_stability != "fragile":
        return "toward_strengthening"
    if base_delta <= 0.0 or stress_delta <= -6.0 or volatility_outlook == "elevated":
        return "toward_weakening"
    return "balanced"


def _scenario_takeaways(
    *,
    threshold_map: list[dict[str, Any]],
    uncertainty_width: float,
    path_stability: str,
) -> dict[str, Any]:
    downside = _find_threshold(threshold_map, "downside_case")
    stress = _find_threshold(threshold_map, "stress_case")
    downside_delta = _delta_pct(downside) or 0.0
    stress_delta = _delta_pct(stress) or 0.0
    favorable_case_survives_mild_stress = downside_delta >= -3.5
    favorable_case_is_narrow = uncertainty_width > 0.09 or path_stability == "fragile"
    downside_damage_is_contained = downside_delta >= -5.0
    stress_breaks_candidate_support = stress_delta < -6.0 or str((stress or {}).get("relation") or "") == "below"
    return {
        "favorable_case_survives_mild_stress": favorable_case_survives_mild_stress,
        "favorable_case_is_narrow": favorable_case_is_narrow,
        "downside_damage_is_contained": downside_damage_is_contained,
        "stress_breaks_candidate_support": stress_breaks_candidate_support,
    }


def _threshold_map(current_value: float, base_end: float, downside_end: float, stress_end: float) -> list[dict[str, Any]]:
    rows = []
    for threshold_id, label, value in (
        ("base_case", "Base path", base_end),
        ("downside_case", "Downside path", downside_end),
        ("stress_case", "Stress path", stress_end),
    ):
        relation = "above" if value >= current_value else "below"
        rows.append(
            {
                "threshold_id": threshold_id,
                "label": label,
                "value": round(float(value), 4),
                "relation": relation,
                "delta_pct": round(((float(value) - current_value) / abs(current_value or 1.0)) * 100.0, 3),
                "note": f"{label} ends {relation} the current anchor.",
            }
        )
    return rows


def _scenario_summary(
    *,
    timestamps: list[str],
    base_path: dict[str, Any],
    downside_path: dict[str, Any],
    stress_path: dict[str, Any],
    usefulness_label: str,
) -> list[dict[str, Any]]:
    return [
        {
            "scenario_type": "base",
            "label": "Base",
            "summary": "Base path is the retained p50 scenario from the seeded canonical sample set.",
            "path": _point_series(timestamps, np.asarray(base_path["close"], dtype=float)),
            "sample_index": int(base_path.get("path_index") or 0),
            "seed": int(base_path.get("seed") or 0),
            "usefulness_label": usefulness_label,
        },
        {
            "scenario_type": "downside",
            "label": "Downside",
            "summary": "Downside path is the retained lower-percentile scenario from the same canonical sample set.",
            "path": _point_series(timestamps, np.asarray(downside_path["close"], dtype=float)),
            "sample_index": int(downside_path.get("path_index") or 0),
            "seed": int(downside_path.get("seed") or 0),
            "usefulness_label": usefulness_label,
        },
        {
            "scenario_type": "stress",
            "label": "Stress",
            "summary": "Stress path is the retained tail scenario from the same canonical sample set, not a pointwise minimum mashup.",
            "path": _point_series(timestamps, np.asarray(stress_path["close"], dtype=float)),
            "sample_index": int(stress_path.get("path_index") or 0),
            "seed": int(stress_path.get("seed") or 0),
            "usefulness_label": usefulness_label,
        },
    ]


def _market_path_case_family(*, usefulness_label: str, uses_proxy_series: bool) -> str:
    if usefulness_label == "suppressed":
        return "suppressed_support"
    if usefulness_label == "unstable":
        return "unstable_support"
    if uses_proxy_series or usefulness_label == "usable_with_caution":
        return "cautionary_support"
    return "bounded_support"


def _market_path_objective(usefulness_label: str) -> str:
    if usefulness_label == "suppressed":
        return "Do not use market-path output in the decision until direct or approved-proxy support is restored."
    return (
        "Use market-path output only to frame scenario range, threshold monitoring, and downside shape. "
        "It cannot overrule sleeve fit, implementation truth, or source integrity."
    )


def _market_path_case_note(
    *,
    usefulness_label: str,
    uses_proxy_series: bool,
    path_quality_label: str,
    candidate_fragility_label: str,
    threshold_drift_direction: str,
) -> str:
    provenance_note = " Proxy-backed history keeps the case secondary." if uses_proxy_series else ""
    if usefulness_label == "suppressed":
        return "Market-path support is suppressed and should not influence the candidate decision right now."
    if usefulness_label == "unstable":
        return (
            f"Market-path support is unstable because path quality is {path_quality_label} and fragility is "
            f"{candidate_fragility_label}; threshold drift is {threshold_drift_direction}.{provenance_note}"
        )
    if usefulness_label == "usable_with_caution":
        return (
            f"Market-path support is usable with caution; path quality is {path_quality_label}, fragility is "
            f"{candidate_fragility_label}, and drift is {threshold_drift_direction}.{provenance_note}"
        )
    return (
        f"Market-path support is bounded but usable; path quality is {path_quality_label}, fragility is "
        f"{candidate_fragility_label}, and drift is {threshold_drift_direction}.{provenance_note}"
    )


def _support_provenance(
    *,
    provider_source: str,
    identity: dict[str, Any],
    rows: list[dict[str, Any]],
    series_quality_summary: dict[str, Any],
    quality_flags: list[str],
) -> dict[str, Any]:
    history_provider_name = str((rows[-1] if rows else {}).get("provider") or "").strip() or None
    route_state = _route_state(identity, quality_flags)
    return {
        "runtime_canonical_provider": "twelve_data",
        "history_provider_name": history_provider_name,
        "series_role": str(identity.get("series_role") or ""),
        "uses_proxy_series": bool(series_quality_summary.get("uses_proxy_series")),
        "recovered_by_secondary_provider": history_provider_name not in {None, "", "twelve_data"},
        "last_good_artifact_served": "last_good_artifact_served" in quality_flags,
        "provider_source": provider_source,
        "route_state": route_state,
        "driving_symbol": _driving_symbol(identity),
        "proxy_symbol": _proxy_symbol(identity),
        "direct_support_unavailable_reason": (
            "approved_proxy_used"
            if str(identity.get("series_role") or "") == "approved_proxy"
            else None
        ),
    }


def _threshold_context(
    *,
    threshold_map: list[dict[str, Any]],
    threshold_drift_direction: str,
) -> dict[str, Any]:
    strengthening = _find_threshold(threshold_map, "base_case")
    weakening = _find_threshold(threshold_map, "downside_case")
    stress = _find_threshold(threshold_map, "stress_case")
    candidates = [item for item in (strengthening, weakening, stress) if item is not None]
    nearest = None
    if candidates:
        nearest = min(
            candidates,
            key=lambda item: abs(float(item.get("delta_pct") or 0.0)),
        )
    return {
        "summary": (
            f"Nearest threshold is {str((nearest or {}).get('label') or 'unavailable')} and drift is "
            f"{threshold_drift_direction.replace('_', ' ')}."
            if nearest is not None
            else "Threshold context is unavailable."
        ),
        "nearest_threshold_id": str((nearest or {}).get("threshold_id") or "") or None,
        "drift_direction": threshold_drift_direction,
        "strengthening": strengthening,
        "weakening": weakening,
        "stress": stress,
    }


def _scenario_endpoint_summary(
    *,
    scenario_summary: list[dict[str, Any]],
    current_value: float,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for scenario in scenario_summary:
        path = list(scenario.get("path") or [])
        endpoint = path[-1] if path else {}
        endpoint_value = float(endpoint.get("value")) if endpoint.get("value") is not None else None
        items.append(
            {
                "scenario_type": str(scenario.get("scenario_type") or ""),
                "label": str(scenario.get("label") or ""),
                "endpoint_value": endpoint_value,
                "endpoint_timestamp": str(endpoint.get("timestamp") or "") or None,
                "summary": str(scenario.get("summary") or ""),
                "sample_index": scenario.get("sample_index"),
                "seed": scenario.get("seed"),
                "endpoint_delta_pct": (
                    round(((float(endpoint_value) - current_value) / abs(current_value or 1.0)) * 100.0, 3)
                    if endpoint_value is not None
                    else None
                ),
            }
        )
    return items


def compact_forecast_support_from_market_path(market_path_support: dict[str, Any] | None) -> dict[str, Any] | None:
    if not market_path_support:
        return None
    provider_source = str(market_path_support.get("provider_source") or "kronos")
    usefulness = str(market_path_support.get("usefulness_label") or "suppressed")
    quality_flags = list(market_path_support.get("quality_flags") or [])
    degraded_state = str(market_path_support.get("suppression_reason") or "") or None
    if degraded_state is None and "last_good_artifact_served" in quality_flags:
        degraded_state = "last_good_artifact_served"
    if degraded_state is None:
        setup_state = str(market_path_support.get("market_setup_state") or "").strip()
        degraded_state = setup_state if setup_state in {"degraded", "stale", "unavailable"} else None
    support_strength = {
        "strong": "strong",
        "usable": "moderate",
        "usable_with_caution": "cautious",
        "unstable": "weak",
        "suppressed": "support_only",
    }.get(usefulness, "support_only")
    quality_summary = dict(market_path_support.get("series_quality_summary") or {})
    confidence_label = str(dict(market_path_support.get("model_metadata") or {}).get("confidence_label") or "").strip() or None
    return {
        "provider": provider_source,
        "model_name": str(dict(market_path_support.get("model_metadata") or {}).get("model_name") or _MODEL_NAME),
        "horizon": int(market_path_support.get("forecast_horizon") or _HORIZON),
        "support_strength": support_strength,
        "confidence_summary": str(market_path_support.get("candidate_implication") or "Market path support remains bounded."),
        "degraded_state": degraded_state,
        "generated_at": str(market_path_support.get("generated_at") or _now_iso()),
        "uncertainty_width_label": str(dict(market_path_support.get("model_metadata") or {}).get("uncertainty_width_label") or usefulness),
        "scenario_support_strength": usefulness,
        "persistence_score": float(market_path_support.get("path_quality_score") or 0.0),
        "fade_risk": float(market_path_support.get("candidate_fragility_score") or 0.0),
        "trigger_distance": float(market_path_support.get("current_distance_to_strengthening") or 0.0) if market_path_support.get("current_distance_to_strengthening") is not None else None,
        "trigger_pressure": float(market_path_support.get("current_distance_to_weakening") or 0.0) if market_path_support.get("current_distance_to_weakening") is not None else None,
        "path_asymmetry": None,
        "uncertainty_width_score": float(dict(market_path_support.get("model_metadata") or {}).get("uncertainty_width_score") or 0.0),
        "regime_alignment_score": None,
        "cross_asset_confirmation_score": None,
        "confidence_label": confidence_label,
        "escalation_flag": (
            usefulness in {"unstable", "suppressed"}
            or str(quality_summary.get("quality_label") or "") == "broken"
            or str(market_path_support.get("market_setup_state") or "").strip() in {"degraded", "stale", "unavailable"}
        ),
    }


def _artifact_requires_upgrade(market_path_support: dict[str, Any] | None) -> bool:
    if not isinstance(market_path_support, dict):
        return True
    usefulness = str(market_path_support.get("usefulness_label") or "")
    if usefulness in {"high", "moderate", "weak"}:
        return True
    required_fields = (
        "market_setup_state",
        "freshness_state",
        "driving_symbol",
        "driving_series_role",
        "output_timestamps",
        "sampling_summary",
        "scenario_endpoint_summary",
        "liquidity_feature_mode",
        "path_quality_label",
        "path_quality_score",
        "candidate_fragility_label",
        "candidate_fragility_score",
        "threshold_drift_direction",
        "scenario_takeaways",
        "timing_state",
        "timing_reasons",
        "timing_artifact_valid",
    )
    if any(field not in market_path_support for field in required_fields):
        return True
    if not isinstance(market_path_support.get("scenario_endpoint_summary"), list):
        return True
    if not isinstance(market_path_support.get("sampling_summary"), dict):
        return True
    if not isinstance(market_path_support.get("scenario_takeaways"), dict):
        return True
    return False


def market_path_artifact_requires_upgrade(market_path_support: dict[str, Any] | None) -> bool:
    return _artifact_requires_upgrade(market_path_support)


def _artifact_safe_for_last_good(market_path_support: dict[str, Any] | None) -> bool:
    if not isinstance(market_path_support, dict):
        return False
    minimal_fields = (
        "usefulness_label",
        "forecast_horizon",
        "provider_source",
        "scenario_summary",
    )
    return all(field in market_path_support for field in minimal_fields)


def _load_kronos_predictor():
    with _PREDICTOR_LOCK:
        cached = _RUNTIME_CACHE.get("predictor")
        if cached is not None:
            cached_context = int(getattr(cached, "max_context", _MAX_CONTEXT) or _MAX_CONTEXT)
            if cached_context == _MAX_CONTEXT:
                return cached
            _RUNTIME_CACHE["predictor"] = None
            _RUNTIME_CACHE["load_failed"] = None
        if _RUNTIME_CACHE.get("load_failed") is not None:
            raise RuntimeError(str(_RUNTIME_CACHE["load_failed"]))
        kronos_root = get_repo_root() / "Kronos"
        if str(kronos_root) not in sys.path:
            sys.path.insert(0, str(kronos_root))
        try:
            import torch
            from model import Kronos, KronosPredictor, KronosTokenizer
        except Exception as exc:  # noqa: BLE001
            _RUNTIME_CACHE["load_failed"] = f"Kronos import failed: {exc}"
            raise RuntimeError(str(_RUNTIME_CACHE["load_failed"])) from exc
        device = "cuda:0" if torch.cuda.is_available() else "mps" if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available() else "cpu"
        try:
            tokenizer = KronosTokenizer.from_pretrained(_MODEL_TOKENIZER)
            model = Kronos.from_pretrained(_MODEL_VERSION)
            predictor = KronosPredictor(model, tokenizer, device=device, max_context=_MAX_CONTEXT)
        except Exception as exc:  # noqa: BLE001
            _RUNTIME_CACHE["load_failed"] = f"Kronos runtime load failed: {exc}"
            raise RuntimeError(str(_RUNTIME_CACHE["load_failed"])) from exc
        _RUNTIME_CACHE["predictor"] = predictor
        return predictor


def _clear_kronos_runtime_cache(*, reason: str | None = None) -> None:
    with _PREDICTOR_LOCK:
        _RUNTIME_CACHE["predictor"] = None
        _RUNTIME_CACHE["load_failed"] = None
        try:
            import torch

            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception:
            pass
    if reason:
        logger.warning("Blueprint Kronos predictor cache cleared", extra={"reason": reason})


def _is_retriable_kronos_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    if not message:
        return False
    return (
        "size of tensor" in message
        or "must match the size of tensor" in message
        or "non-singleton dimension" in message
        or "shape" in message
    )


def _validate_ohlcva_geometry(pred_df: Any) -> None:
    try:
        frame = pred_df[["open", "high", "low", "close", "volume", "amount"]]
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Predicted path missing OHLCVA columns: {exc}") from exc
    if frame.isnull().values.any():
        raise ValueError("Predicted path contains NaN values.")
    if ((frame["high"] < frame[["open", "close", "low"]].max(axis=1)) | (frame["low"] > frame[["open", "close", "high"]].min(axis=1))).any():
        raise ValueError("Predicted bar geometry is invalid.")
    if (frame["volume"] < 0).any() or (frame["amount"] < 0).any():
        raise ValueError("Predicted liquidity fields are negative.")


def _predict_sample_paths(
    predictor: Any,
    *,
    adapter_payload: dict[str, Any],
    seed_list: list[int],
) -> list[dict[str, Any]]:
    outputs: list[dict[str, Any]] = []
    dataframe = adapter_payload["df"]
    x_timestamp = adapter_payload["x_timestamp"]
    y_timestamp = adapter_payload["y_timestamp"]
    for path_index, seed in enumerate(seed_list):
        _set_runtime_seed(seed)
        pred_df = predictor.predict(
            df=dataframe,
            x_timestamp=x_timestamp,
            y_timestamp=y_timestamp,
            pred_len=len(y_timestamp),
            T=_SAMPLE_TEMPERATURE,
            top_p=_SAMPLE_TOP_P,
            sample_count=1,
            verbose=False,
        )
        _validate_ohlcva_geometry(pred_df)
        close_values = np.asarray(pred_df["close"].tolist(), dtype=float)
        outputs.append(
            {
                "path_index": path_index,
                "seed": seed,
                "close": close_values,
                "ohlcva": pred_df[["open", "high", "low", "close", "volume", "amount"]].copy(),
            }
        )
    return outputs


def _run_kronos_samples(adapter_payload: dict[str, Any], *, sample_runs: int = _SAMPLE_PATH_COUNT) -> list[dict[str, Any]]:
    seed_list = _sample_seed_list(count=sample_runs)
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            with _PREDICTOR_LOCK:
                predictor = _load_kronos_predictor()
                return _predict_sample_paths(
                    predictor,
                    adapter_payload=adapter_payload,
                    seed_list=seed_list,
                )
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt == 0 and _is_retriable_kronos_error(exc):
                _clear_kronos_runtime_cache(reason=f"retriable_runtime_error:{exc}")
                continue
            raise
    if last_error is not None:
        raise last_error
    raise RuntimeError("Kronos execution failed without an explicit exception.")


def _decorate_last_good_artifact(
    market_path_support: dict[str, Any],
    *,
    error_message: str,
    series_quality_summary: dict[str, Any],
) -> dict[str, Any]:
    support = dict(market_path_support)
    quality_flags = sorted(set(list(support.get("quality_flags") or []) + ["last_good_artifact_served"]))
    support["quality_flags"] = quality_flags
    support["series_quality_summary"] = series_quality_summary
    support["freshness_state"] = "last_good"
    support["market_setup_state"] = "stale"
    provenance = dict(support.get("support_provenance") or {})
    provenance["last_good_artifact_served"] = True
    provenance["route_state"] = "last_good_artifact"
    support["support_provenance"] = provenance
    truth_manifest = dict(support.get("truth_manifest") or {})
    truth_manifest["freshness_state"] = "last_good"
    support["truth_manifest"] = truth_manifest
    model_metadata = dict(support.get("model_metadata") or {})
    model_metadata["last_good_artifact_served"] = True
    model_metadata["last_good_artifact_served_at"] = _now_iso()
    model_metadata["last_model_error"] = error_message
    support["model_metadata"] = model_metadata
    return support


def _build_support_from_samples(
    *,
    conn: sqlite3.Connection,
    candidate_id: str,
    identity: dict[str, Any],
    rows: list[dict[str, Any]],
    sample_paths: list[dict[str, Any]],
    adapter_payload: dict[str, Any],
    series_quality_summary: dict[str, Any],
) -> dict[str, Any]:
    observed_rows = rows[-_OBSERVED_POINTS:]
    observed_series = [_point(item["timestamp_utc"], float(item["close"])) for item in observed_rows]
    current_value = float(observed_rows[-1]["close"])
    timestamps = [str(item) for item in list(adapter_payload.get("output_timestamps") or [])]
    input_timestamps = [str(item) for item in list(adapter_payload.get("input_timestamps") or [])]
    close_samples = (
        np.vstack([np.asarray(dict(path).get("close", []), dtype=float) for path in sample_paths])
        if sample_paths
        else np.empty((0, len(timestamps)), dtype=float)
    )
    if close_samples.size == 0:
        support = _empty_support(
            candidate_id=candidate_id,
            provider_source=_provider_source_for_identity(identity),
            suppression_reason="model_execution_failed",
            eligibility_state="failed",
        )
        support["series_quality_summary"] = series_quality_summary
        support["observed_series"] = observed_series
        return support
    endpoint_values = close_samples[:, -1]
    base_index = _representative_path_index(endpoint_values, 0.5)
    downside_index = _representative_path_index(endpoint_values, 0.25)
    stress_index = _representative_path_index(endpoint_values, 0.1)
    base_path = dict(sample_paths[base_index])
    downside_path = dict(sample_paths[downside_index])
    stress_path = dict(sample_paths[stress_index])
    base_close = np.asarray(base_path["close"], dtype=float)
    downside_close = np.asarray(downside_path["close"], dtype=float)
    stress_close = np.asarray(stress_path["close"], dtype=float)
    lower_close = np.quantile(close_samples, 0.1, axis=0)
    upper_close = np.quantile(close_samples, 0.9, axis=0)
    uncertainty_width = float((upper_close[-1] - lower_close[-1]) / abs(current_value or 1.0))
    path_stability = _path_stability_label(uncertainty_width)
    historical_returns = np.diff(np.asarray([float(item["close"]) for item in observed_rows], dtype=float))
    projected_returns = np.diff(base_close)
    volatility_outlook = _volatility_outlook(projected_returns, historical_returns)
    base_usefulness_label = _usefulness_label(
        series_quality_summary=series_quality_summary,
        uncertainty_width=uncertainty_width,
        path_stability=path_stability,
        volatility_outlook=volatility_outlook,
    )
    threshold_map = _threshold_map(current_value, float(base_close[-1]), float(downside_close[-1]), float(stress_close[-1]))
    strengthening_threshold = _find_threshold(threshold_map, "base_case")
    weakening_threshold = _find_threshold(threshold_map, "downside_case")
    stress_threshold = _find_threshold(threshold_map, "stress_case")
    threshold_drift_direction = _threshold_drift_direction(
        base_threshold=strengthening_threshold,
        downside_threshold=weakening_threshold,
        stress_threshold=stress_threshold,
        path_stability=path_stability,
        volatility_outlook=volatility_outlook,
    )
    path_quality_label, path_quality_score = _path_quality_semantics(
        series_quality_summary=series_quality_summary,
        threshold_map=threshold_map,
        uncertainty_width=uncertainty_width,
        path_stability=path_stability,
        volatility_outlook=volatility_outlook,
    )
    candidate_fragility_label, candidate_fragility_score = _candidate_fragility_semantics(
        series_quality_summary=series_quality_summary,
        threshold_map=threshold_map,
        uncertainty_width=uncertainty_width,
        path_stability=path_stability,
        volatility_outlook=volatility_outlook,
    )
    scenario_takeaways = _scenario_takeaways(
        threshold_map=threshold_map,
        uncertainty_width=uncertainty_width,
        path_stability=path_stability,
    )
    sleeve_keys = _candidate_sleeve_keys(conn, candidate_id)
    usefulness_label, policy_flags = _recalibrate_usefulness_label(
        base_usefulness_label=base_usefulness_label,
        series_quality_summary=series_quality_summary,
        sleeve_keys=sleeve_keys,
        path_quality_label=path_quality_label,
        candidate_fragility_label=candidate_fragility_label,
        threshold_drift_direction=threshold_drift_direction,
        volatility_outlook=volatility_outlook,
        scenario_takeaways=scenario_takeaways,
    )
    candidate_implication = _candidate_implication(
        usefulness_label=usefulness_label,
        path_stability=path_stability,
        volatility_outlook=volatility_outlook,
        threshold_map=threshold_map,
        uses_proxy_series=bool(series_quality_summary.get("uses_proxy_series")),
    )
    quality_flags: list[str] = []
    if usefulness_label in {"usable_with_caution", "unstable"}:
        quality_flags.append("output_caution")
    if usefulness_label == "unstable":
        quality_flags.append("output_unstable")
    if str(series_quality_summary.get("quality_label") or "") == "degraded":
        quality_flags.append("series_quality_degraded")
    if bool(series_quality_summary.get("uses_proxy_series")):
        quality_flags.append("approved_proxy_series")
    if str(adapter_payload.get("liquidity_feature_mode") or "").strip() == "price_only":
        quality_flags.append("price_only_liquidity_mode")
    if len(sample_paths) < _SAMPLE_PATH_COUNT:
        quality_flags.append("sampling_degraded")
    quality_flags.extend(policy_flags)
    quality_flags = sorted(set(quality_flags))
    freshness_state = _freshness_state(series_quality_summary=series_quality_summary, quality_flags=quality_flags)
    market_setup_state = _market_setup_state(
        support={
            "suppression_reason": "output_suppressed" if usefulness_label == "suppressed" else None,
            "freshness_state": freshness_state,
            "liquidity_feature_mode": adapter_payload.get("liquidity_feature_mode"),
        },
        series_quality_summary=series_quality_summary,
        quality_flags=quality_flags,
    )
    confidence_label = _confidence_label(
        series_quality_summary=series_quality_summary,
        usefulness_label=usefulness_label,
        market_setup_state=market_setup_state,
        uncertainty_width=uncertainty_width,
        path_count=len(sample_paths),
    )
    scope_key, scope_label = _resolve_scope_manifest(conn, candidate_id=candidate_id, identity=identity)
    model_metadata = {
        "model_name": _MODEL_NAME,
        "model_version": _MODEL_VERSION,
        "model_family": _MODEL_NAME,
        "checkpoint": _MODEL_VERSION,
        "tokenizer": _MODEL_TOKENIZER,
        "wrapper_class": _WRAPPER_CLASS,
        "service_name": _SERVICE_NAME,
        "runtime_engine": _RUNTIME_ENGINE,
        "support_semantics_version": _SUPPORT_SEMANTICS_VERSION,
        "uncertainty_width_score": round(uncertainty_width, 4),
        "uncertainty_width_label": (
            "tight"
            if uncertainty_width <= 0.035
            else "bounded"
            if uncertainty_width <= 0.08
            else "wide"
            if uncertainty_width <= 0.14
            else "very_wide"
        ),
        "series_role": identity.get("series_role"),
        "confidence_label": confidence_label,
    }
    provider_source = "twelve_data+kronos" if str(identity.get("series_role") or "") == "direct" else "twelve_data+approved_proxy+kronos"
    scenario_summary = _scenario_summary(
        timestamps=timestamps,
        base_path=base_path,
        downside_path=downside_path,
        stress_path=stress_path,
        usefulness_label=usefulness_label,
    )
    route_state = _route_state(identity, quality_flags)
    driving_symbol = _driving_symbol(identity)
    proxy_symbol = _proxy_symbol(identity)
    provider = _provider_name(provider_source)
    sampling_summary = {
        "sampling_mode": "seeded_single_path_ensemble",
        "temperature": _SAMPLE_TEMPERATURE,
        "top_p": _SAMPLE_TOP_P,
        "seed_policy": f"incrementing_from_{_SAMPLE_SEED_BASE}",
        "seed_count": len(sample_paths),
        "sample_path_count": len(sample_paths),
        "summary_method": "retained_percentile_paths",
        "base_path_index": base_index,
        "downside_path_index": downside_index,
        "stress_path_index": stress_index,
        "seed_manifest": [int(dict(path).get("seed") or 0) for path in sample_paths],
    }
    timing_state, timing_reasons = _timing_state_from_market_path(
        usefulness_label=usefulness_label,
        market_setup_state=market_setup_state,
        path_quality_label=path_quality_label,
        candidate_fragility_label=candidate_fragility_label,
        threshold_drift_direction=threshold_drift_direction,
        uses_proxy_series=bool(series_quality_summary.get("uses_proxy_series")),
        series_quality_summary=series_quality_summary,
    )
    return {
        "candidate_id": candidate_id,
        "eligibility_state": "eligible",
        "usefulness_label": usefulness_label,
        "suppression_reason": "output_suppressed" if usefulness_label == "suppressed" else None,
        "market_setup_state": market_setup_state,
        "freshness_state": freshness_state,
        "timing_state": timing_state,
        "timing_label": _timing_label(timing_state),
        "timing_reasons": timing_reasons,
        "timing_artifact_valid": True,
        "timing_artifact_schema_status": "current_schema",
        "observed_series": observed_series,
        "projected_series": _point_series(timestamps, base_close),
        "input_timestamps": input_timestamps,
        "output_timestamps": timestamps,
        "uncertainty_band": {
            "label": "Central uncertainty band",
            "lower_points": _point_series(timestamps, lower_close),
            "upper_points": _point_series(timestamps, upper_close),
        },
        "volatility_outlook": volatility_outlook,
        "path_stability": path_stability,
        "path_quality_label": path_quality_label,
        "path_quality_score": path_quality_score,
        "candidate_fragility_label": candidate_fragility_label,
        "candidate_fragility_score": candidate_fragility_score,
        "threshold_map": threshold_map,
        "strengthening_threshold": strengthening_threshold,
        "weakening_threshold": weakening_threshold,
        "current_distance_to_strengthening": _delta_pct(strengthening_threshold),
        "current_distance_to_weakening": _delta_pct(weakening_threshold),
        "threshold_drift_direction": threshold_drift_direction,
        "scenario_summary": scenario_summary,
        "scenario_takeaways": scenario_takeaways,
        "candidate_implication": candidate_implication,
        "market_path_case_family": _market_path_case_family(
            usefulness_label=usefulness_label,
            uses_proxy_series=bool(series_quality_summary.get("uses_proxy_series")),
        ),
        "market_path_objective": _market_path_objective(usefulness_label),
        "market_path_case_note": _market_path_case_note(
            usefulness_label=usefulness_label,
            uses_proxy_series=bool(series_quality_summary.get("uses_proxy_series")),
            path_quality_label=path_quality_label,
            candidate_fragility_label=candidate_fragility_label,
            threshold_drift_direction=threshold_drift_direction,
        ),
        "support_provenance": _support_provenance(
            provider_source=provider_source,
            identity=identity,
            rows=rows,
            series_quality_summary=series_quality_summary,
            quality_flags=quality_flags,
        ),
        "truth_manifest": {
            "model_family": _MODEL_NAME,
            "checkpoint": _MODEL_VERSION,
            "service_name": _SERVICE_NAME,
            "provider": provider,
            "driving_symbol": driving_symbol,
            "driving_series_role": str(identity.get("series_role") or ""),
            "forecast_horizon": len(timestamps) or _HORIZON,
            "forecast_interval": _INTERVAL,
            "freshness_state": freshness_state,
        },
        "model_family": _MODEL_NAME,
        "checkpoint": _MODEL_VERSION,
        "tokenizer": _MODEL_TOKENIZER,
        "wrapper_class": _WRAPPER_CLASS,
        "service_name": _SERVICE_NAME,
        "runtime_engine": _RUNTIME_ENGINE,
        "provider": provider,
        "driving_symbol": driving_symbol,
        "driving_series_role": str(identity.get("series_role") or ""),
        "uses_proxy_series": bool(series_quality_summary.get("uses_proxy_series")),
        "proxy_symbol": proxy_symbol,
        "proxy_reason": _proxy_reason(identity),
        "route_state": route_state,
        "scope_key": scope_key,
        "scope_label": scope_label,
        "liquidity_feature_mode": str(adapter_payload.get("liquidity_feature_mode") or "unknown"),
        "volume_available": bool(adapter_payload.get("volume_available")),
        "amount_available": bool(adapter_payload.get("amount_available")),
        "sampling_summary": sampling_summary,
        "sample_path_manifest": [
            {
                "path_index": int(dict(path).get("path_index") or 0),
                "seed": int(dict(path).get("seed") or 0),
                "endpoint_value": round(float(np.asarray(dict(path).get("close", [current_value]), dtype=float)[-1]), 4),
                "endpoint_delta_pct": round(
                    (
                        (float(np.asarray(dict(path).get("close", [current_value]), dtype=float)[-1]) - current_value)
                        / abs(current_value or 1.0)
                    ) * 100.0,
                    3,
                ),
            }
            for path in sample_paths
        ],
        "threshold_context": _threshold_context(
            threshold_map=threshold_map,
            threshold_drift_direction=threshold_drift_direction,
        ),
        "scenario_endpoint_summary": _scenario_endpoint_summary(
            scenario_summary=scenario_summary,
            current_value=current_value,
        ),
        "generated_at": _now_iso(),
        "provider_source": provider_source,
        "forecast_horizon": len(timestamps) or _HORIZON,
        "forecast_interval": _INTERVAL,
        "quality_flags": quality_flags,
        "series_quality_summary": series_quality_summary,
        "model_metadata": model_metadata,
    }


def build_candidate_market_path_support(
    candidate_id: str,
    *,
    allow_refresh: bool = False,
    force_refresh: bool = False,
    require_ui_enabled: bool = True,
) -> dict[str, Any] | None:
    if not _feature_enabled() or (require_ui_enabled and not _ui_enabled()):
        return None
    with _candidate_build_lock(candidate_id):
        with _connection() as conn:
            ensure_blueprint_market_tables(conn)
            identities = ensure_candidate_market_identities(conn, candidate_id)
            if not identities:
                return _empty_support(
                    candidate_id=candidate_id,
                    provider_source="twelve_data",
                    suppression_reason="symbol_mapping_failed",
                    eligibility_state="unavailable",
                )
            identity, rows = _choose_identity_with_series(conn, candidate_id=candidate_id)
            if identity is None:
                return _empty_support(
                    candidate_id=str(candidate_id),
                    provider_source="twelve_data",
                    suppression_reason="symbol_mapping_failed",
                    eligibility_state="unavailable",
                )
            if allow_refresh:
                refresh_candidate_series(conn, candidate_id=candidate_id, stale_only=not force_refresh)
                identity, rows = _choose_identity_with_series(conn, candidate_id=candidate_id)
                if identity is None:
                    return _empty_support(
                        candidate_id=str(candidate_id),
                        provider_source="twelve_data",
                        suppression_reason="symbol_mapping_failed",
                        eligibility_state="unavailable",
                    )
            freshness = check_candidate_series_freshness(conn, candidate_id=str(identity["candidate_id"]), series_role=str(identity["series_role"]))
            series_quality_summary = dict(freshness.get("series_quality_summary") or {})
            hard_failure = _hard_failure_reason(series_quality_summary, row_count=len(rows))
            if hard_failure and hard_failure[0] == "insufficient_history":
                support = _empty_support(
                    candidate_id=str(identity["candidate_id"]),
                    provider_source=_provider_source_for_identity(identity),
                    suppression_reason="insufficient_history",
                    eligibility_state="insufficient_history",
                )
                support["series_quality_summary"] = series_quality_summary
                support["observed_series"] = [_point(item["timestamp_utc"], float(item["close"])) for item in rows[-_OBSERVED_POINTS:]]
                return support
            if hard_failure and hard_failure[0] == "stale_series":
                support = _empty_support(
                    candidate_id=str(identity["candidate_id"]),
                    provider_source=_provider_source_for_identity(identity),
                    suppression_reason="stale_series",
                    eligibility_state="stale",
                )
                support["series_quality_summary"] = series_quality_summary
                support["observed_series"] = [_point(item["timestamp_utc"], float(item["close"])) for item in rows[-_OBSERVED_POINTS:]]
                return support
            if hard_failure and hard_failure[0] == "quality_degraded":
                support = _empty_support(
                    candidate_id=str(identity["candidate_id"]),
                    provider_source=_provider_source_for_identity(identity),
                    suppression_reason="quality_degraded",
                    eligibility_state="degraded",
                )
                support["series_quality_summary"] = series_quality_summary
                support["observed_series"] = [_point(item["timestamp_utc"], float(item["close"])) for item in rows[-_OBSERVED_POINTS:]]
                return support
            input_series_version = latest_series_version(
                conn,
                candidate_id=str(identity["candidate_id"]),
                series_role=str(identity.get("series_role") or "direct"),
                interval=str(identity.get("primary_interval") or _INTERVAL),
            )
            cached = latest_forecast_artifact(conn, candidate_id=str(identity["candidate_id"]))
            if (
                not force_refresh
                and cached is not None
                and str(cached.get("input_series_version") or "") == str(input_series_version or "")
                and isinstance(cached.get("market_path_support"), dict)
                and not _artifact_requires_upgrade(dict(cached.get("market_path_support") or {}))
            ):
                return dict(cached["market_path_support"])
            if not _kronos_enabled():
                support = _empty_support(
                    candidate_id=str(identity["candidate_id"]),
                    provider_source=_provider_source_for_identity(identity),
                    suppression_reason="feature_flag_disabled",
                    eligibility_state="disabled",
                )
                support["series_quality_summary"] = series_quality_summary
                support["observed_series"] = [_point(item["timestamp_utc"], float(item["close"])) for item in rows[-_OBSERVED_POINTS:]]
                return support
            adapter_payload = build_kronos_input(
                identity=identity,
                rows=rows,
                horizon=_HORIZON,
                interval=_INTERVAL,
                max_context=_MAX_CONTEXT,
                min_history_bars=_MIN_HISTORY_BARS,
            )
            if not adapter_payload.get("supported"):
                support = _empty_support(
                    candidate_id=str(identity["candidate_id"]),
                    provider_source=_provider_source_for_identity(identity),
                    suppression_reason=str(adapter_payload.get("failure_class") or "model_execution_failed"),
                    eligibility_state="unsupported",
                )
                support["series_quality_summary"] = series_quality_summary
                support["observed_series"] = [_point(item["timestamp_utc"], float(item["close"])) for item in rows[-_OBSERVED_POINTS:]]
                return support
            try:
                sample_paths = _run_kronos_samples(adapter_payload)
                market_path_support = _build_support_from_samples(
                    conn=conn,
                    candidate_id=str(identity["candidate_id"]),
                    identity=identity,
                    rows=rows,
                    sample_paths=sample_paths,
                    adapter_payload=adapter_payload,
                    series_quality_summary=series_quality_summary,
                )
                forecast_run_id = record_forecast_run(
                    conn,
                    candidate_id=str(identity["candidate_id"]),
                    series_role=str(identity.get("series_role") or "direct"),
                    model_name=_MODEL_NAME,
                    model_version=_MODEL_VERSION,
                    input_series_version=str(input_series_version or "unknown_series_version"),
                    run_status="suppressed" if market_path_support.get("suppression_reason") else "ready",
                    usefulness_label=str(market_path_support.get("usefulness_label") or "suppressed"),
                    suppression_reason=str(market_path_support.get("suppression_reason") or "") or None,
                    details={
                        "series_role": identity.get("series_role"),
                        "provider_source": market_path_support.get("provider_source"),
                        "truth_manifest": market_path_support.get("truth_manifest"),
                        "sampling_summary": market_path_support.get("sampling_summary"),
                        "market_setup_state": market_path_support.get("market_setup_state"),
                        "scope_key": market_path_support.get("scope_key"),
                        "scope_label": market_path_support.get("scope_label"),
                    },
                )
                persist_forecast_artifact(
                    conn,
                    forecast_run_id=forecast_run_id,
                    candidate_id=str(identity["candidate_id"]),
                    input_series_version=str(input_series_version or "unknown_series_version"),
                    market_path_support=market_path_support,
                )
                return market_path_support
            except Exception as exc:  # noqa: BLE001
                error_message = str(exc)
                cached_support = dict(cached.get("market_path_support") or {}) if isinstance((cached or {}).get("market_path_support"), dict) else None
                safe_last_good = (
                    cached_support is not None
                    and str(cached.get("input_series_version") or "") == str(input_series_version or "")
                    and _artifact_safe_for_last_good(cached_support)
                )
                if safe_last_good:
                    fallback_support = _decorate_last_good_artifact(
                        cached_support,
                        error_message=error_message,
                        series_quality_summary=series_quality_summary,
                    )
                    record_forecast_run(
                        conn,
                        candidate_id=str(identity["candidate_id"]),
                        series_role=str(identity.get("series_role") or "direct"),
                        model_name=_MODEL_NAME,
                        model_version=_MODEL_VERSION,
                        input_series_version=str(input_series_version or "unknown_series_version"),
                        run_status="served_last_good",
                        usefulness_label=str(fallback_support.get("usefulness_label") or "suppressed"),
                        suppression_reason=None,
                        details={
                            "error": error_message,
                            "series_role": identity.get("series_role"),
                            "served_last_good_artifact": True,
                            "provider_source": fallback_support.get("provider_source"),
                        },
                    )
                    return fallback_support
                support = _empty_support(
                    candidate_id=str(identity["candidate_id"]),
                    provider_source=_provider_source_for_identity(identity),
                    suppression_reason="model_execution_failed",
                    eligibility_state="failed",
                )
                support["series_quality_summary"] = series_quality_summary
                support["observed_series"] = [_point(item["timestamp_utc"], float(item["close"])) for item in rows[-_OBSERVED_POINTS:]]
                support["model_metadata"] = {
                    "model_name": _MODEL_NAME,
                    "model_version": _MODEL_VERSION,
                    "support_semantics_version": _SUPPORT_SEMANTICS_VERSION,
                    "error": error_message,
                }
                record_forecast_run(
                    conn,
                    candidate_id=str(identity["candidate_id"]),
                    series_role=str(identity.get("series_role") or "direct"),
                    model_name=_MODEL_NAME,
                    model_version=_MODEL_VERSION,
                    input_series_version=str(input_series_version or "unknown_series_version"),
                    run_status="failed",
                    usefulness_label="suppressed",
                    suppression_reason="model_execution_failed",
                    details={"error": error_message, "series_role": identity.get("series_role")},
                )
                return support


def refresh_candidate_market_path_support(
    candidate_id: str,
    *,
    force_refresh: bool = False,
) -> dict[str, Any] | None:
    return build_candidate_market_path_support(
        candidate_id,
        allow_refresh=False,
        force_refresh=force_refresh,
        require_ui_enabled=False,
    )


def run_market_forecast_refresh_lane(
    *,
    candidate_id: str | None = None,
    sleeve_key: str | None = None,
    stale_only: bool = True,
) -> dict[str, Any]:
    results: list[dict[str, Any]] = []
    refreshed_count = 0
    served_last_good_count = 0
    skipped_count = 0
    suppressed_count = 0
    failed_count = 0
    with _connection() as conn:
        rows = list_blueprint_market_candidates(conn, candidate_id=candidate_id, sleeve_key=sleeve_key)
        for row in rows:
            cid = str(row.get("candidate_id") or "") or f"candidate_instrument_{str(row.get('symbol') or '').strip().lower()}"
            identities = ensure_candidate_market_identities(conn, cid)
            if not identities:
                results.append(
                    {
                        "candidate_id": cid,
                        "symbol": row.get("symbol"),
                        "status": "failed",
                        "suppression_reason": "symbol_mapping_failed",
                    }
                )
                failed_count += 1
                continue
            identity, _loaded_rows = _choose_identity_with_series(conn, candidate_id=cid)
            series_role = str((identity or {}).get("series_role") or "direct")
            input_series_version = latest_series_version(
                conn,
                candidate_id=str((identity or {}).get("candidate_id") or cid),
                series_role=series_role,
                interval=str((identity or {}).get("primary_interval") or _INTERVAL),
            )
            cached = latest_forecast_artifact(conn, candidate_id=str((identity or {}).get("candidate_id") or cid))
            up_to_date = (
                cached is not None
                and str(cached.get("input_series_version") or "") == str(input_series_version or "")
                and not _artifact_requires_upgrade(dict(cached.get("market_path_support") or {}))
            )
            if stale_only and up_to_date:
                skipped_count += 1
                results.append(
                    {
                        "candidate_id": cid,
                        "symbol": row.get("symbol"),
                        "status": "skipped_current",
                        "input_series_version": input_series_version,
                    }
                )
                continue
            support = build_candidate_market_path_support(
                cid,
                allow_refresh=False,
                force_refresh=not stale_only,
                require_ui_enabled=False,
            )
            usefulness_label = str((support or {}).get("usefulness_label") or "suppressed")
            suppression_reason = str((support or {}).get("suppression_reason") or "") or None
            model_metadata = dict((support or {}).get("model_metadata") or {})
            served_last_good = bool(model_metadata.get("last_good_artifact_served"))
            if support is None:
                failed_count += 1
                results.append(
                    {
                        "candidate_id": cid,
                        "symbol": row.get("symbol"),
                        "status": "failed",
                        "suppression_reason": "feature_flag_disabled",
                    }
                )
                continue
            if served_last_good:
                served_last_good_count += 1
            elif usefulness_label == "suppressed" or suppression_reason:
                suppressed_count += 1
            else:
                refreshed_count += 1
            results.append(
                {
                    "candidate_id": cid,
                    "symbol": row.get("symbol"),
                    "status": (
                        "served_last_good"
                        if served_last_good
                        else "ready"
                        if usefulness_label != "suppressed" and not suppression_reason
                        else "suppressed"
                    ),
                    "usefulness_label": usefulness_label,
                    "suppression_reason": suppression_reason,
                    "path_quality_label": support.get("path_quality_label"),
                    "candidate_fragility_label": support.get("candidate_fragility_label"),
                    "threshold_drift_direction": support.get("threshold_drift_direction"),
                    "input_series_version": input_series_version,
                    "last_good_artifact_served": served_last_good,
                    "last_model_error": model_metadata.get("last_model_error") if served_last_good else None,
                }
            )
    return {
        "scope": {
            "candidate_id": candidate_id,
            "sleeve_key": sleeve_key,
            "stale_only": stale_only,
            "eligible_count": len(results),
        },
        "status": "ok" if failed_count == 0 else "partial",
        "refreshed_count": refreshed_count,
        "served_last_good_count": served_last_good_count,
        "skipped_count": skipped_count,
        "suppressed_count": suppressed_count,
        "failure_count": failed_count,
        "items": results,
    }
