from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from importlib import import_module
import json
import logging
import os
import re
from typing import Any
from zoneinfo import ZoneInfo

from app.config import get_db_path
from app.models.db import connect
from app.v2.core.domain_objects import (
    CandidateAssessment,
    InstrumentTruth,
    MacroTruth,
    MarketDataPoint,
    MarketSeriesTruth,
    PolicyBoundary,
    SignalPacket,
    utc_now_iso,
)
from app.v2.contracts.chart_contracts import (
    chart_callout,
    chart_marker,
    chart_panel,
    chart_series_from_truth,
    chart_threshold,
    degraded_chart_panel,
    forecast_panel_from_bundle,
    load_routed_market_series,
)
from app.v2.core.holdings_overlay import apply_overlay
from app.v2.core.interpretation_engine import build_sleeve_assessment, interpret
from app.v2.core.mandate_rubric import apply_rubric, build_policy_boundaries
from app.v2.core.market_authority_probes import evaluate_market_authority_probe, summarize_market_authority_probe
from app.v2.donors.portfolio_truth import get_portfolio_truth
from app.v2.donors.blueprint import SQLiteBlueprintDonor
from app.v2.doctrine.doctrine_evaluator import evaluate
from app.v2.core.change_ledger import record_change
from app.v2.sources.registry import get_freshness_registry, get_market_adapter
from app.v2.features.forecast_feature_service import build_signal_support_bundle
from app.v2.features.daily_brief_decision_synthesis import synthesize_daily_brief_decisions
from app.v2.sources.registry import get_news_adapter
from app.v2.core.market_strip_registry import market_strip_spec, market_strip_symbols
from app.v2.core.market_state_sources import load_public_fred_market_state_truth
from app.v2.storage.surface_snapshot_store import record_surface_snapshot
from app.v2.surfaces.common import degraded_section, empty_section, ready_section, runtime_provenance, surface_state
from app.v2.translators.market_signal_translator import translate as translate_market_signal
from app.v2.translators.news_signal_translator import translate as translate_news_signal
from app.v2.truth.envelopes import describe_truth_envelope
from app.v2.truth.replay_inputs import compact_replay_inputs


_CONTRACT_VERSION = "0.3.0"
_SURFACE_ID = "daily_brief"

_MARKET_STATE_CAPTIONS: dict[str, dict[str, str]] = {
    # S&P 500
    "^GSPC":            {"up_significant": "Risk appetite firmer",    "up_moderate": "Equity tone firmer",          "up_minor": "Equity tone steady",    "down_significant": "Risk appetite retreating",  "down_moderate": "Equity tone softer",          "down_minor": "Equity tone steady"},
    # Dow
    "^DJI":             {"up_significant": "Industrials leading",     "up_moderate": "Value tone firmer",           "up_minor": "Cyclicals steady",      "down_significant": "Cyclicals under pressure",  "down_moderate": "Cyclicals softer",            "down_minor": "Cyclicals steady"},
    # Nasdaq
    "^IXIC":            {"up_significant": "Growth leading",          "up_moderate": "Tech tone firmer",            "up_minor": "Tech tone steady",      "down_significant": "Growth unwind",             "down_moderate": "Duration pressure on growth", "down_minor": "Tech tone steady"},
    # Russell 2K
    "^RUT":             {"up_significant": "Risk appetite broadening","up_moderate": "Domestic cyclicals firmer",   "up_minor": "Small caps steady",     "down_significant": "Small cap selloff",         "down_moderate": "Small caps lag",              "down_minor": "Small caps steady"},
    # S&P Equal Weight
    "^SPXEW":           {"up_significant": "Equal weight leading",    "up_moderate": "Participation broadening",    "up_minor": "Equal weight steady",   "down_significant": "Leadership narrowing",      "down_moderate": "Equal weight lagging",        "down_minor": "Equal weight steady"},
    # World Equity
    "^990100-USD-STRD": {"up_significant": "Global risk on",          "up_moderate": "Global tone firmer",          "up_minor": "Global tone steady",    "down_significant": "Global risk off",           "down_moderate": "Global tone softer",          "down_minor": "Global tone steady"},
    # VIX — up = more stress, down = less stress
    "^VIX":             {"up_significant": "Volatility spike",        "up_moderate": "Risk aversion rising",        "up_minor": "Volatility subdued",    "down_significant": "Risk aversion easing",      "down_moderate": "Risk appetite steadier",      "down_minor": "Volatility subdued"},
    # Credit spreads (BAMLH0A0HYM2): up = widening = stress; down = narrowing = improving
    "BAMLH0A0HYM2":     {"up_significant": "Spreads widening",        "up_moderate": "Risk premium firmer",         "up_minor": "Credit stress muted",   "down_significant": "Spreads tighten sharply",   "down_moderate": "Credit tone firmer",          "down_minor": "Credit stress muted"},
    # FX / USD
    "DXY":              {"up_significant": "Dollar rally",            "up_moderate": "Dollar firmer",               "up_minor": "Dollar little changed", "down_significant": "Dollar weakens",            "down_moderate": "Dollar softens",              "down_minor": "Dollar little changed"},
    # Regional banks / financials
    "^KRX":             {"up_significant": "Regional banks firmer",   "up_moderate": "Financials tone firmer",      "up_minor": "Financials steady",     "down_significant": "Regional bank stress",      "down_moderate": "Financials under pressure",   "down_minor": "Financials steady"},
    # Fed Funds (policy rate)
    "FEDFUNDS":         {"up_significant": "Front end firmer",        "up_moderate": "Cuts priced out",             "up_minor": "Policy path steady",    "down_significant": "Easing priced in",          "down_moderate": "Policy path steadier",        "down_minor": "Policy path steady"},
    # SOFR (overnight rate)
    "SOFR":             {"up_significant": "Overnight rate firmer",   "up_moderate": "Policy floor firmer",         "up_minor": "Policy floor steady",   "down_significant": "Policy floor easing",       "down_moderate": "Overnight rate softer",       "down_minor": "Policy floor steady"},
    # UST 2Y
    "DGS2":             {"up_significant": "Front end firmer",        "up_moderate": "Cuts priced out",             "up_minor": "Front end steady",      "down_significant": "Easing priced in",          "down_moderate": "Policy path steadier",        "down_minor": "Front end steady"},
    # Rates 10Y
    "^TNX":             {"up_significant": "Long yields higher",      "up_moderate": "Duration under pressure",     "up_minor": "Yields steady",         "down_significant": "Rates easing",              "down_moderate": "Duration relief",             "down_minor": "Yields steady"},
    # Real Yield 10Y
    "DFII10":           {"up_significant": "Real yields higher",      "up_moderate": "Valuation headwind",          "up_minor": "Real yields steady",    "down_significant": "Real yield relief",         "down_moderate": "Real yield easing",           "down_minor": "Real yields steady"},
    # UST 30Y
    "^TYX":             {"up_significant": "Long yields higher",      "up_moderate": "Long end firmer",             "up_minor": "Long end steady",       "down_significant": "Long end easing",           "down_moderate": "Long end relief",             "down_minor": "Long end steady"},
    # 30Y Mortgage
    "MORTGAGE30US":     {"up_significant": "Mortgage rates bid",      "up_moderate": "Mortgage rates firmer",       "up_minor": "Mortgage rates steady", "down_significant": "Mortgage spreads wider",    "down_moderate": "Mortgage rates softer",       "down_minor": "Mortgage rates steady"},
    # Bonds aggregate
    "NASDAQNCPAG":      {"up_significant": "Bond bid firmer",         "up_moderate": "Duration firmer",             "up_minor": "Duration steady",       "down_significant": "Bond market softer",        "down_moderate": "Duration softer",             "down_minor": "Duration steady"},
    # Inflation
    "CPI_YOY":          {"up_significant": "Inflation print firmer",  "up_moderate": "Inflation edging up",         "up_minor": "Inflation steady",      "down_significant": "Inflation print easing",    "down_moderate": "Inflation softening",         "down_minor": "Inflation steady"},
    # Gold
    "GC=F":             {"up_significant": "Haven bid",               "up_moderate": "Inflation hedge firmer",      "up_minor": "Gold steady",           "down_significant": "Gold easing",               "down_moderate": "Haven bid fading",            "down_minor": "Gold steady"},
    # Brent Crude
    "BZ=F":             {"up_significant": "Energy shock risk",       "up_moderate": "Oil bid persists",            "up_minor": "Crude steady",          "down_significant": "Crude easing",              "down_moderate": "Energy bid fading",           "down_minor": "Crude steady"},
    # WTI Crude
    "CL=F":             {"up_significant": "Energy shock risk",       "up_moderate": "Oil bid persists",            "up_minor": "Crude steady",          "down_significant": "Crude easing",              "down_moderate": "Energy bid fading",           "down_minor": "Crude steady"},
    # Bitcoin
    "BTC-USD":          {"up_significant": "Crypto appetite firmer",  "up_moderate": "Risk asset bid",              "up_minor": "Crypto steady",         "down_significant": "Risk asset softer",         "down_moderate": "Volatility elevated",         "down_minor": "Crypto steady"},
}

_CAPTION_FALLBACK: dict[str, str] = {
    "up_significant":   "Market tone firmer",
    "up_moderate":      "Market tone firmer",
    "up_minor":         "Little changed",
    "down_significant": "Market tone softer",
    "down_moderate":    "Market tone softer",
    "down_minor":       "Little changed",
}
logger = logging.getLogger(__name__)
_SLEEVE_ID = "sleeve_daily_brief_context"
_SLEEVE_LABEL = "Daily Brief Context"
_SLEEVE_PURPOSE = "multi asset daily brief monitoring"
_MARKET_SYMBOLS = market_strip_symbols()
_CHINA_TZ = ZoneInfo("Asia/Shanghai")
_NEW_YORK_TZ = ZoneInfo("America/New_York")
_FRESHNESS_ORDER = {
    "degraded_monitoring_mode": 4,
    "execution_failed_or_incomplete": 3,
    "stored_valid_context": 2,
    "fresh_partial_rebuild": 1,
    "fresh_full_rebuild": 0,
}
_MAGNITUDE_ORDER = {"significant": 2, "moderate": 1, "minor": 0, "unknown": -1}

_ASSET_CLASS_TO_SLEEVE_IDS: dict[str, list[str]] = {
    "equity": ["sleeve_global_equity_core", "sleeve_developed_ex_us_optional", "sleeve_emerging_markets"],
    "fixed_income": ["sleeve_ig_bonds"],
    "real_assets": ["sleeve_real_assets"],
    "cash": ["sleeve_cash_bills"],
    "commodity": ["sleeve_real_assets"],
    "alternative": ["sleeve_cash_bills"],
    "volatility": ["sleeve_global_equity_core", "sleeve_cash_bills"],
}
_BRIEF_SLEEVE_TO_PORTFOLIO_SLEEVES: dict[str, list[str]] = {
    "sleeve_global_equity_core": ["global_equity"],
    "sleeve_developed_ex_us_optional": ["global_equity"],
    "sleeve_emerging_markets": ["global_equity"],
    "sleeve_ig_bonds": ["ig_bond"],
    "sleeve_real_assets": ["real_asset"],
    "sleeve_cash_bills": ["cash"],
}
_REGISTRY_SLEEVE_TO_BRIEF_SLEEVES: dict[str, list[str]] = {
    "global_equity_core": ["sleeve_global_equity_core"],
    "developed_ex_us_optional": ["sleeve_developed_ex_us_optional"],
    "emerging_markets": ["sleeve_emerging_markets"],
    "china_satellite": ["sleeve_emerging_markets"],
    "ig_bonds": ["sleeve_ig_bonds"],
    "real_assets": ["sleeve_real_assets"],
    "cash_bills": ["sleeve_cash_bills"],
}

_TRUST_STATUS_LABELS: dict[str, str] = {
    "fresh_full_rebuild": "Current",
    "fresh_partial_rebuild": "Partially current",
    "stored_valid_context": "Stored context",
    "degraded_monitoring_mode": "Degraded",
    "execution_failed_or_incomplete": "Unavailable",
}


def _default_session_relevance_window_hours() -> float:
    raw = str(os.getenv("IA_SESSION_RELEVANCE_WINDOW_HOURS", "12")).strip()
    try:
        value = float(raw)
    except ValueError:
        value = 12.0
    return max(1.0, value)


def _slug(value: str) -> str:
    return str(value or "").strip().lower().replace(" ", "_").replace("-", "_").replace(":", "_")


def _trace_enabled() -> bool:
    return os.getenv("IA_TRACE_MARKET_QUOTES", "").strip() == "1"


def _trace(event: str, **fields: Any) -> None:
    if not _trace_enabled():
        return
    logger.info("MARKET_QUOTE_TRACE %s", json.dumps({"event": event, **fields}, sort_keys=True, default=str))


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _series_change_pct(series: MarketSeriesTruth) -> float | None:
    if len(series.points) >= 2:
        previous = _safe_float(series.points[-2].value)
        latest = _safe_float(series.points[-1].value)
        if previous not in {None, 0} and latest is not None:
            return ((latest - previous) / abs(previous)) * 100.0

    for pack in series.evidence:
        facts = dict(pack.facts or {})
        for key in ("change_pct_1d", "one_day_change_pct", "pct_change_1d", "daily_change_pct"):
            value = _safe_float(facts.get(key))
            if value is not None:
                return value
    return None


def _market_truth_is_usable_for_brief(truth: MarketSeriesTruth, payload: dict[str, Any]) -> tuple[bool, str | None]:
    cache_status = str(payload.get("cache_status") or "")
    if cache_status == "unavailable":
        return False, "routed_unavailable"
    current_value = _safe_float(payload.get("value") or payload.get("price") or payload.get("close"))
    if current_value is None:
        return False, "missing_current_value"
    if len(list(getattr(truth, "points", []) or [])) < 2:
        return False, "insufficient_series_points"
    if _series_change_pct(truth) is None:
        return False, "missing_one_day_movement"
    return True, None


def _macro_series(truth: MacroTruth) -> MarketSeriesTruth:
    strength = None
    for key in ("change_pct_1d", "one_day_change_pct", "regime_delta_pct", "growth_delta_pct"):
        strength = _safe_float(truth.indicators.get(key))
        if strength is not None:
            break

    points: list[MarketDataPoint] = []
    if strength is not None:
        base_value = 100.0
        points = [
            MarketDataPoint(at=truth.as_of, value=base_value),
            MarketDataPoint(at=truth.as_of, value=base_value * (1 + (strength / 100.0))),
        ]

    return MarketSeriesTruth(
        series_id=f"macro_series_{_slug(truth.macro_id)}",
        label=truth.regime or truth.macro_id,
        frequency="daily",
        units="index",
        points=points,
        evidence=truth.evidence,
        as_of=truth.as_of,
    )


def _asset_class_for(symbol: str, label: str) -> str:
    normalized_symbol = str(symbol or "").strip().upper()
    spec = market_strip_spec(normalized_symbol)
    explicit_asset_class = str(spec.get("asset_class") or "").strip().lower()
    if explicit_asset_class:
        return explicit_asset_class
    normalized_label = str(label or "").strip().lower()
    if normalized_symbol in {"ACWI", "SPY", "VEU", "VT"} or "equity" in normalized_label:
        return "equity"
    if normalized_symbol in {"AGG", "BND", "TLT"} or "bond" in normalized_label or "rates" in normalized_label:
        return "fixed_income"
    if normalized_symbol in {"GLD", "DBC"} or "commodity" in normalized_label:
        return "real_assets"
    if normalized_symbol in {"DXY", "UUP"} or "currency" in normalized_label or "dollar" in normalized_label:
        return "cash"
    return "multi_asset"


def _candidate_from_signal(
    *,
    entity_id: str,
    symbol: str,
    label: str,
    market_context: MarketSeriesTruth,
    signal_kind: str,
) -> tuple[CandidateAssessment, SignalPacket]:
    instrument = InstrumentTruth(
        instrument_id=f"instrument_{_slug(entity_id)}",
        symbol=symbol,
        name=label,
        asset_class=_asset_class_for(symbol, label),
        metrics={
            "sleeve_affiliation": _SLEEVE_PURPOSE,
            "benchmark_authority_level": "direct",
            "signal_kind": signal_kind,
        },
        evidence=market_context.evidence,
        as_of=market_context.as_of,
    )
    signal, card = interpret(instrument, market_context)
    conviction = min(0.95, max(0.35, abs(signal.strength) / 4.0))
    supports = [item.summary for item in card.signals if item.direction in {"up", "positive"}]
    risks = [item.summary for item in card.signals if item.direction in {"down", "negative", "mixed"}]
    candidate = CandidateAssessment(
        candidate_id=f"candidate_{_slug(entity_id)}",
        sleeve_id=_SLEEVE_ID,
        instrument=instrument,
        interpretation=card,
        mandate_fit="aligned",
        conviction=round(conviction, 2),
        score_breakdown={"signal_strength": round(abs(signal.strength), 2)},
        key_supports=supports[:3],
        key_risks=risks[:3],
    )
    return candidate, signal


def _dominant_boundary(boundaries: list[PolicyBoundary]) -> PolicyBoundary:
    return next((boundary for boundary in boundaries if boundary.action_boundary == "blocked"), boundaries[0])


def _freshness_value(source_id: str, *, truths_loaded: bool = False) -> str:
    """Look up freshness from the registry; if the lookup fails and truths were actually
    loaded for this source, return stored_valid_context rather than poisoning the banner."""
    try:
        return get_freshness_registry().get_freshness(source_id).freshness_class.value
    except Exception:
        return "stored_valid_context" if truths_loaded else "execution_failed_or_incomplete"


def _combined_freshness(states: list[str]) -> str:
    filtered = [state for state in states if state]
    if not filtered:
        return "degraded_monitoring_mode"
    return max(filtered, key=lambda state: _FRESHNESS_ORDER.get(state, -1))


def _raw_macro_payloads(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, tuple):
        return list(payload)
    if isinstance(payload, dict):
        if {"macro_id", "regime", "summary"} & set(payload):
            return [payload]
        values = [item for item in payload.values() if isinstance(item, (dict, MacroTruth))]
        return values or [payload]
    return [payload]


def _load_macro_truths() -> tuple[list[MacroTruth], str | None]:
    try:
        registry_module = import_module("app.v2.sources.registry")
        adapter_getter = getattr(registry_module, "get_macro_adapter")
        translator_module = import_module("app.v2.translators.macro_signal_translator")
    except (AttributeError, ModuleNotFoundError, ImportError):
        return [], None

    adapter = adapter_getter()
    translator = getattr(translator_module, "translate", None)
    if not callable(translator):
        return [], None

    payload: Any = None
    for method_name in ("fetch_all", "fetch_batch", "fetch"):
        method = getattr(adapter, method_name, None)
        if not callable(method):
            continue
        try:
            if method_name == "fetch":
                payload = [method("DGS10", surface_name="daily_brief")]
            elif method_name == "fetch_all":
                payload = method(surface_name="daily_brief")
            else:
                payload = method([], surface_name="daily_brief")
            break
        except TypeError:
            continue
        except Exception:
            return [], getattr(adapter, "source_id", None)

    if payload is None:
        return [], getattr(adapter, "source_id", None)

    truths: list[MacroTruth] = []
    for item in _raw_macro_payloads(payload):
        try:
            translated = translator(item)
        except Exception:
            continue
        if isinstance(translated, MacroTruth):
            truths.append(translated)
        elif isinstance(translated, list):
            truths.extend(entry for entry in translated if isinstance(entry, MacroTruth))
    return truths, getattr(adapter, "source_id", None)


def _apply_market_spec_overrides(truth: MarketSeriesTruth, spec: dict[str, Any]) -> MarketSeriesTruth:
    source_family_override = str(spec.get("source_family_override") or "").strip() or None
    authority_level_override = str(spec.get("authority_level_override") or "").strip() or None
    provenance_strength_override = str(spec.get("provenance_strength_override") or "").strip() or None
    if not any([source_family_override, authority_level_override, provenance_strength_override]):
        return truth

    for pack in list(getattr(truth, "evidence", []) or []):
        facts = dict(pack.facts or {})
        provider_execution = dict(facts.get("provider_execution") or {})
        if source_family_override:
            facts["source_family"] = source_family_override
            provider_execution["source_family"] = source_family_override
        if authority_level_override:
            facts["authority_level"] = authority_level_override
            provider_execution["authority_level"] = authority_level_override
            if authority_level_override == "proxy":
                facts["movement_state"] = "proxy"
                if str(provider_execution.get("semantic_grade") or "").strip() == "movement_capable":
                    provider_execution["semantic_grade"] = "proxy_movement_capable"
        if provenance_strength_override:
            provider_execution["provenance_strength"] = provenance_strength_override
        facts["provider_execution"] = provider_execution
        pack.facts = facts
    return truth


def _market_card_observed_date(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return date.fromisoformat(text[:10])
    except ValueError:
        return None


def _previous_business_day(value: date) -> date:
    current = value
    while current.weekday() >= 5:
        current -= timedelta(days=1)
    return current


def _slot_eligible_market_close_date(now: datetime | None = None) -> date:
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    return _previous_business_day(current.astimezone(_CHINA_TZ).date() - timedelta(days=1))


def _expected_market_close_date(now: datetime | None = None) -> date:
    return _slot_eligible_market_close_date(now=now)


def _eligible_market_close_date(*, spec: dict[str, Any], facts: dict[str, Any], provider_execution: dict[str, Any]) -> date:
    declared = (
        facts.get("eligible_as_of_china_date")
        or provider_execution.get("eligible_as_of_china_date")
    )
    declared_date = _market_card_observed_date(declared)
    if declared_date is not None:
        return declared_date
    model = str(spec.get("close_validation_model") or "slot_eligible_daily_close").strip().lower()
    if model == "slot_eligible_daily_close":
        return _slot_eligible_market_close_date()
    return _expected_market_close_date()


def _current_surface_slot_boundary(now: datetime | None = None) -> datetime:
    current = now or datetime.now(UTC)
    if current.tzinfo is None:
        current = current.replace(tzinfo=UTC)
    local = current.astimezone(_CHINA_TZ)
    if local.hour >= 20:
        slot_local = local.replace(hour=20, minute=0, second=0, microsecond=0)
    elif local.hour >= 8:
        slot_local = local.replace(hour=8, minute=0, second=0, microsecond=0)
    else:
        previous = local - timedelta(days=1)
        slot_local = previous.replace(hour=20, minute=0, second=0, microsecond=0)
    return slot_local.astimezone(UTC)


def _previous_surface_slot_boundary(now: datetime | None = None) -> datetime:
    current = _current_surface_slot_boundary(now=now)
    previous = current.astimezone(_CHINA_TZ) - timedelta(hours=12)
    return previous.astimezone(UTC)


def _coerce_datetime_utc(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _market_state_freshness_mode(
    *,
    validation_status: str | None,
    provider_execution: dict[str, Any],
    truth_envelope: dict[str, Any],
) -> str:
    if str(validation_status or "").strip() not in {"", "valid"}:
        return "rejected"
    retrieved_at = (
        provider_execution.get("fetched_at")
        or truth_envelope.get("retrieved_at_utc")
        or provider_execution.get("observed_at")
        or truth_envelope.get("as_of_utc")
    )
    retrieved_dt = _coerce_datetime_utc(retrieved_at)
    if retrieved_dt is None:
        return "stale"
    current_slot = _current_surface_slot_boundary()
    previous_slot = _previous_surface_slot_boundary()
    if retrieved_dt >= current_slot:
        return "fresh_current_slot"
    if retrieved_dt >= previous_slot:
        return "fresh_previous_slot"
    return "stale"


def _session_relevance_window_hours(spec: dict[str, Any]) -> float:
    value = _safe_float(spec.get("session_relevance_window_hours"))
    if value is not None:
        return max(1.0, value)
    return _default_session_relevance_window_hours()


def _session_relevance_reference_dt(
    *,
    source_type: str | None,
    provider_execution: dict[str, Any],
    truth_envelope: dict[str, Any],
    observed_at: Any,
) -> datetime | None:
    if str(source_type or "").strip() == "market_close":
        return _coerce_datetime_utc(
            provider_execution.get("fetched_at")
            or truth_envelope.get("retrieved_at_utc")
            or observed_at
        )
    return _coerce_datetime_utc(
        observed_at
        or truth_envelope.get("release_date")
        or truth_envelope.get("availability_date")
        or truth_envelope.get("observation_date")
        or truth_envelope.get("as_of_utc")
        or truth_envelope.get("retrieved_at_utc")
    )


def _is_session_reactivated(*, facts: dict[str, Any], provider_execution: dict[str, Any]) -> tuple[bool, str | None]:
    reason = str(facts.get("reactivation_reason") or provider_execution.get("reactivation_reason") or "").strip() or None
    threshold_state = str(facts.get("threshold_state") or provider_execution.get("threshold_state") or "").strip().lower()
    novelty_class = str(facts.get("novelty_class") or provider_execution.get("novelty_class") or "").strip().lower()
    if threshold_state == "breached":
        return True, reason or "Threshold breach is still active."
    if novelty_class == "reactivated":
        return True, reason or "Fresh confirmation has reactivated the theme."
    return False, reason


def _market_state_session_relevance(
    *,
    spec: dict[str, Any],
    source_type: str | None,
    validation_status: str | None,
    provider_execution: dict[str, Any],
    truth_envelope: dict[str, Any],
    observed_at: Any,
    facts: dict[str, Any],
) -> dict[str, Any]:
    window_hours = _session_relevance_window_hours(spec)
    reference_dt = _session_relevance_reference_dt(
        source_type=source_type,
        provider_execution=provider_execution,
        truth_envelope=truth_envelope,
        observed_at=observed_at,
    )
    observation_age_hours = None
    if reference_dt is not None:
        session_anchor = _current_surface_slot_boundary()
        observation_age_hours = max(
            0.0,
            round((session_anchor - reference_dt).total_seconds() / 3600.0, 2),
        )
    is_reactivated, reactivation_reason = _is_session_reactivated(
        facts=facts,
        provider_execution=provider_execution,
    )
    if str(validation_status or "").strip() not in {"", "valid"}:
        return {
            "state": "stale_for_session",
            "observation_age_hours": observation_age_hours,
            "window_hours": window_hours,
            "reactivation_reason": reactivation_reason,
        }
    if is_reactivated:
        return {
            "state": "reactivated",
            "observation_age_hours": observation_age_hours,
            "window_hours": window_hours,
            "reactivation_reason": reactivation_reason,
        }
    if observation_age_hours is None or observation_age_hours <= window_hours:
        return {
            "state": "fresh_session_move",
            "observation_age_hours": observation_age_hours,
            "window_hours": window_hours,
            "reactivation_reason": reactivation_reason,
        }
    if str(source_type or "").strip() == "official_release":
        return {
            "state": "cadence_valid_backdrop",
            "observation_age_hours": observation_age_hours,
            "window_hours": window_hours,
            "reactivation_reason": reactivation_reason,
        }
    return {
        "state": "stale_for_session",
        "observation_age_hours": observation_age_hours,
        "window_hours": window_hours,
        "reactivation_reason": reactivation_reason,
    }


def _semantic_tone_for_market_state(
    *,
    direction: str,
    display_style: str,
    metric_polarity: str,
    runtime: dict[str, Any],
) -> str:
    if display_style == "release_level":
        return "neutral"
    if direction == "unknown":
        return "warn"
    if bool(runtime.get("derived_or_proxy")):
        return "info"
    if metric_polarity == "lower_is_better":
        if direction == "down":
            return "good"
        if direction == "up":
            return "bad"
        return "neutral"
    if direction == "up":
        return "good"
    if direction == "down":
        return "bad"
    return "neutral"


def _market_state_backdrop_caption(*, source_type: str | None, cadence: str | None) -> str:
    cadence_label = str(cadence or "").strip().lower() or "latest"
    if str(source_type or "").strip() == "official_release":
        return f"Latest {cadence_label} reading"
    return "Carry-over read"


def _market_state_backdrop_sub_caption(*, source_type: str | None) -> str:
    if str(source_type or "").strip() == "official_release":
        return "Backdrop input; no fresh session update"
    return "Carry-over input; no fresh session update"


def _market_state_backdrop_summary(
    *,
    label: str,
    source_type: str | None,
    cadence: str | None,
    as_of: date | None,
) -> str:
    cadence_label = str(cadence or "").strip().lower() or "latest"
    as_of_label = as_of.isoformat() if as_of is not None else "the latest available date"
    if str(source_type or "").strip() == "official_release":
        return f"{label} remains the latest {cadence_label} official reading as of {as_of_label}; no fresh session update is available."
    return f"{label} remains the latest validated market reading as of {as_of_label}; no fresh session update is available."


def _market_truth_validation(truth: MarketSeriesTruth, spec: dict[str, Any]) -> tuple[bool, str, str | None]:
    facts = _truth_facts(truth)
    provider_execution = dict(facts.get("provider_execution") or {})
    policy = str(spec.get("validation_policy") or "").strip().lower()
    if not str(spec.get("metric_definition") or facts.get("metric_definition") or "").strip():
        return False, "missing_metric_definition", "Missing explicit metric definition."
    if not str(facts.get("source_provider") or provider_execution.get("provider_name") or "").strip():
        return False, "missing_provider", "Missing explicit source provider."
    if policy == "exact_daily_close":
        if not bool(provider_execution.get("usable_truth")):
            return False, "unavailable", "No usable daily close returned."
        if str(provider_execution.get("authority_level") or "").strip().lower() in {"derived", "proxy"}:
            return False, "proxy_disallowed", "Proxy daily closes are not allowed for this card."
        observed_at = (
            facts.get("close_date")
            or provider_execution.get("close_date")
            or provider_execution.get("observed_at")
            or facts.get("observed_at")
            or truth.as_of
        )
        observed_date = _market_card_observed_date(observed_at)
        expected_date = _eligible_market_close_date(spec=spec, facts=facts, provider_execution=provider_execution)
        if observed_date is None:
            return False, "missing_as_of", "Missing verified close date."
        if observed_date != expected_date:
            return False, "stale", f"Expected {expected_date.isoformat()} close, received {observed_date.isoformat()}."
        return True, "valid", None
    if policy == "official_release":
        if not bool(provider_execution.get("usable_truth")):
            return False, "unavailable", "No usable official release returned."
        if str(provider_execution.get("authority_level") or "").strip().lower() in {"derived", "proxy"}:
            return False, "proxy_disallowed", "Official-release cards cannot use proxy inputs."
        return True, "valid", None
    return bool(provider_execution.get("usable_truth")), "valid", None


def _annotate_market_truth(truth: MarketSeriesTruth, spec: dict[str, Any]) -> MarketSeriesTruth:
    is_exact, validation_status, validation_reason = _market_truth_validation(truth, spec)
    metric_definition = str(spec.get("metric_definition") or "").strip() or None
    metric_polarity = str(spec.get("metric_polarity") or "higher_is_better").strip() or "higher_is_better"
    source_type = str(spec.get("source_type") or "").strip() or None
    source_authority_tier = str(spec.get("source_authority_tier") or "").strip() or None
    display_style = str(spec.get("display_style") or "standard").strip() or "standard"
    expected_close_date = _expected_market_close_date().isoformat() if str(spec.get("validation_policy") or "") == "exact_daily_close" else None
    for pack in list(getattr(truth, "evidence", []) or []):
        facts = dict(pack.facts or {})
        provider_execution = dict(facts.get("provider_execution") or {})
        resolved_provider = str(facts.get("source_provider") or provider_execution.get("provider_name") or "").strip() or None
        resolved_authority_tier = (
            str(provider_execution.get("provenance_strength") or "").strip()
            or source_authority_tier
        )
        eligible_close_date = (
            _eligible_market_close_date(spec=spec, facts=facts, provider_execution=provider_execution).isoformat()
            if source_type == "market_close"
            else None
        )
        resolved_reason = validation_reason
        if resolved_reason is None and validation_status == "valid":
            if source_type == "market_close":
                resolved_reason = f"Validated declared daily close for slot-eligible close date {eligible_close_date or expected_close_date or 'expected close'} via {resolved_provider or 'declared provider'}."
            elif source_type == "official_release":
                resolved_reason = f"Validated latest official release via {resolved_provider or 'declared provider'}."
        facts["metric_definition"] = metric_definition
        facts["metric_polarity"] = metric_polarity
        facts["source_type"] = source_type
        facts["source_provider"] = resolved_provider
        facts["source_authority_tier"] = resolved_authority_tier or None
        facts["display_style"] = display_style
        facts["is_exact"] = is_exact
        facts["validation_status"] = validation_status
        facts["validation_reason"] = resolved_reason
        facts["expected_close_date"] = eligible_close_date or expected_close_date
        facts["provider_execution"] = provider_execution or None
        pack.facts = facts
    return truth


def _load_market_truth_from_spec(symbol: str, spec: dict[str, Any]) -> MarketSeriesTruth | None:
    data_source = str(spec.get("data_source") or "").strip().lower()
    if data_source == "public_fred":
        try:
            truth = load_public_fred_market_state_truth(
                symbol=symbol,
                label=str(spec.get("label") or symbol),
                series_id=str(spec.get("series_id") or symbol),
                transform=str(spec.get("transform") or "identity"),
                units=str(spec.get("unit") or "value"),
                cadence=str(spec.get("cadence") or "daily"),
            )
        except Exception:
            return None
        return _annotate_market_truth(_apply_market_spec_overrides(truth, spec), spec)

    routed_family = str(spec.get("routed_family") or "benchmark_proxy")
    routed_identifier = str(spec.get("routed_identifier") or symbol)
    quote_identifier = str(spec.get("quote_identifier") or symbol)
    try:
        truth, payload = load_routed_market_series(
            surface_name="daily_brief",
            endpoint_family=routed_family,
            identifier=routed_identifier,
            label=symbol,
            lookback=120,
        )
        routed_usable, routed_reason = _market_truth_is_usable_for_brief(truth, payload)
        _trace(
            "daily_brief.market_symbol.routed_result",
            symbol=symbol,
            endpoint_family=routed_family,
            points_count=len(list(getattr(truth, "points", []) or [])),
            provider_name=payload.get("provider_name"),
            cache_status=payload.get("cache_status"),
            error_state=payload.get("error_state"),
            fallback_used=payload.get("fallback_used"),
            retrieval_path=payload.get("retrieval_path"),
            fallback_to_quote_adapter=not routed_usable,
            fallback_reason=routed_reason,
        )
        if not routed_usable:
            raise RuntimeError(routed_reason or "routed_market_truth_unusable")
    except Exception:
        if routed_family == "market_close":
            return None
        adapter = get_market_adapter()
        try:
            try:
                payload = adapter.fetch(quote_identifier, surface_name="daily_brief")
            except TypeError:
                payload = adapter.fetch(quote_identifier)
            truth = translate_market_signal(payload)
            _trace(
                "daily_brief.market_symbol.quote_fallback_used",
                symbol=symbol,
                endpoint_family="quote_latest",
                provider_name=payload.get("provider_name"),
                price_present=payload.get("price") is not None,
                change_pct_1d_present=payload.get("change_pct_1d") is not None,
                retrieval_path=payload.get("retrieval_path"),
            )
        except Exception:
            return None
    return _annotate_market_truth(_apply_market_spec_overrides(truth, spec), spec)


def _load_market_truths() -> list[MarketSeriesTruth]:
    truths: list[MarketSeriesTruth] = []
    for symbol in _MARKET_SYMBOLS:
        spec = market_strip_spec(symbol)
        truth = _load_market_truth_from_spec(symbol, spec)
        if truth is None:
            continue
        is_exact, validation_status, _ = _market_truth_validation(truth, spec)
        if validation_status != "valid":
            _trace(
                "daily_brief.market_symbol.validation_failed",
                symbol=symbol,
                validation_status=validation_status,
                expected_close_date=_expected_market_close_date().isoformat()
                if str(spec.get("validation_policy") or "") == "exact_daily_close"
                else None,
            )
            continue
        if truth.points or truth.evidence:
            truths.append(truth)
    return truths


def _load_news_truths() -> list[MarketSeriesTruth]:
    adapter = get_news_adapter()
    try:
        try:
            payload = adapter.fetch(limit=6, surface_name="daily_brief")
        except TypeError:
            payload = adapter.fetch(limit=6)
    except Exception:
        return []
    try:
        truths = translate_news_signal(payload)
    except Exception:
        return []
    return [truth for truth in truths if isinstance(truth, MarketSeriesTruth)]


def _signal_confirms(direction: str, label: str) -> str:
    if direction == "unknown":
        return f"Confirms once {label} has a usable live move instead of a thin or degraded input."
    if direction in {"up", "positive"}:
        return f"Confirms if {label} can hold the recent strength instead of giving it back."
    if direction in {"down", "negative"}:
        return f"Confirms if {label} stays under pressure and the bounce remains weak."
    return f"Confirms if {label} breaks out of the current holding pattern with follow-through."


def _signal_breaks(direction: str, label: str) -> str:
    if direction == "unknown":
        return f"Breaks if {label} remains input-constrained and still cannot establish a usable move."
    if direction in {"up", "positive"}:
        return f"Breaks if {label} reverses sharply enough to erase the latest improvement."
    if direction in {"down", "negative"}:
        return f"Breaks if {label} regains strength quickly enough to neutralize the current pressure."
    return f"Breaks if {label} remains directionless and fails to establish a usable signal."


def _do_not_overread(overclaim_risk: str, label: str) -> str | None:
    if overclaim_risk == "high":
        return f"Do not treat the {label} read as a trading trigger — evidence remains bounded and overclaim risk is elevated."
    if overclaim_risk == "medium":
        return f"The {label} signal carries moderate overclaim risk; interpret as context, not a stand-alone trigger."
    return None


def _mapping_directness(signal_kind: str, benchmark_authority_level: str) -> str:
    if benchmark_authority_level == "direct" and signal_kind == "market":
        return "direct"
    if signal_kind == "macro":
        return "macro-only"
    return "sleeve-proxy"


def _truth_envelope_from_truth(truth: MarketSeriesTruth | MacroTruth) -> dict[str, Any] | None:
    for pack in list(getattr(truth, "evidence", []) or []):
        envelope = dict((pack.facts or {}).get("truth_envelope") or {})
        if envelope:
            return envelope
    return None


def _truth_facts(truth: MarketSeriesTruth | MacroTruth) -> dict[str, Any]:
    for pack in list(getattr(truth, "evidence", []) or []):
        facts = dict(pack.facts or {})
        if facts:
            return facts
    return {}


def _coerce_freshness_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    text = str(value or "").strip()
    if not text:
        return {}
    return {"freshness_class": text}


def _truth_freshness_state(truth: MarketSeriesTruth | MacroTruth) -> str:
    facts = _truth_facts(truth)
    provider_execution = dict(facts.get("provider_execution") or {})
    freshness_payload = _coerce_freshness_payload(facts.get("freshness_state"))
    freshness = str(
        provider_execution.get("freshness_class")
        or freshness_payload.get("freshness_class")
        or getattr((list(getattr(truth, "evidence", []) or [None])[0]), "freshness", "")
        or ""
    ).strip()
    if freshness in {"current", "fresh"}:
        return "fresh_full_rebuild"
    if freshness:
        return freshness
    truth_envelope = dict(facts.get("truth_envelope") or {})
    acquisition_mode = str(truth_envelope.get("acquisition_mode") or "").strip().lower()
    if acquisition_mode == "live":
        return "fresh_full_rebuild"
    if acquisition_mode == "cached":
        return "stored_valid_context"
    if acquisition_mode == "fallback":
        return "degraded_monitoring_mode"
    return (
        "stored_valid_context"
        if (getattr(truth, "points", None) or getattr(truth, "evidence", None))
        else "execution_failed_or_incomplete"
    )


def _loaded_truth_freshness(source_id: str | None, truths: list[MarketSeriesTruth] | list[MacroTruth]) -> str:
    if truths:
        states = [_truth_freshness_state(truth) for truth in truths]
        live_like = {"fresh_full_rebuild"}
        partial_like = {"fresh_partial_rebuild"}
        stored_like = {"stored_valid_context"}
        degraded_like = {"degraded_monitoring_mode", "execution_failed_or_incomplete", "unavailable"}
        if any(state in live_like for state in states) and any(state in degraded_like | stored_like | partial_like for state in states if state not in live_like):
            return "fresh_partial_rebuild"
        if any(state in partial_like for state in states):
            return "fresh_partial_rebuild"
        if any(state in stored_like for state in states) and any(state in degraded_like for state in states):
            return "stored_valid_context"
        return _combined_freshness(states)
    if not source_id:
        return "execution_failed_or_incomplete"
    return _freshness_value(source_id, truths_loaded=False)


def _signal_source_family(signal_kind: str, symbol: str, retrieval_path: str | None) -> str:
    if signal_kind == "macro":
        return "macro"
    if signal_kind == "news":
        return "news"
    spec = market_strip_spec(symbol)
    routed_family = str(spec.get("routed_family") or "quote_latest")
    if str(retrieval_path or "").startswith("routed_"):
        return routed_family
    return "quote_latest"


def _signal_runtime_provenance(signal: dict[str, Any]) -> dict[str, Any]:
    truth_envelope = dict(signal.get("truth_envelope") or {})
    movement_state = str(signal.get("movement_state") or "").strip()
    retrieval_path = str(signal.get("retrieval_path") or "").strip() or None
    provider_execution = dict(signal.get("provider_execution") or {})
    return runtime_provenance(
        source_family=str(provider_execution.get("source_family") or "").strip()
        or _signal_source_family(
            str(signal.get("signal_kind") or ""),
            str(signal.get("symbol") or ""),
            retrieval_path,
        ),
        provider_used=str(signal.get("provider_used") or "").strip() or None,
        path_used=retrieval_path,
        provider_execution=provider_execution,
        truth_envelope=truth_envelope,
        freshness=str(signal.get("freshness_detail") or "").strip() or None,
        provenance_strength=(
            str(provider_execution.get("provenance_strength") or "").strip()
            or (
                "derived_or_proxy"
                if movement_state in {"input_constrained", "proxy", "derived"}
                else "cache_continuity"
                if str(truth_envelope.get("acquisition_mode") or "") == "cached"
                else None
            )
        ),
        derived_or_proxy=movement_state in {"input_constrained", "proxy", "derived"},
        insufficiency_reason=movement_state if movement_state in {"input_constrained", "unavailable"} else None,
    )


def _signal_trust_status(provenance: dict[str, Any]) -> str:
    if not bool(provenance.get("usable_truth")):
        return "Input constrained"
    if str(provenance.get("sufficiency_state") or "") in {"proxy_bounded", "price_only"}:
        return "Bounded support"
    if bool(provenance.get("derived_or_proxy")):
        return "Proxy-bounded"
    if str(provenance.get("live_or_cache") or "") == "cache":
        return "Cache continuity"
    if str(provenance.get("freshness") or "") in {"fresh_full_rebuild", "fresh_partial_rebuild", "current"}:
        return "Direct support"
    return _TRUST_STATUS_LABELS.get(str(provenance.get("freshness") or ""), "Bounded support")


def _signal_quality_score(provenance: dict[str, Any]) -> float:
    if not bool(provenance.get("usable_truth")):
        return 0.15
    sufficiency_state = str(provenance.get("sufficiency_state") or "")
    if sufficiency_state == "movement_capable":
        base = 1.0
    elif sufficiency_state in {"proxy_bounded", "history_capable"}:
        base = 0.65
    elif sufficiency_state in {"price_only", "field_present", "value_and_reference_period", "headline_timestamp_present"}:
        base = 0.5
    else:
        base = 0.35
    if bool(provenance.get("derived_or_proxy")):
        base -= 0.15
    if str(provenance.get("live_or_cache") or "") == "cache":
        base -= 0.1
    if str(provenance.get("freshness") or "") in {"degraded_monitoring_mode", "execution_failed_or_incomplete", "unavailable"}:
        base -= 0.2
    return max(0.0, min(1.0, round(base, 2)))


def _truth_quality_from_facts(facts: dict[str, Any], *, fallback_presence: bool) -> float:
    provider_execution = dict(facts.get("provider_execution") or {})
    if provider_execution:
        return _signal_quality_score(
            runtime_provenance(
                source_family=str(facts.get("source_family") or "").strip() or None,
                provider_execution=provider_execution,
                truth_envelope=dict(facts.get("truth_envelope") or {}),
                freshness=str((_coerce_freshness_payload(facts.get("freshness_state"))).get("freshness_class") or "").strip() or None,
            )
        )
    return 0.5 if fallback_presence else 0.0


def _average_quality(values: list[float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _market_state_card_from_truth(truth: MarketSeriesTruth) -> dict[str, Any]:
    symbol = str(truth.label or truth.series_id).strip().upper()
    spec = market_strip_spec(symbol)
    candidate, signal = _candidate_from_signal(
        entity_id=truth.series_id,
        symbol=symbol,
        label=truth.label,
        market_context=truth,
        signal_kind="market",
    )
    facts = _truth_facts(truth)
    freshness_payload = _coerce_freshness_payload(facts.get("freshness_state"))
    provider_execution = dict(facts.get("provider_execution") or {})
    metric_definition = str(facts.get("metric_definition") or spec.get("metric_definition") or "").strip() or None
    metric_polarity = str(facts.get("metric_polarity") or spec.get("metric_polarity") or "higher_is_better").strip() or "higher_is_better"
    source_type = str(facts.get("source_type") or spec.get("source_type") or "").strip() or None
    source_provider = str(facts.get("source_provider") or provider_execution.get("provider_name") or "").strip() or None
    source_authority_tier = (
        str(facts.get("source_authority_tier") or provider_execution.get("provenance_strength") or "").strip() or None
    )
    display_style = str(facts.get("display_style") or spec.get("display_style") or "standard").strip() or "standard"
    cadence = str(spec.get("cadence") or "").strip() or None
    validation_status = str(facts.get("validation_status") or "").strip() or None
    validation_reason = str(facts.get("validation_reason") or "").strip() or None
    is_exact = bool(facts.get("is_exact"))
    observed_at = provider_execution.get("observed_at") or facts.get("observed_at") or truth.as_of
    observed_date = _market_card_observed_date(observed_at)
    current_value = _safe_float(truth.points[-1].value if truth.points else None)
    authority_probe = evaluate_market_authority_probe(
        symbol=symbol,
        primary_provider=source_provider,
        primary_value=current_value,
        as_of=observed_date,
    )
    signal_card = {
        "signal_id": signal.signal_id,
        "label": spec.get("label") or candidate.instrument.name,
        "symbol": candidate.instrument.symbol,
        "signal_kind": "market",
        "direction": signal.direction,
        "magnitude": signal.magnitude,
        "summary": _signal_summary_for(
            label=str(spec.get("label") or candidate.instrument.name),
            symbol=candidate.instrument.symbol,
            signal_kind="market",
            truth=truth,
        ),
        "mapping_directness": "direct" if str(provider_execution.get("authority_level") or "") == "direct" else "sleeve-proxy",
        "current_value": truth.points[-1].value if truth.points else None,
        "truth_envelope": _truth_envelope_from_truth(truth),
        "retrieval_path": str(facts.get("retrieval_path") or "").strip() or None,
        "provider_used": source_provider,
        "freshness_detail": str(freshness_payload.get("freshness_class") or "").strip() or None,
        "movement_state": str(facts.get("movement_state") or "").strip() or None,
        "provider_execution": provider_execution or None,
    }
    runtime = _signal_runtime_provenance(signal_card)
    freshness_mode = _market_state_freshness_mode(
        validation_status=validation_status,
        provider_execution=provider_execution,
        truth_envelope=dict(signal_card.get("truth_envelope") or {}),
    )
    session_relevance = _market_state_session_relevance(
        spec=spec,
        source_type=source_type,
        validation_status=validation_status,
        provider_execution=provider_execution,
        truth_envelope=dict(signal_card.get("truth_envelope") or {}),
        observed_at=observed_at,
        facts=facts,
    )
    session_relevance_state = str(session_relevance.get("state") or "fresh_session_move")
    observation_age_hours = _safe_float(session_relevance.get("observation_age_hours"))
    session_window_hours = _safe_float(session_relevance.get("window_hours")) or _default_session_relevance_window_hours()
    change_pct = None if display_style == "release_level" else _series_change_pct(truth)
    display_direction = signal.direction
    display_magnitude = signal.magnitude
    display_change_pct = change_pct
    note_summary = metric_definition or signal_card["summary"] if display_style == "release_level" else signal_card["summary"]
    caption = "Latest official release" if display_style == "release_level" else _market_state_caption(symbol, signal.direction, signal.magnitude, change_pct)
    sub_caption = _market_state_sub_caption(symbol, signal.direction, signal.magnitude)
    semantic_tone = _semantic_tone_for_market_state(
        direction=signal.direction,
        display_style=display_style,
        metric_polarity=metric_polarity,
        runtime=runtime,
    )
    if session_relevance_state in {"cadence_valid_backdrop", "stale_for_session"}:
        display_direction = "neutral"
        display_magnitude = "minor"
        display_change_pct = None
        note_summary = _market_state_backdrop_summary(
            label=str(spec.get("label") or candidate.instrument.name),
            source_type=source_type,
            cadence=cadence,
            as_of=observed_date,
        )
        caption = _market_state_backdrop_caption(source_type=source_type, cadence=cadence)
        sub_caption = _market_state_backdrop_sub_caption(source_type=source_type)
        semantic_tone = "neutral" if source_type == "official_release" else "info"
    if display_style == "release_level":
        value_parts: list[str] = ["neutral", "unknown"]
    else:
        value_parts = [display_direction, display_magnitude]
        if display_change_pct is not None:
            value_parts.append(f"{display_change_pct:+.2f}%")
    note_parts = [note_summary]
    if current_value is not None:
        note_parts.append(f"Current {current_value:.2f}.")
    if metric_definition and display_style != "release_level":
        note_parts.append(metric_definition)
    if source_provider:
        authority_label = source_authority_tier.replace("_", " ") if source_authority_tier else None
        provider_note = f"Provider {source_provider.replace('_', ' ')}"
        if authority_label:
            provider_note = f"{provider_note} · {authority_label}"
        note_parts.append(provider_note)
    if observed_date is not None:
        if source_type == "market_close":
            note_parts.append(f"As of {observed_date.isoformat()} close.")
        elif source_type == "official_release":
            note_parts.append(f"As of {observed_date.isoformat()} official release.")
    probe_note = summarize_market_authority_probe(authority_probe)
    if probe_note:
        note_parts.append(probe_note)
    envelope_note = describe_truth_envelope(dict(signal_card.get("truth_envelope") or {}))
    if envelope_note:
        note_parts.append(envelope_note)
    if session_relevance_state in {"cadence_valid_backdrop", "stale_for_session"}:
        note_parts.append(
            f"Session relevance downgraded after {observation_age_hours:.1f}h without a fresh session update."
            if observation_age_hours is not None
            else "Session relevance downgraded because no fresh session update is available."
        )
    elif session_relevance_state == "reactivated":
        note_parts.append(
            str(session_relevance.get("reactivation_reason") or "Fresh confirmation is reactivating the latest observation.")
        )
    return {
        "card_id": f"market_state_{_slug(symbol)}",
        "label": spec.get("label") or candidate.instrument.name,
        "value": " · ".join(value_parts),
        "note": " ".join(part for part in note_parts if str(part or "").strip()),
        "runtime_provenance": runtime,
        "tone": semantic_tone,
        "current_value": current_value,
        "change_pct_1d": change_pct,
        "caption": caption,
        "sub_caption": sub_caption,
        "as_of": observed_date.isoformat() if observed_date is not None else (str(observed_at or "").strip() or None),
        "source_provider": source_provider,
        "source_type": source_type,
        "source_authority_tier": source_authority_tier or runtime.get("source_authority_tier"),
        "metric_definition": metric_definition,
        "metric_polarity": metric_polarity,
        "is_exact": is_exact,
        "validation_status": validation_status,
        "validation_reason": validation_reason,
        "freshness_mode": freshness_mode,
        "primary_provider": authority_probe.primary_provider,
        "cross_check_provider": authority_probe.cross_check_provider,
        "cross_check_status": authority_probe.cross_check_status,
        "authority_gap_reason": authority_probe.authority_gap_reason,
        "session_relevance_state": session_relevance_state,
        "observation_age_hours": observation_age_hours,
        "session_relevance_window_hours": session_window_hours,
    }


def _market_state_caption(symbol: str, direction: str, magnitude: str, change_pct: float | None) -> str:
    if change_pct is None or direction == "unknown":
        return "No fresh read"
    if direction == "neutral":
        return "Unchanged"
    key = f"{direction}_{magnitude}"
    table = _MARKET_STATE_CAPTIONS.get(symbol.upper(), {})
    return table.get(key) or _CAPTION_FALLBACK.get(key, "Little changed")


_MARKET_STATE_SUBCAPTIONS: dict[str, dict[str, str]] = {
    "^GSPC": {
        "up_significant":   "Cyclicals and growth outperforming; broad risk-on",
        "up_moderate":      "Equity momentum building across sectors",
        "up_minor":         "Large-cap bid; no broad rotation signal",
        "down_significant": "Defensive rotation in play; broad de-risking",
        "down_moderate":    "Selling pressure building; watch breadth",
        "down_minor":       "Large-cap weight absorbing mild pressure",
        "neutral_minor":    "Index range-bound; no directional conviction",
    },
    "^DJI": {
        "up_significant":   "Value and industrials leading; growth lagging",
        "up_moderate":      "Cyclical stocks bid; value rotation underway",
        "up_minor":         "Cyclical names edging up; rotation muted",
        "down_significant": "Cyclical de-risking; economically sensitive softer",
        "down_moderate":    "Value names under pressure; rotation risk",
        "down_minor":       "Cyclicals drifting; no clear rotation signal",
        "neutral_minor":    "Industrials range-bound; rotation absent",
    },
    "^IXIC": {
        "up_significant":   "Long-duration bid; growth positioning rewarded",
        "up_moderate":      "Tech momentum; rate-sensitive growth outperforming",
        "up_minor":         "Growth names firm; tech tone constructive",
        "down_significant": "Long-duration unwind; growth positioning hit",
        "down_moderate":    "Rate sensitivity weighing on growth names",
        "down_minor":       "Tech names soft; rate sensitivity limited",
        "neutral_minor":    "Growth names flat; rate impact contained",
    },
    "^RUT": {
        "up_significant":   "Domestic risk appetite strong; breadth expanding",
        "up_moderate":      "Cyclical breadth improving; small cap momentum",
        "up_minor":         "Small-cap breadth steady; risk appetite intact",
        "down_significant": "Liquidity and credit risk in focus",
        "down_moderate":    "Risk appetite narrowing to large caps",
        "down_minor":       "Small-cap premium compressed; risk appetite muted",
        "neutral_minor":    "Small caps flat; breadth stable",
    },
    "^SPXEW": {
        "up_significant":   "Rally broadening; not just mega-cap driven",
        "up_moderate":      "Breadth improving; participation widening",
        "up_minor":         "Breadth stable; cap-weight gap contained",
        "down_significant": "Market concentration rising; few names leading",
        "down_moderate":    "Index breadth weakening; rally narrowing",
        "down_minor":       "Broad market soft; cap-weight gap narrow",
        "neutral_minor":    "Breadth flat; market participation unchanged",
    },
    "^990100-USD-STRD": {
        "up_significant":   "Global growth read positive; EM and DM bid",
        "up_moderate":      "International equities firmer; global appetite",
        "up_minor":         "International equities steady; DM tone intact",
        "down_significant": "Cross-asset de-risking; global growth concerns",
        "down_moderate":    "International softening; global growth in question",
        "down_minor":       "Global tone softer; no regime change",
        "neutral_minor":    "International markets flat; global read neutral",
    },
    "^VIX": {
        "up_significant":   "Tail risk hedging expensive; options bid",
        "up_moderate":      "Uncertainty rising; implied vol expanding",
        "up_minor":         "Implied vol ticking up; options market cautious",
        "down_significant": "Risk premium compressing; fear gauge fading",
        "down_moderate":    "Calm returning; implied risk premium lower",
        "down_minor":       "Vol settling; risk premium fading slowly",
        "neutral_minor":    "Implied vol range-bound; fear gauge steady",
    },
    "BAMLH0A0HYM2": {
        "up_significant":   "HY credit under stress; risk-off in credit",
        "up_moderate":      "Credit risk premium expanding; HY under pressure",
        "up_minor":         "HY spreads drifting wider; credit tone cautious",
        "down_significant": "HY spreads tightening sharply; credit risk-on",
        "down_moderate":    "Credit tone improving; HY bid firmer",
        "down_minor":       "Credit spreads edging in; HY risk appetite intact",
        "neutral_minor":    "Credit spreads stable; HY market range-bound",
    },
    "DXY": {
        "up_significant":   "EM and commodity headwinds building",
        "up_moderate":      "Commodity and EM assets under pressure",
        "up_minor":         "USD edging up; commodity pressure modest",
        "down_significant": "Tailwind for commodities and EM assets",
        "down_moderate":    "Dollar softness supportive for risk assets",
        "down_minor":       "Dollar drifting lower; no regime shift",
        "neutral_minor":    "USD range-bound; no dollar trend in play",
    },
    "^KRX": {
        "up_significant":   "Credit conditions improving; lending margins wider",
        "up_moderate":      "Rate environment supportive for financials",
        "up_minor":         "Financials firm; rate environment steady",
        "down_significant": "Credit tightening risk; watch deposit flows",
        "down_moderate":    "Rate headwinds weighing on financials",
        "down_minor":       "Regional banks under mild pressure; credit intact",
        "neutral_minor":    "Banking sector flat; credit stress absent",
    },
    "FEDFUNDS": {
        "up_significant":   "Rate cut timeline pushed back significantly",
        "up_moderate":      "Market pricing fewer near-term rate cuts",
        "up_minor":         "Rate expectations edging higher; policy watch",
        "down_significant": "Aggressive easing cycle being priced in",
        "down_moderate":    "Policy path shifting toward earlier cuts",
        "down_minor":       "Rate path drifting slightly lower; hold intact",
        "neutral_minor":    "Rate hold confirmed; forward guidance unchanged",
    },
    "SOFR": {
        "up_significant":   "Near-term funding costs rising; policy floor up",
        "up_moderate":      "Short-end liquidity conditions tightening",
        "up_minor":         "Near-term funding costs edging higher",
        "down_significant": "Funding conditions easing; policy floor softening",
        "down_moderate":    "Near-term rate expectations drifting lower",
        "down_minor":       "Overnight funding stable; policy floor intact",
        "neutral_minor":    "Overnight rate anchored; repo market calm",
    },
    "DGS2": {
        "up_significant":   "Rate cut bets aggressively unwound",
        "up_moderate":      "Fewer near-term cuts priced; hawkish read",
        "up_minor":         "Front end firm; policy expectations holding",
        "down_significant": "Market pricing aggressive rate cut cycle",
        "down_moderate":    "Front end rallying; policy turning more dovish",
        "down_minor":       "Front end bid; rate cut path unchanged",
        "neutral_minor":    "Front end anchored; rate expectations stable",
    },
    "^TNX": {
        "up_significant":   "Headwind for equities and duration assets",
        "up_moderate":      "Rate pressure on growth and rate-sensitive sectors",
        "up_minor":         "Duration pressure modest; growth names cautious",
        "down_significant": "Duration relief; bond bid and growth reprieve",
        "down_moderate":    "Rate relief supportive for growth and duration",
        "down_minor":       "Mild rate relief; duration assets edging firmer",
        "neutral_minor":    "10Y yield range-bound; duration risk contained",
    },
    "DFII10": {
        "up_significant":   "Equity valuation multiple under pressure",
        "up_moderate":      "Real yield headwind for rate-sensitive equities",
        "up_minor":         "Real rate headwind modest; valuations steady",
        "down_significant": "Equity risk premium compressing; valuation relief",
        "down_moderate":    "Real yield easing; growth multiples supported",
        "down_minor":       "Real yield easing mildly; growth multiples intact",
        "neutral_minor":    "Real yield flat; equity valuation support stable",
    },
    "^TYX": {
        "up_significant":   "Long-end selloff; term premium rising",
        "up_moderate":      "Long-duration risk rising; term premium up",
        "up_minor":         "Long end drifting higher; term premium building",
        "down_significant": "Flight to duration; long-end rally",
        "down_moderate":    "Long-end rally; fixed income demand firmer",
        "down_minor":       "Long end settling; duration cost easing",
        "neutral_minor":    "Long end stable; term premium range-bound",
    },
    "MORTGAGE30US": {
        "up_significant":   "Housing affordability deteriorating rapidly",
        "up_moderate":      "Housing market headwinds building; buyers stressed",
        "up_minor":         "Mortgage costs edging up; housing affordability watch",
        "down_significant": "Housing affordability improving; demand may firm",
        "down_moderate":    "Mortgage rate relief; housing activity may recover",
        "down_minor":       "Mortgage rates stabilising; housing relief modest",
        "neutral_minor":    "Mortgage rate flat; housing activity unchanged",
    },
    "NASDAQNCPAG": {
        "up_significant":   "Flight to fixed income; risk-off bid",
        "up_moderate":      "Duration and credit both participating",
        "up_minor":         "Bond index firm; aggregate duration steady",
        "down_significant": "Bond market selloff; duration and credit both soft",
        "down_moderate":    "Rate or credit headwinds weighing on bonds",
        "down_minor":       "Bond index soft; rate or credit drift modest",
        "neutral_minor":    "Aggregate bond index flat; duration risk neutral",
    },
    "CPI_YOY": {
        "up_significant":   "Fed cut timeline at risk; hawkish repricing",
        "up_moderate":      "Inflation pressure; policy pivot timeline at risk",
        "up_minor":         "Inflation holding; policy path constrained",
        "down_significant": "Disinflation opening door for Fed easing",
        "down_moderate":    "Cooling inflation supportive for policy pivot",
        "down_minor":       "Inflation softening; policy pivot window opening",
        "neutral_minor":    "Inflation steady; policy path unchanged",
    },
    "GC=F": {
        "up_significant":   "Real rate and haven demand dominant",
        "up_moderate":      "Real yield and dollar dynamics supportive",
        "up_minor":         "Haven bid present; real rate dynamics active",
        "down_significant": "Risk-on tone reducing haven demand",
        "down_moderate":    "Haven demand fading; risk appetite returning",
        "down_minor":       "Haven demand steady; risk tone neutral",
        "neutral_minor":    "Gold flat; haven and real rate dynamics balanced",
    },
    "BZ=F": {
        "up_significant":   "Supply shock risk; energy costs rising",
        "up_moderate":      "Supply concerns or demand read positive",
        "up_minor":         "Energy supply read positive; cost pass-through modest",
        "down_significant": "Demand concerns or supply relief in play",
        "down_moderate":    "Energy complex under mild pressure",
        "down_minor":       "Crude soft; energy inflation pressure contained",
        "neutral_minor":    "Brent flat; supply and demand read balanced",
    },
    "CL=F": {
        "up_significant":   "Energy input costs rising across economy",
        "up_moderate":      "Energy sector likely outperforming",
        "up_minor":         "Energy cost pressure moderate; sector watch",
        "down_significant": "Demand concerns weighing on energy complex",
        "down_moderate":    "Energy sector under mild pressure",
        "down_minor":       "WTI drifting lower; energy cost relief",
        "neutral_minor":    "WTI flat; energy supply read unchanged",
    },
    "BTC-USD": {
        "up_significant":   "Speculative risk appetite strong; beta elevated",
        "up_moderate":      "Risk-on tone in digital assets",
        "up_minor":         "Risk appetite firm; crypto beta steady",
        "down_significant": "Risk assets broadly under pressure",
        "down_moderate":    "Risk appetite wavering; crypto beta exposed",
        "down_minor":       "Crypto pulling back; risk-on tone cautious",
        "neutral_minor":    "Bitcoin flat; risk appetite read neutral",
    },
}


def _market_state_sub_caption(symbol: str, direction: str, magnitude: str) -> str | None:
    if direction == "unknown":
        return None
    key = f"{direction}_{magnitude}"
    return _MARKET_STATE_SUBCAPTIONS.get(symbol.upper(), {}).get(key)


def _move_text(change_pct: float | None) -> str:
    if change_pct is None:
        return "the latest move is not established because the current input is constrained"
    if change_pct > 0:
        return f"the latest move is firmer by {change_pct:.2f}%"
    if change_pct < 0:
        return f"the latest move is weaker by {abs(change_pct):.2f}%"
    return "the latest move is broadly unchanged"


def _signal_summary_for(*, label: str, symbol: str, signal_kind: str, truth: MarketSeriesTruth) -> str:
    change_pct = _series_change_pct(truth)
    if signal_kind == "macro":
        return f"{label} remains the main macro reference point here; {_move_text(change_pct)}."
    if signal_kind == "news":
        return f"{label} is affecting the market read; {_move_text(change_pct)}."
    anchor = label or symbol
    return f"{anchor} is setting the current market tone; {_move_text(change_pct)}."


def _implication_for(*, asset_class: str, label: str, signal_kind: str) -> str:
    if signal_kind == "macro":
        return (
            f"{label} matters because it can change the growth, inflation, or policy backdrop "
            "that sets the brief before any holdings-specific overlay is applied."
        )
    if signal_kind == "news":
        return (
            f"{label} matters because it can change the narrative pressure on risk, rates, "
            "or implementation timing even when the portfolio overlay is absent."
        )
    if asset_class == "equity":
        return "This matters because equity leadership changes the brief's risk appetite and capital deployment read."
    if asset_class == "fixed_income":
        return "This matters because rates and bond moves change ballast, funding pressure, and relative carry."
    if asset_class == "cash":
        return "This matters because cash and currency moves change liquidity value, optionality, and funding posture."
    if asset_class == "real_assets":
        return "This matters because real asset moves change inflation protection and diversification behavior."
    return "This matters because it can change sleeve priorities, diversification behavior, and implementation timing."


def _brief_portfolio_read(signal_cards: list[dict[str, Any]], overlay_context: dict[str, str]) -> str:
    sleeves: list[str] = []
    for signal in signal_cards:
        for sleeve in list(signal.get("affected_sleeves") or []):
            label = sleeve.replace("sleeve_", "").replace("_", " ").strip()
            if label and label not in sleeves:
                sleeves.append(label)
    if sleeves:
        base = (
            "The brief is primarily changing the read on "
            + ", ".join(sleeves[:3])
            + " rather than gating on current holdings."
        )
    else:
        base = "The brief remains market-first and sleeve-first even when no direct portfolio object is mapped."
    overlay_summary = str(overlay_context.get("summary") or "").strip()
    if overlay_summary:
        return f"{base} {overlay_summary}"
    return base


def _timeframe_summary(signal_card: dict[str, Any]) -> str:
    envelope_summary = describe_truth_envelope(dict(signal_card.get("truth_envelope") or {}))
    if envelope_summary:
        return envelope_summary
    label = str(signal_card.get("label") or "Signal")
    as_of = str(signal_card.get("as_of") or "").strip()
    signal_kind = str(signal_card.get("signal_kind") or "market")
    if signal_kind == "macro":
        return f"{label} is being read as the latest macro period available as of {as_of or 'the current run'}."
    if signal_kind == "news":
        return f"{label} reflects the latest retrieved news context as of {as_of or 'the current run'}."
    return f"{label} reflects the latest market session carried into this brief as of {as_of or 'the current run'}."


def _affected_sleeves_for(asset_class: str) -> list[str]:
    return _ASSET_CLASS_TO_SLEEVE_IDS.get(asset_class, [])


def _portfolio_overlay_context() -> tuple[object | None, dict[str, list[str]], dict[str, str] | None, dict[str, str]]:
    try:
        portfolio = get_portfolio_truth("default")
    except Exception:
        return None, {}, None, {
            "state": "overlay_absent",
            "summary": "No holdings overlay is active yet. The Daily Brief remains market-first and portfolio consequence stays sleeve-level until a portfolio overlay is loaded.",
        }

    holdings = list(getattr(portfolio, "holdings", []) or [])
    if not holdings:
        return portfolio, {}, None, {
            "state": "overlay_absent",
            "summary": "No holdings overlay is active yet. The Daily Brief remains market-first and portfolio consequence stays sleeve-level until a portfolio overlay is loaded.",
        }

    holdings_by_portfolio_sleeve: dict[str, list[str]] = {}
    impacted_sleeves: list[str] = []
    for holding in holdings:
        symbol = str(holding.get("symbol") or "").strip().upper()
        sleeve = str(holding.get("sleeve") or "").strip().lower()
        if not symbol or not sleeve:
            continue
        holdings_by_portfolio_sleeve.setdefault(sleeve, [])
        if symbol not in holdings_by_portfolio_sleeve[sleeve]:
            holdings_by_portfolio_sleeve[sleeve].append(symbol)
        if sleeve not in impacted_sleeves:
            impacted_sleeves.append(sleeve)

    overlay_summary = {
        "portfolio_id": getattr(portfolio, "portfolio_id", None),
        "summary": (
            f"{len(holdings)} holding{'s' if len(holdings) != 1 else ''} are active across "
            f"{len(holdings_by_portfolio_sleeve)} mapped sleeve{'s' if len(holdings_by_portfolio_sleeve) != 1 else ''}. "
            "Daily Brief consequence can now attach to specific holdings where the signal map overlaps."
        ),
        "impacted_sleeves": impacted_sleeves,
    }
    return portfolio, holdings_by_portfolio_sleeve, overlay_summary, {
        "state": "ready",
        "summary": overlay_summary["summary"],
    }


def _brief_candidate_context() -> dict[str, list[str]]:
    try:
        conn = connect(get_db_path())
    except Exception:
        return {}
    try:
        donor = SQLiteBlueprintDonor(conn)
        rows = donor.list_candidates()
    except Exception:
        conn.close()
        return {}
    try:
        by_sleeve: dict[str, list[str]] = {}
        for row in rows:
            symbol = str(row.get("symbol") or "").strip().upper()
            registry_sleeve = str(row.get("sleeve_key") or "").strip().lower()
            if not symbol or registry_sleeve not in _REGISTRY_SLEEVE_TO_BRIEF_SLEEVES:
                continue
            for brief_sleeve in _REGISTRY_SLEEVE_TO_BRIEF_SLEEVES[registry_sleeve]:
                by_sleeve.setdefault(brief_sleeve, [])
                if symbol not in by_sleeve[brief_sleeve]:
                    by_sleeve[brief_sleeve].append(symbol)
        return by_sleeve
    finally:
        conn.close()


def _ranked_signal_cards(
    market_truths: list[MarketSeriesTruth],
    macro_truths: list[MacroTruth],
    news_truths: list[MarketSeriesTruth],
    overclaim_risk: str,
    freshness_state: str,
    holdings_by_portfolio_sleeve: dict[str, list[str]] | None = None,
    candidate_symbols_by_brief_sleeve: dict[str, list[str]] | None = None,
) -> tuple[list[dict[str, Any]], list[CandidateAssessment]]:
    interpreted: list[tuple[CandidateAssessment, SignalPacket, MarketSeriesTruth]] = []

    for truth in market_truths:
        symbol = str(truth.label or truth.series_id).strip().upper()
        candidate, signal = _candidate_from_signal(
            entity_id=truth.series_id,
            symbol=symbol,
            label=truth.label,
            market_context=truth,
            signal_kind="market",
        )
        interpreted.append((candidate, signal, truth))

    for truth in macro_truths:
        market_context = _macro_series(truth)
        candidate, signal = _candidate_from_signal(
            entity_id=truth.macro_id,
            symbol=(truth.regime or truth.macro_id)[:12].upper().replace(" ", "_"),
            label=truth.summary or truth.regime or truth.macro_id,
            market_context=market_context,
            signal_kind="macro",
        )
        interpreted.append((candidate, signal, market_context))

    for truth in news_truths:
        candidate, signal = _candidate_from_signal(
            entity_id=truth.series_id,
            symbol="NEWS",
            label=truth.label,
            market_context=truth,
            signal_kind="news",
        )
        interpreted.append((candidate, signal, truth))

    interpreted.sort(
        key=lambda item: (_MAGNITUDE_ORDER.get(item[1].magnitude, -1), abs(item[1].strength)),
        reverse=True,
    )
    signal_cards = []
    for candidate, signal, truth in interpreted:
        signal_kind = candidate.instrument.metrics.get("signal_kind") or signal.signal_kind
        asset_class = candidate.instrument.asset_class
        benchmark_authority_level = str(candidate.instrument.metrics.get("benchmark_authority_level") or "bounded")
        spec = market_strip_spec(candidate.instrument.symbol)
        label = str(spec.get("label") or candidate.instrument.name)
        facts = _truth_facts(truth)
        freshness_payload = _coerce_freshness_payload(facts.get("freshness_state"))
        freshness_detail = str(
            freshness_payload.get("freshness_class")
            or getattr((list(getattr(truth, "evidence", []) or [None])[0]), "freshness", "")
            or ""
        ).strip() or None
        provider_execution = dict(facts.get("provider_execution") or {})
        affected_sleeves = _affected_sleeves_for(asset_class)
        affected_holdings: list[str] = []
        affected_candidates: list[str] = []
        for sleeve_id in affected_sleeves:
            for portfolio_sleeve in _BRIEF_SLEEVE_TO_PORTFOLIO_SLEEVES.get(sleeve_id, []):
                for symbol in (holdings_by_portfolio_sleeve or {}).get(portfolio_sleeve, []):
                    if symbol not in affected_holdings:
                        affected_holdings.append(symbol)
            for symbol in (candidate_symbols_by_brief_sleeve or {}).get(sleeve_id, []):
                if symbol not in affected_candidates:
                    affected_candidates.append(symbol)

        signal_card = {
            "signal_id": signal.signal_id,
            "label": label,
            "symbol": candidate.instrument.symbol,
            "series_id": truth.series_id,
            "signal_kind": signal_kind,
            "direction": signal.direction,
            "magnitude": signal.magnitude,
            "summary": _signal_summary_for(
                label=label,
                symbol=candidate.instrument.symbol,
                signal_kind=str(signal_kind),
                truth=truth,
            ),
            "implication": _implication_for(
                asset_class=str(asset_class),
                label=label,
                signal_kind=str(signal_kind),
            ),
            "confidence": candidate.interpretation.confidence,
            "as_of": truth.as_of,
            "confirms": _signal_confirms(signal.direction, label),
            "breaks": _signal_breaks(signal.direction, label),
            "do_not_overread": _do_not_overread(overclaim_risk, label),
            "affected_sleeves": affected_sleeves,
            "affected_holdings": affected_holdings,
            "affected_candidates": affected_candidates,
            "mapping_directness": _mapping_directness(signal_kind, benchmark_authority_level),
            "trust_status": "Bounded support",
            "related_work_id": None,
            "current_value": truth.points[-1].value if truth.points else None,
            "history": [point.value for point in truth.points],
            "timestamps": [point.at for point in truth.points],
            "truth_envelope": _truth_envelope_from_truth(truth),
            "retrieval_path": str(facts.get("retrieval_path") or "").strip() or None,
            "provider_used": (
                str(getattr(truth.evidence[0].citations[0], "note", "") or "").strip()
                if truth.evidence and truth.evidence[0].citations
                else None
            ),
            "freshness_detail": freshness_detail,
            "movement_state": str(facts.get("movement_state") or "").strip() or None,
            "provider_execution": provider_execution or None,
            "source_type": str(facts.get("source_type") or "").strip() or None,
            "metric_definition": str(facts.get("metric_definition") or "").strip() or None,
            "reference_period": str(facts.get("reference_period") or "").strip() or None,
            "release_date": str(facts.get("release_date") or "").strip() or None,
            "availability_date": str(facts.get("availability_date") or "").strip() or None,
            "event_cluster_id": str(facts.get("event_cluster_id") or "").strip() or None,
            "event_family": str(facts.get("event_family") or "").strip() or None,
            "event_subtype": str(facts.get("event_subtype") or "").strip() or None,
            "event_region": str(facts.get("event_region") or "").strip() or None,
            "event_entities": list(facts.get("event_entities") or []),
            "market_channels": list(facts.get("market_channels") or []),
            "confirmation_assets": list(facts.get("confirmation_assets") or []),
            "event_trigger_summary": str(facts.get("event_trigger_summary") or "").strip() or None,
            "event_title": str(facts.get("event_title") or "").strip() or None,
            "event_fingerprint": str(facts.get("event_fingerprint") or "").strip() or None,
        }
        signal_card["runtime_provenance"] = _signal_runtime_provenance(signal_card)
        signal_card["trust_status"] = _signal_trust_status(dict(signal_card.get("runtime_provenance") or {}))
        signal_cards.append(signal_card)

    return signal_cards, [candidate for candidate, _, _ in interpreted]


def _fallback_contract() -> dict[str, object]:
    base_contract = {
        "contract_version": _CONTRACT_VERSION,
        "surface_id": _SURFACE_ID,
        "generated_at": utc_now_iso(),
        "freshness_state": "degraded_monitoring_mode",
        "what_changed": [],
        "why_it_matters_economically": (
            "Tier 1B macro inputs are not live in V2 yet, so this surface stays in bounded monitoring mode."
        ),
        "why_it_matters_here": (
            "No sleeve or candidate impact summary is available until the Daily Brief source path is fully wired."
        ),
        "review_posture": "research_only: wait for live macro and market inputs before strengthening the brief.",
        "what_confirms_or_breaks": (
            "A translated macro source, refreshed market context, and doctrine pass would confirm this surface. "
            "Missing Tier 1B inputs keep the read bounded."
        ),
        "surface_state": surface_state(
            "degraded",
            reason_codes=["no_live_macro_or_market_truth"],
            summary="Daily Brief is preserving the full Cortex section structure while live truth is missing.",
        ),
        "section_states": {
            "market_state": degraded_section(
                "no_market_cards",
                "No translated market or macro cards are available yet.",
            ),
            "signal_stack": empty_section(
                "no_signals",
                "No Daily Brief signals are available in the current window.",
            ),
            "portfolio_impact": degraded_section(
                "no_portfolio_mapping",
                "Portfolio consequence rows cannot be fully mapped until more truth arrives.",
            ),
            "review_triggers": ready_section(),
            "monitoring_conditions": degraded_section(
                "no_monitoring_thresholds",
                "Monitoring thresholds are waiting on live signal truth.",
            ),
            "scenarios": degraded_section(
                "no_scenarios",
                "Scenario blocks are preserved but only degraded content is available.",
            ),
            "evidence": degraded_section(
                "thin_evidence",
                "Evidence and trust remain present but shallow.",
            ),
            "regime_context": empty_section(
                "no_regime_context",
                "No backdrop context is available in the current window.",
            ),
            "diagnostics": ready_section(),
        },
        "evidence_and_trust": {
            "freshness_state": "degraded_monitoring_mode",
            "source_count": 0,
            "completeness_score": 0.0,
        },
        "market_state_cards": [],
        "macro_chart_panels": [
            degraded_chart_panel(
                panel_id="daily_brief_macro_degraded",
                title="Macro regime context",
                chart_type="market_context",
                summary="Macro chart support is unavailable until live source truth is restored.",
                what_to_notice="Keep the section shell visible and render this degraded state instead of dropping the chart slot.",
                degraded_state="no_series_available",
            )
        ],
        "cross_asset_chart_panels": [
            degraded_chart_panel(
                panel_id="daily_brief_cross_asset_degraded",
                title="Cross-asset context",
                chart_type="market_context",
                summary="Cross-asset chart support is unavailable until benchmark history is restored.",
                what_to_notice="Keep the section shell visible and render this degraded state instead of deleting it.",
                degraded_state="no_series_available",
            )
        ],
        "fx_chart_panels": [
            degraded_chart_panel(
                panel_id="daily_brief_fx_degraded",
                title="FX and local context",
                chart_type="market_context",
                summary="FX chart support is unavailable until routed series are restored.",
                what_to_notice="Preserve the chart shell and show this degraded state rather than collapsing the section.",
                degraded_state="no_series_available",
            )
        ],
        "signal_chart_panels": [],
        "scenario_chart_panels": [],
        "signal_stack": [],
        "signal_stack_groups": [],
        "regime_context_drivers": [],
        "monitoring_conditions": [],
        "portfolio_impact_rows": [],
        "review_triggers": [],
        "scenario_blocks": [],
        "evidence_bars": [
            {"label": "Freshness", "score": 28, "tone": "warn"},
            {"label": "Support depth", "score": 15, "tone": "warn"},
            {"label": "Source quality", "score": 10, "tone": "bad"},
            {"label": "Directness", "score": 10, "tone": "bad"},
            {"label": "Run strength", "score": 24, "tone": "warn"},
        ],
        "diagnostics": [
            {"label": "Signals processed", "value": "0"},
            {"label": "Source count", "value": "0"},
            {"label": "Completeness score", "value": "0%"},
            {"label": "Mode", "value": "Degraded monitoring"},
        ],
        "portfolio_overlay": None,
    }
    return apply_overlay(base_contract, holdings=None)


def _portfolio_consequence_for(signal_card: dict[str, Any]) -> str:
    affected_sleeves = list(signal_card.get("affected_sleeves") or [])
    affected_holdings = list(signal_card.get("affected_holdings") or [])
    affected_candidates = list(signal_card.get("affected_candidates") or [])
    readable_sleeves = [item.replace("sleeve_", "").replace("_", " ") for item in affected_sleeves[:3]]
    if affected_sleeves:
        base = f"Changes the read on {', '.join(readable_sleeves)}."
        if affected_holdings:
            return f"{base} Holdings overlay now narrows that to {', '.join(affected_holdings[:3])}."
        if affected_candidates:
            return (
                f"{base} Keep the consequence at sleeve level; {', '.join(affected_candidates[:3])} "
                "are the live implementation candidates if the sleeve read strengthens."
            )
        return f"{base} Keep the consequence at sleeve level until holdings-specific action is warranted."
    if affected_holdings:
        return f"Now maps directly to {', '.join(affected_holdings[:3])}, so the brief can be escalated into holdings review."
    if affected_candidates:
        return (
            "No direct book object is mapped yet, but live implementation candidates are available: "
            f"{', '.join(affected_candidates[:3])}."
        )
    return "No direct book object is mapped yet, so this remains a market and sleeve read rather than a holdings instruction."


def _posture_label(signal_card: dict[str, Any], review_posture: str) -> str:
    declared = str(signal_card.get("next_action") or "").strip()
    if declared:
        return declared
    if str(signal_card.get("mapping_directness") or "") == "direct" and "review" in review_posture:
        return "Review now"
    if str(signal_card.get("mapping_directness") or "") == "macro-only":
        return "Monitor"
    if signal_card.get("affected_sleeves") or signal_card.get("affected_holdings"):
        return "Monitor"
    return "Do not act yet"


def _review_lane(signal_card: dict[str, Any], review_posture: str) -> str:
    decision_status = str(signal_card.get("decision_status") or "").strip().lower()
    if decision_status in {"review_now", "triggered"}:
        return "review_now"
    if decision_status in {"near_trigger", "monitor", "watch_trigger"}:
        return "monitor"
    if decision_status in {"backdrop", "do_not_act_yet"}:
        return "do_not_act_yet"
    posture = _posture_label(signal_card, review_posture)
    normalized = posture.lower()
    if "review" in normalized:
        return "review_now"
    if "monitor" in normalized:
        return "monitor"
    return "do_not_act_yet"


def _portfolio_impact_status_label(signal_card: dict[str, Any], review_posture: str) -> str:
    lane = _review_lane(signal_card, review_posture)
    if lane == "review_now":
        return "Review"
    if lane == "monitor":
        return "Monitor"
    return "Background"


def _first_sentence(text: str) -> str:
    normalized = str(text or "").strip()
    if not normalized:
        return ""
    parts = re.split(r"(?<=[.!?])\s+", normalized, maxsplit=1)
    return parts[0].strip()


def _strip_implementation_clause(text: str) -> str:
    normalized = str(text or "").strip()
    if not normalized:
        return ""
    for marker in (
        " The main ETF choices are ",
        " Main ETF choices are ",
        " The main implementation choices are ",
    ):
        idx = normalized.find(marker)
        if idx != -1:
            normalized = normalized[:idx].strip()
    return normalized.strip()


def _portfolio_impact_consequence(signal_card: dict[str, Any]) -> str:
    candidates = [
        str(signal_card.get("interpretation_subtitle") or "").strip(),
        str(signal_card.get("why_it_matters_micro") or "").strip(),
        str(signal_card.get("summary") or "").strip(),
        str(signal_card.get("portfolio_consequence") or "").strip(),
        _portfolio_consequence_for(signal_card),
    ]
    for candidate in candidates:
        short = _strip_implementation_clause(_first_sentence(candidate) or candidate)
        if short:
            return short
    return "This still changes the portfolio read more than it authorizes a direct trade."


def _portfolio_impact_next_step(signal_card: dict[str, Any], review_posture: str, *, object_type: str) -> str:
    lane = _review_lane(signal_card, review_posture)
    target = {
        "Sleeve": "this sleeve",
        "Holding": "this holding",
        "Context": "this read",
    }.get(str(object_type or "").strip(), "this position")
    if lane == "review_now":
        return f"Review {target} now and decide whether the move is strong enough to change the current stance."
    if lane == "monitor":
        return f"Keep {target} on monitor and only change the stance if confirmation improves or the move reaches review level."
    return f"Keep {target} in the background unless the signal strengthens enough to reopen the review."


def _truth_from_signal(signal_card: dict[str, Any]) -> MarketSeriesTruth:
    history = [_safe_float(value) for value in list(signal_card.get("history") or [])]
    timestamps = [str(value) for value in list(signal_card.get("timestamps") or [])]
    points = [
        MarketDataPoint(
            at=timestamps[index] if index < len(timestamps) and timestamps[index] else utc_now_iso(),
            value=float(value),
        )
        for index, value in enumerate(history)
        if value is not None
    ]
    return MarketSeriesTruth(
        series_id=str(signal_card.get("series_id") or signal_card.get("signal_id") or "signal"),
        label=str(signal_card.get("label") or "Signal"),
        frequency="daily",
        units="price",
        points=points,
        as_of=points[-1].at if points else utc_now_iso(),
    )


_REGIME_BANDS: dict[str, list[tuple[str, float | None, float | None]]] = {
    "^VIX":         [("calm", None, 18.0), ("watch", 18.0, 24.0), ("stress", 24.0, None)],
    "BAMLH0A0HYM2": [("tight", None, 300.0), ("normal", 300.0, 500.0), ("stressed", 500.0, None)],
    "BZ=F":         [("soft", None, 70.0), ("neutral", 70.0, 90.0), ("elevated", 90.0, None)],
    "CL=F":         [("soft", None, 65.0), ("neutral", 65.0, 85.0), ("elevated", 85.0, None)],
    "GC=F":         [("underperforming", None, 2000.0), ("neutral", 2000.0, 2600.0), ("elevated", 2600.0, None)],
    "DXY":          [("soft", None, 99.0), ("neutral", 99.0, 103.0), ("strong", 103.0, None)],
    "BTC-USD":      [("risk-off", None, 50000.0), ("neutral", 50000.0, 90000.0), ("risk-on", 90000.0, None)],
}


def _current_band_label(symbol: str, value: float | None) -> str | None:
    if value is None:
        return None
    bands = _REGIME_BANDS.get(symbol.upper(), [])
    for label, lo, hi in bands:
        if lo is None and hi is not None and value < hi:
            return label
        if lo is not None and hi is None and value >= lo:
            return label
        if lo is not None and hi is not None and lo <= value < hi:
            return label
    return None


def _build_chart_logic(
    symbol: str,
    chart_mode: str,
    truth: MarketSeriesTruth,
    trigger_support: dict[str, Any] | None,
) -> dict[str, Any]:
    points = truth.points
    current_value = _safe_float(points[-1].value) if points else None
    previous_value = _safe_float(points[0].value) if len(points) >= 2 else None
    as_of_date = str(points[-1].at if points else truth.as_of or "")

    if chart_mode == "threshold":
        spec = market_strip_spec(symbol)
        polarity = str(spec.get("metric_polarity") or "lower_is_better")
        confirm_above = polarity == "higher_is_better"
        raw_threshold = (trigger_support or {}).get("threshold")
        trigger_level = _safe_float(raw_threshold)
        return {
            "current_value": current_value,
            "previous_value": previous_value,
            "trigger_level": trigger_level,
            "confirm_above": confirm_above,
            "break_below": not confirm_above,
            "bands": [],
            "current_band": None,
            "release_date": None,
            "as_of_date": as_of_date,
        }

    if chart_mode == "regime":
        bands_raw = _REGIME_BANDS.get(symbol.upper(), [])
        bands = [{"label": lbl, "min": lo, "max": hi} for lbl, lo, hi in bands_raw]
        current_band = _current_band_label(symbol, current_value)
        return {
            "current_value": current_value,
            "previous_value": previous_value,
            "trigger_level": None,
            "confirm_above": None,
            "break_below": None,
            "bands": bands,
            "current_band": current_band,
            "release_date": None,
            "as_of_date": as_of_date,
        }

    if chart_mode == "release":
        current_value_r = _safe_float(points[-1].value) if points else None
        previous_value_r = _safe_float(points[-2].value) if len(points) >= 2 else None
        release_date = str(points[-1].at[:10] if points and points[-1].at else "")
        return {
            "current_value": current_value_r,
            "previous_value": previous_value_r,
            "trigger_level": None,
            "confirm_above": None,
            "break_below": None,
            "bands": [],
            "current_band": None,
            "release_date": release_date,
            "as_of_date": as_of_date,
            "allocation_bars": [],
        }

    if chart_mode == "decomposition":
        # allocation_bars is populated by the caller via trigger_support context.
        # Each element: {"label": str, "value": float, "target": float,
        #                "low": float, "high": float, "unit": "pct"}
        allocation_bars = list((trigger_support or {}).get("allocation_bars") or [])
        return {
            "current_value": None,
            "previous_value": None,
            "trigger_level": None,
            "confirm_above": None,
            "break_below": None,
            "bands": [],
            "current_band": None,
            "release_date": None,
            "as_of_date": as_of_date,
            "allocation_bars": allocation_bars,
        }

    return {
        "current_value": current_value,
        "previous_value": previous_value,
        "trigger_level": None,
        "confirm_above": None,
        "break_below": None,
        "bands": [],
        "current_band": None,
        "release_date": None,
        "as_of_date": as_of_date,
        "allocation_bars": [],
    }


def _context_chart_panel(signal_card: dict[str, Any], *, freshness_state: str) -> dict[str, Any]:
    truth = _truth_from_signal(signal_card)
    symbol = str(signal_card.get("symbol") or "").strip().upper()
    spec = market_strip_spec(symbol)
    chart_mode = str(spec.get("chart_mode") or "market_context")
    if len(truth.points) < 2:
        return degraded_chart_panel(
            panel_id=f"context_chart_{signal_card['signal_id']}",
            title=str(signal_card["label"]),
            chart_type=chart_mode,
            summary=str(signal_card["summary"]),
            what_to_notice="The section stays visible, but routed series history is too short for a stable chart.",
            degraded_state="insufficient_history",
            freshness_state=freshness_state,
        )
    trust_state = (
        "weak_support"
        if signal_card.get("signal_kind") == "news"
        else "proxy_support"
        if signal_card.get("signal_kind") == "macro"
        else "direct_support"
    )
    support_bundle = dict(signal_card.get("support_bundle") or {})
    trigger_support = dict((support_bundle.get("monitoring_condition") or {}).get("trigger_support") or {})
    chart_logic = _build_chart_logic(symbol, chart_mode, truth, trigger_support or None)
    primary_series = (
        None
        if chart_mode == "release"
        else chart_series_from_truth(
            chart_id=f"context_chart_{signal_card['signal_id']}_primary",
            series_type="line",
            label=str(signal_card["label"]),
            truth=truth,
            source_family=str(signal_card.get("signal_kind") or "market"),
            source_label=str(signal_card.get("trust_status") or "Daily Brief context"),
            freshness_state=freshness_state,
            trust_state=trust_state,
        )
    )
    callouts = [
        chart_callout(
            callout_id=f"context_chart_{signal_card['signal_id']}_macro",
            label="Macro",
            tone="info",
            detail=str(signal_card.get("why_it_matters_macro") or signal_card.get("implication") or signal_card.get("summary") or ""),
        ),
        chart_callout(
            callout_id=f"context_chart_{signal_card['signal_id']}_next_action",
            label="Next action",
            tone="warn" if "review" in str(signal_card.get("next_action") or "").lower() else "info",
            detail=str(signal_card.get("next_action") or "Monitor"),
        ),
    ]
    return chart_panel(
        panel_id=f"context_chart_{signal_card['signal_id']}",
        title=str(signal_card["label"]),
        chart_type=chart_mode,
        chart_mode=chart_mode,
        primary_series=primary_series,
        callouts=callouts,
        summary=str(signal_card["summary"]),
        what_to_notice="Use this chart as support for the section summary, not as a replacement for the explanation grammar.",
        freshness_state=freshness_state,
        trust_state=trust_state,
        chart_logic=chart_logic,
    )


def _signal_chart_panel(signal_card: dict[str, Any], support_bundle: dict[str, Any], *, freshness_state: str) -> dict[str, Any]:
    truth = _truth_from_signal(signal_card)
    if len(truth.points) < 2:
        return degraded_chart_panel(
            panel_id=f"signal_chart_{signal_card['signal_id']}",
            title=f"{signal_card['label']} detail",
            chart_type="signal_detail",
            summary=str(signal_card["summary"]),
            what_to_notice="Signal detail remains chart-degraded because routed series history is insufficient.",
            degraded_state="insufficient_history",
            freshness_state=freshness_state,
        )
    trigger_support = dict(support_bundle["monitoring_condition"].get("trigger_support") or {})
    trust_state = (
        "weak_support"
        if signal_card.get("signal_kind") == "news"
        else "proxy_support"
        if signal_card.get("signal_kind") == "macro"
        else "direct_support"
    )
    return chart_panel(
        panel_id=f"signal_chart_{signal_card['signal_id']}",
        title=f"{signal_card['label']} detail",
        chart_type="signal_detail",
        primary_series=chart_series_from_truth(
            chart_id=f"signal_chart_{signal_card['signal_id']}_primary",
            series_type="line",
            label=str(signal_card["label"]),
            truth=truth,
            source_family=str(signal_card.get("signal_kind") or "market"),
            source_label=str(signal_card.get("trust_status") or "Daily Brief signal"),
            freshness_state=freshness_state,
            trust_state=trust_state,
        ),
        markers=[
            chart_marker(
                marker_id=f"signal_chart_{signal_card['signal_id']}_confirm",
                timestamp=truth.points[-1].at,
                label="Confirms",
                marker_type="confirm",
                linked_object_id=str(signal_card["signal_id"]),
                linked_surface="daily_brief",
                summary=str(signal_card["confirms"]),
            ),
            chart_marker(
                marker_id=f"signal_chart_{signal_card['signal_id']}_break",
                timestamp=truth.points[-1].at,
                label="Breaks",
                marker_type="break",
                linked_object_id=str(signal_card["signal_id"]),
                linked_surface="daily_brief",
                summary=str(signal_card["breaks"]),
            ),
        ],
        thresholds=[
            chart_threshold(
                threshold_id=f"signal_chart_{signal_card['signal_id']}_threshold",
                label="Near-term trigger",
                value=_safe_float(trigger_support.get("threshold")),
                threshold_type=str(trigger_support.get("trigger_type") or "near_term"),
                action_if_crossed=str(trigger_support.get("next_action_if_hit") or "Escalate review."),
                what_it_means=str(trigger_support.get("next_action_if_broken") or "Treat current support as broken."),
            )
        ],
        callouts=[
            chart_callout(
                callout_id=f"signal_chart_{signal_card['signal_id']}_macro",
                label="Macro",
                tone="info",
                detail=str(signal_card.get("why_it_matters_macro") or signal_card.get("summary") or ""),
            ),
            chart_callout(
                callout_id=f"signal_chart_{signal_card['signal_id']}_next_action",
                label="Next action",
                tone="warn" if "review" in str(signal_card.get("next_action") or "").lower() else "info",
                detail=str(signal_card.get("next_action") or "Monitor") + ": " + str(signal_card.get("portfolio_consequence") or ""),
            ),
            chart_callout(
                callout_id=f"signal_chart_{signal_card['signal_id']}_thesis",
                label="Thesis trigger",
                tone="neutral",
                detail=str(signal_card.get("thesis_trigger") or signal_card.get("breaks") or ""),
            ),
            chart_callout(
                callout_id=f"signal_chart_{signal_card['signal_id']}_path_risk",
                label="Path risk",
                tone="warn" if "high" in str(signal_card.get("path_risk_note") or "").lower() else "info",
                detail=str(signal_card.get("path_risk_note") or "Path support remains bounded."),
            ),
        ],
        summary=str(signal_card["summary"]),
        what_to_notice="Look for whether the path is still consistent with the confirms and breaks logic.",
        freshness_state=freshness_state,
        trust_state=trust_state,
    )


def _economic_read_from_drivers(drivers: list[dict[str, Any]], fallback: str) -> str:
    if not drivers:
        return fallback
    lead = drivers[0]
    return " ".join(
        part
        for part in [
            str(lead.get("why_it_matters_macro") or "").strip(),
            str(lead.get("why_it_matters_micro") or "").strip(),
        ]
        if part
    ) or fallback


def _portfolio_read_from_drivers(
    drivers: list[dict[str, Any]],
    portfolio_overlay_context: dict[str, str] | None,
) -> str:
    if not drivers:
        return _brief_portfolio_read([], portfolio_overlay_context)
    lead = drivers[0]
    parts = [
        str(lead.get("why_it_matters_short_term") or "").strip(),
        str(lead.get("why_it_matters_long_term") or "").strip(),
    ]
    if portfolio_overlay_context and portfolio_overlay_context.get("summary"):
        parts.append(str(portfolio_overlay_context["summary"]).strip())
    return " ".join(part for part in parts if part).strip()


def _confirms_breaks_from_drivers(drivers: list[dict[str, Any]], fallback: str) -> str:
    if not drivers:
        return fallback
    lead = drivers[0]
    parts = [
        str(lead.get("confirms") or "").strip(),
        str(lead.get("breaks") or "").strip(),
        str(lead.get("near_term_trigger") or "").strip(),
        str(lead.get("thesis_trigger") or "").strip(),
    ]
    return " ".join(part for part in parts if part).strip() or fallback


def _public_signal_card(signal: dict[str, Any]) -> dict[str, Any]:
    hidden_keys = {
        "support_bundle",
        "monitoring_condition",
        "scenario_block",
        "forecast_support",
        "review_lane",
        "source_context",
        "path_risk",
        "decision_change_potential",
        "breadth_of_consequence",
        "transmission_clarity",
        "evidence_sufficiency",
        "duplication_group",
        "actionability_class",
        "prominence_score",
    }
    return {key: value for key, value in signal.items() if key not in hidden_keys}


def build() -> dict[str, object]:
    macro_truths, macro_source_id = _load_macro_truths()
    market_truths = _load_market_truths()
    news_truths = _load_news_truths()

    if not macro_truths and not market_truths and not news_truths:
        return _fallback_contract()

    freshness_state = _combined_freshness(
        [
            _loaded_truth_freshness("market_price", market_truths),
            _loaded_truth_freshness(macro_source_id, macro_truths),
            _loaded_truth_freshness("news", news_truths),
        ]
    )

    lead_candidate_temp = None
    if market_truths or macro_truths:
        all_interpreted: list[tuple[CandidateAssessment, SignalPacket, MarketSeriesTruth]] = []
        for truth in market_truths:
            symbol = str(truth.label or truth.series_id).strip().upper()
            candidate, signal = _candidate_from_signal(
                entity_id=truth.series_id,
                symbol=symbol,
                label=truth.label,
                market_context=truth,
                signal_kind="market",
            )
            all_interpreted.append((candidate, signal, truth))
        for truth in macro_truths:
            market_context = _macro_series(truth)
            candidate, signal = _candidate_from_signal(
                entity_id=truth.macro_id,
                symbol=(truth.regime or truth.macro_id)[:12].upper().replace(" ", "_"),
                label=truth.summary or truth.regime or truth.macro_id,
                market_context=market_context,
                signal_kind="macro",
            )
            all_interpreted.append((candidate, signal, market_context))
        for truth in news_truths:
            candidate, signal = _candidate_from_signal(
                entity_id=truth.series_id,
                symbol="NEWS",
                label=truth.label,
                market_context=truth,
                signal_kind="news",
            )
            all_interpreted.append((candidate, signal, truth))

        if all_interpreted:
            all_interpreted.sort(
                key=lambda item: (_MAGNITUDE_ORDER.get(item[1].magnitude, -1), abs(item[1].strength)),
                reverse=True,
            )
            lead_candidate_temp = all_interpreted[0][0]

    if lead_candidate_temp is None:
        return _fallback_contract()

    restraints = evaluate(lead_candidate_temp)
    boundaries = build_policy_boundaries(lead_candidate_temp, sleeve_purpose=_SLEEVE_PURPOSE)
    constraint_summary = apply_rubric(lead_candidate_temp, _dominant_boundary(boundaries), restraints).model_copy(
        update={"boundaries": boundaries}
    )
    overclaim_risk = constraint_summary.overclaim_risk

    _, holdings_by_portfolio_sleeve, portfolio_overlay_summary, portfolio_overlay_context = _portfolio_overlay_context()
    candidate_symbols_by_brief_sleeve = _brief_candidate_context()

    signal_cards, candidates = _ranked_signal_cards(
        market_truths,
        macro_truths,
        news_truths,
        overclaim_risk,
        freshness_state,
        holdings_by_portfolio_sleeve=holdings_by_portfolio_sleeve,
        candidate_symbols_by_brief_sleeve=candidate_symbols_by_brief_sleeve,
    )

    if not candidates:
        return _fallback_contract()

    sleeve = build_sleeve_assessment(
        sleeve_id=_SLEEVE_ID,
        label=_SLEEVE_LABEL,
        purpose=_SLEEVE_PURPOSE,
        candidates=candidates,
    )
    sleeve_summary = " ".join(
        dict.fromkeys(
            candidate.interpretation.why_it_matters_here
            for candidate in candidates
            if candidate.interpretation.why_it_matters_here
        )
    ).strip()
    if sleeve_summary:
        sleeve = sleeve.model_copy(update={"summary": sleeve_summary})

    review_posture = (
        f"{constraint_summary.visible_decision_state.state}: "
        f"{constraint_summary.visible_decision_state.rationale}"
    )
    decision_bundle = synthesize_daily_brief_decisions(
        signal_cards,
        review_posture=review_posture,
        why_here=sleeve.summary,
        holdings_overlay_present=bool(portfolio_overlay_summary),
    )
    decision_stack: list[dict[str, Any]] = decision_bundle["drivers"]
    primary_drivers: list[dict[str, Any]] = decision_bundle["primary_drivers"]
    support_drivers: list[dict[str, Any]] = decision_bundle["support_drivers"]
    regime_context_drivers: list[dict[str, Any]] = decision_bundle["regime_context_drivers"]
    signal_stack_groups: list[dict[str, Any]] = decision_bundle["signal_stack_groups"]
    contingent_drivers: list[dict[str, Any]] = decision_bundle["contingent_drivers"]
    if not primary_drivers:
        return _fallback_contract()

    visible_decision_pool = [*primary_drivers, *support_drivers, *regime_context_drivers]
    market_signal_scores = [
        _signal_quality_score(dict(signal.get("runtime_provenance") or {}))
        for signal in visible_decision_pool
        if str(signal.get("signal_kind") or "") == "market"
    ]
    macro_truth_scores = [
        _truth_quality_from_facts(_truth_facts(truth), fallback_presence=bool(truth.indicators.get("current_value")))
        for truth in macro_truths
    ]
    news_truth_scores = [
        _truth_quality_from_facts(_truth_facts(truth), fallback_presence=bool(truth.label))
        for truth in news_truths
    ]
    source_quality_scores = {
        "market": _average_quality(market_signal_scores),
        "macro": _average_quality(macro_truth_scores),
        "news": _average_quality(news_truth_scores),
    }
    completeness_inputs = [
        _average_quality([_signal_quality_score(dict(signal.get("runtime_provenance") or {})) for signal in decision_stack]),
        _average_quality([_signal_quality_score(dict(signal.get("runtime_provenance") or {})) for signal in visible_decision_pool]),
        source_quality_scores["market"],
        source_quality_scores["macro"],
        1.0 if constraint_summary.doctrine_annotations else 0.0,
        1.0 if sleeve.summary else 0.0,
    ]
    what_confirms_or_breaks = "; ".join(constraint_summary.doctrine_annotations[:3]).strip()
    if not what_confirms_or_breaks:
        what_confirms_or_breaks = "; ".join(constraint_summary.reviewer_notes[:2]).strip()
    if not what_confirms_or_breaks:
        leading_conditions = []
        for signal in primary_drivers[:2]:
            leading_conditions.append(f"{signal['label']}: {signal['confirms']} {signal['breaks']}")
        what_confirms_or_breaks = " ".join(leading_conditions).strip() or "Keep the brief anchored to the highest-conviction signals and their explicit break conditions."

    truth_by_symbol = {
        str(truth.label or truth.series_id).strip().upper(): truth
        for truth in market_truths
    }
    market_state_cards: list[dict[str, Any]] = [
        _market_state_card_from_truth(truth_by_symbol[symbol])
        for symbol in _MARKET_SYMBOLS
        if symbol in truth_by_symbol
    ]

    portfolio_impact_rows: list[dict[str, Any]] = []
    review_triggers: list[dict[str, Any]] = []
    monitoring_conditions: list[dict[str, Any]] = []
    scenario_blocks: list[dict[str, Any]] = []
    macro_chart_panels: list[dict[str, Any]] = []
    cross_asset_chart_panels: list[dict[str, Any]] = []
    fx_chart_panels: list[dict[str, Any]] = []
    signal_chart_panels: list[dict[str, Any]] = []
    scenario_chart_panels: list[dict[str, Any]] = []
    direct_count = 0
    forecast_providers: set[str] = set()
    degraded_forecast_count = 0

    for signal in primary_drivers:
        if signal["mapping_directness"] == "direct":
            direct_count += 1
        posture_label = _portfolio_impact_status_label(signal, review_posture)
        review_triggers.append(
            {
                "trigger_id": f"trigger_{signal['signal_id']}",
                "lane": _review_lane(signal, review_posture),
                "label": str(signal.get("decision_title") or signal["label"]),
                "reason": str(signal.get("portfolio_consequence") or signal["summary"]),
            }
        )

        sleeves = list(signal.get("affected_sleeves") or [])
        holdings = list(signal.get("affected_holdings") or [])
        if not sleeves and not holdings:
            portfolio_impact_rows.append(
                {
                    "object_id": f"impact_{signal['signal_id']}",
                    "object_label": signal["label"],
                    "object_type": "Context",
                    "mapping": signal["mapping_directness"],
                    "status_label": posture_label,
                    "consequence": _portfolio_impact_consequence(signal),
                    "next_step": _portfolio_impact_next_step(signal, review_posture, object_type="Context"),
                }
            )
        for sleeve_id in sleeves:
            portfolio_impact_rows.append(
                {
                    "object_id": sleeve_id,
                    "object_label": sleeve_id,
                    "object_type": "Sleeve",
                    "mapping": signal["mapping_directness"],
                    "status_label": posture_label,
                    "consequence": _portfolio_impact_consequence(signal),
                    "next_step": _portfolio_impact_next_step(signal, review_posture, object_type="Sleeve"),
                }
            )
        for holding_id in holdings:
            portfolio_impact_rows.append(
                {
                    "object_id": holding_id,
                    "object_label": holding_id,
                    "object_type": "Holding",
                    "mapping": "direct",
                    "status_label": posture_label,
                    "consequence": _portfolio_impact_consequence(signal),
                    "next_step": _portfolio_impact_next_step(signal, review_posture, object_type="Holding"),
                }
            )

        portfolio_consequence = str(signal.get("portfolio_consequence") or _portfolio_consequence_for(signal))
        support = dict(signal.get("support_bundle") or {})
        monitoring_condition = dict(signal.get("monitoring_condition") or support.get("monitoring_condition") or {})
        scenario_block = dict(signal.get("scenario_block") or support.get("scenario_block") or {})
        if monitoring_condition:
            monitoring_conditions.append(monitoring_condition)
        if scenario_block:
            scenario_blocks.append(scenario_block)
        signal_chart_panels.append(
            {
                "signal_id": signal["signal_id"],
                "panel": _signal_chart_panel(signal, support, freshness_state=freshness_state),
            }
        )
        scenario_chart_panels.append(
            {
                "signal_id": signal["signal_id"],
                "panel": forecast_panel_from_bundle(
                    panel_id=f"scenario_chart_{signal['signal_id']}",
                    title=f"{signal['label']} scenario support",
                    bundle=support["bundle"],
                    summary=scenario_block.get("summary") or str(signal.get("summary") or ""),
                    what_to_notice="Treat this chart as support for what could change the brief, not as decision authority.",
                    history_truth=_truth_from_signal(signal),
                ),
            }
        )
        bundle = support["bundle"]
        if bundle.support.provider:
            forecast_providers.add(bundle.support.provider)
        if bundle.support.degraded_state:
            degraded_forecast_count += 1

    for signal in support_drivers:
        panel = _context_chart_panel(signal, freshness_state=freshness_state)
        kind = str(signal.get("signal_kind") or "")
        symbol = str(signal.get("symbol") or "").upper()
        if kind == "macro":
            macro_chart_panels.append(panel)
        elif symbol == "DXY":
            fx_chart_panels.append(panel)
        else:
            cross_asset_chart_panels.append(panel)

    for signal in regime_context_drivers:
        panel = _context_chart_panel(signal, freshness_state=freshness_state)
        kind = str(signal.get("signal_kind") or "")
        symbol = str(signal.get("symbol") or "").upper()
        if kind == "macro":
            macro_chart_panels.append(panel)
        elif symbol == "DXY":
            fx_chart_panels.append(panel)
        else:
            cross_asset_chart_panels.append(panel)

    if not macro_chart_panels:
        macro_chart_panels.append(
            degraded_chart_panel(
                panel_id="daily_brief_macro_degraded",
                title="Macro regime context",
                chart_type="market_context",
                summary="Macro context remains available through text, but a chart panel could not be emitted.",
                what_to_notice="Preserve the section shell and rely on the textual explanation until longer macro history is available.",
                degraded_state="no_series_available",
                freshness_state=freshness_state,
            )
        )
    if not cross_asset_chart_panels:
        cross_asset_chart_panels.append(
            degraded_chart_panel(
                panel_id="daily_brief_cross_asset_degraded",
                title="Cross-asset context",
                chart_type="market_context",
                summary="Cross-asset context remains available through text, but a chart panel could not be emitted.",
                what_to_notice="Preserve the section shell and rely on text until routed benchmark history is available.",
                degraded_state="no_series_available",
                freshness_state=freshness_state,
            )
        )
    if not fx_chart_panels:
        fx_chart_panels.append(
            degraded_chart_panel(
                panel_id="daily_brief_fx_degraded",
                title="FX and local context",
                chart_type="market_context",
                summary="FX context remains available through text, but the chart panel is degraded.",
                what_to_notice="Preserve the section and render this typed degraded state instead of deleting it.",
                degraded_state="no_series_available",
                freshness_state=freshness_state,
            )
        )

    source_count = sum(1 for value in source_quality_scores.values() if value >= 0.35)
    completeness_score = _average_quality(completeness_inputs)
    data_timeframes = [
        {
            "label": signal["label"],
            "summary": _timeframe_summary(signal),
            "truth_envelope": signal.get("truth_envelope"),
            "runtime_provenance": signal.get("runtime_provenance"),
        }
        for signal in primary_drivers[:4]
    ]
    section_states = {
        "market_state": ready_section() if market_state_cards else degraded_section("no_market_cards", "No market-state cards were emitted."),
        "signal_stack": ready_section() if support_drivers or signal_stack_groups else empty_section("no_signals", "No Daily Brief support signals were emitted."),
        "regime_context": ready_section() if regime_context_drivers else empty_section("no_regime_context", "No backdrop context was emitted."),
        "portfolio_impact": ready_section() if portfolio_impact_rows else degraded_section("no_portfolio_mapping", "No direct portfolio impact rows were emitted."),
        "review_triggers": ready_section(),
        "monitoring_conditions": ready_section() if contingent_drivers else degraded_section("no_monitoring_thresholds", "Monitoring conditions were not emitted."),
        "scenarios": ready_section() if scenario_blocks else degraded_section("no_scenarios", "Scenario blocks remain degraded."),
        "evidence": ready_section() if source_count >= 2 else degraded_section("thin_evidence", "Evidence remains available but thin."),
        "diagnostics": ready_section(),
    }
    data_confidence = (
        "high"
        if source_count >= 3 and completeness_score >= 0.75
        else "mixed"
        if source_count >= 2 and completeness_score >= 0.4
        else "low"
    )
    decision_confidence = str(lead_candidate_temp.interpretation.confidence or "medium")
    leading_economic_read = _economic_read_from_drivers(primary_drivers, lead_candidate_temp.interpretation.why_it_matters_economically)
    public_primary_drivers = [_public_signal_card(signal) for signal in primary_drivers]
    public_decision_stack = [_public_signal_card(signal) for signal in support_drivers]
    public_regime_context = [_public_signal_card(signal) for signal in regime_context_drivers]
    public_signal_stack_groups = [
        {
            "group_id": str(group.get("group_id") or ""),
            "label": str(group.get("label") or "Support"),
            "summary": str(group.get("summary") or ""),
            "representative": _public_signal_card(dict(group.get("representative") or {})) if group.get("representative") else None,
            "count": int(group.get("count") or len(list(group.get("signals") or []))),
            "signals": [_public_signal_card(signal) for signal in list(group.get("signals") or [])],
        }
        for group in signal_stack_groups
    ]

    base_contract = {
        "contract_version": _CONTRACT_VERSION,
        "surface_id": _SURFACE_ID,
        "generated_at": utc_now_iso(),
        "freshness_state": freshness_state,
        "surface_state": surface_state(
            "ready" if source_count >= 2 else "degraded",
            reason_codes=[] if source_count >= 2 else ["thin_source_coverage"],
            summary=(
                "Daily Brief is hydrated with market, macro, and news truth."
                if source_count >= 2
                else "Daily Brief remains live, but source coverage is still thin."
            ),
        ),
        "section_states": section_states,
        "what_changed": public_primary_drivers,
        "market_state_cards": market_state_cards,
        "macro_chart_panels": macro_chart_panels,
        "cross_asset_chart_panels": cross_asset_chart_panels,
        "fx_chart_panels": fx_chart_panels,
        "signal_chart_panels": signal_chart_panels,
        "scenario_chart_panels": scenario_chart_panels,
        "signal_stack": public_decision_stack,
        "signal_stack_groups": public_signal_stack_groups,
        "regime_context_drivers": public_regime_context,
        "monitoring_conditions": monitoring_conditions,
        "contingent_drivers": contingent_drivers,
        "portfolio_impact_rows": portfolio_impact_rows,
        "review_triggers": review_triggers,
        "scenario_blocks": scenario_blocks,
        "why_it_matters_economically": leading_economic_read,
        "why_it_matters_here": _portfolio_read_from_drivers(primary_drivers, portfolio_overlay_context),
        "review_posture": review_posture,
        "what_confirms_or_breaks": _confirms_breaks_from_drivers(primary_drivers, what_confirms_or_breaks),
        "data_confidence": data_confidence,
        "decision_confidence": decision_confidence,
        "data_timeframes": data_timeframes,
        "evidence_and_trust": {
            "freshness_state": freshness_state,
            "source_count": source_count,
            "completeness_score": completeness_score,
        },
        "evidence_bars": [
            {"label": "Freshness", "score": 92 if freshness_state == "fresh_full_rebuild" else 74 if freshness_state == "fresh_partial_rebuild" else 58 if freshness_state == "stored_valid_context" else 42 if freshness_state == "degraded_monitoring_mode" else 24, "tone": "good" if freshness_state in {"fresh_full_rebuild", "fresh_partial_rebuild"} else "warn"},
            {"label": "Support depth", "score": int(completeness_score * 100), "tone": "good" if completeness_score >= 0.75 else "warn"},
            {"label": "Source quality", "score": min(100, source_count * 28), "tone": "good" if source_count >= 3 else "warn"},
            {"label": "Directness", "score": min(100, max(18, direct_count * 30)), "tone": "good" if direct_count else "warn"},
            {"label": "Run strength", "score": 88 if source_count >= 3 else 68 if source_count == 2 else 46, "tone": "good" if source_count >= 2 else "warn"},
            {"label": "Forecast support", "score": 82 if forecast_providers and degraded_forecast_count == 0 else 56 if forecast_providers else 32, "tone": "good" if forecast_providers and degraded_forecast_count == 0 else "warn"},
        ],
        "diagnostics": [
            {"label": "Signals processed", "value": str(len(decision_stack))},
            {"label": "Investor support stack", "value": str(len(support_drivers))},
            {"label": "Backdrop drivers", "value": str(len(regime_context_drivers))},
            {"label": "Source count", "value": str(source_count)},
            {"label": "Completeness score", "value": f"{int(completeness_score * 100)}%"},
            {"label": "Review posture", "value": review_posture},
            {"label": "Forecast providers", "value": ", ".join(sorted(forecast_providers)) if forecast_providers else "Deterministic fallback only"},
            {"label": "Forecast degraded", "value": str(degraded_forecast_count)},
            {"label": "Reference clocks", "value": " | ".join(item["summary"] for item in data_timeframes[:3]) if data_timeframes else "No explicit period clocks emitted"},
        ],
        "portfolio_overlay": portfolio_overlay_summary,
        "portfolio_overlay_context": portfolio_overlay_context,
    }
    record_change(
        event_type="rebuild",
        surface_id="daily_brief",
        summary="Daily Brief contract rebuilt.",
        implication_summary=lead_candidate_temp.interpretation.why_it_matters_economically,
        portfolio_consequence=sleeve.summary,
        next_action=review_posture,
        report_tab="evidence",
        impact_level="medium" if source_count >= 2 else "low",
        requires_review=source_count >= 2,
    )
    final_contract = apply_overlay(base_contract, holdings=portfolio_overlay_summary)
    snapshot_id = record_surface_snapshot(
        surface_id="daily_brief",
        object_id="daily_brief",
        snapshot_kind="brief",
        state_label=review_posture,
        data_confidence=data_confidence,
        decision_confidence=decision_confidence,
        generated_at=str(final_contract.get("generated_at") or ""),
        contract=final_contract,
        input_summary={
            "source_count": source_count,
            "signal_count": len(decision_stack),
            "reference_clocks": [item["summary"] for item in data_timeframes[:5]],
            "forecast_providers": sorted(forecast_providers),
            "portfolio_overlay_present": bool(portfolio_overlay_summary),
        },
        decision_inputs=compact_replay_inputs(
            truth_envelopes={
                f"signal:{index}": dict(signal.get("truth_envelope") or {})
                for index, signal in enumerate(decision_stack[:12], start=1)
                if signal.get("truth_envelope")
            },
            extra={
                "signals": [
                    {
                        "label": str(signal.get("label") or ""),
                        "signal_kind": str(signal.get("signal_kind") or ""),
                        "truth_envelope": dict(signal.get("truth_envelope") or {}),
                        "mapping_directness": str(signal.get("mapping_directness") or ""),
                        "effect_type": str(signal.get("effect_type") or ""),
                        "next_action": str(signal.get("next_action") or ""),
                    }
                    for signal in decision_stack[:12]
                ],
                "scenario_blocks": [
                    {
                        "label": str(block.get("label") or ""),
                        "summary": str(block.get("summary") or ""),
                        "forecast_provider": str(dict(block.get("forecast_support") or {}).get("provider") or ""),
                    }
                    for block in scenario_blocks[:6]
                ],
                "portfolio_overlay_context": dict(portfolio_overlay_context or {}),
                "forecast_providers": sorted(forecast_providers),
                "data_timeframes": data_timeframes[:8],
            },
        ),
    )
    final_contract["surface_snapshot_id"] = snapshot_id
    return final_contract
