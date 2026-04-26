from __future__ import annotations

import sqlite3
from typing import TYPE_CHECKING, Any

from app.config import get_db_path
from app.services.provider_cache import get_cached_provider_snapshot
from app.services.blueprint_benchmark_registry import resolve_benchmark_assignment
from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, export_live_candidate_registry, seed_default_candidate_registry
from app.services.provider_refresh import _extract_provider_candidate_fields
from app.services.provider_refresh import fetch_routed_family
from app.services.provider_registry import provider_support_status, routed_provider_candidates
from app.v2.donors.blueprint import SQLiteBlueprintDonor
from app.v2.sources.registry import get_market_adapter
from app.v2.translators.instrument_truth_translator import translate
from app.v2.sources.registry import get_issuer_adapter

if TYPE_CHECKING:
    from app.v2.core.domain_objects import InstrumentTruth


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _normalize_identifier(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return ""
    if raw.startswith("candidate_instrument_"):
        return raw.removeprefix("candidate_instrument_").upper()
    if raw.startswith("candidate_"):
        return raw.removeprefix("candidate_").replace("instrument_", "").upper()
    if raw.startswith("instrument_"):
        return raw.removeprefix("instrument_").upper()
    return raw.upper()


def _pick_candidate(candidates: list[dict[str, Any]], identifier: str) -> dict[str, Any] | None:
    normalized = _normalize_identifier(identifier)
    if not normalized:
        return None

    matches = [
        candidate
        for candidate in candidates
        if str(candidate.get("symbol") or "").strip().upper() == normalized
        or str(candidate.get("registry_id") or "").strip() == identifier
    ]
    if not matches:
        return None

    preferred = next(
        (
            candidate
            for candidate in matches
            if str(candidate.get("symbol") or "").strip().upper() == normalized
            and str(candidate.get("sleeve_key") or "").strip()
        ),
        None,
    )
    return preferred or matches[0]


def _cached_routed_payload(
    conn: sqlite3.Connection,
    *,
    surface_name: str,
    endpoint_family: str,
    identifier: str,
    allow_live_fetch: bool = True,
) -> dict[str, Any]:
    normalized = str(identifier or "").strip().upper()
    if not normalized:
        return {}
    for provider_name in routed_provider_candidates(endpoint_family, identifier=normalized):
        supported, _ = provider_support_status(provider_name, endpoint_family, normalized)
        if not supported:
            continue
        snapshot = get_cached_provider_snapshot(
            conn,
            provider_name=provider_name,
            endpoint_family=endpoint_family,
            cache_key=normalized,
            surface_name=surface_name,
        )
        if snapshot is None:
            continue
        payload = dict(snapshot.get("payload") or {})
        payload.setdefault("provider_name", provider_name)
        payload.setdefault("retrieval_path", "routed_cache")
        payload.setdefault("cache_status", str(snapshot.get("cache_status") or "hit"))
        payload.setdefault("freshness_state", snapshot.get("freshness_state"))
        return payload
    if not allow_live_fetch:
        return {}
    payload = fetch_routed_family(
        conn,
        surface_name=surface_name,
        endpoint_family=endpoint_family,
        identifier=normalized,
        triggered_by_job="instrument_truth",
        force_refresh=False,
    )
    if payload.get("provider_name") or payload.get("value") is not None or payload.get("price") is not None:
        return payload
    return {}


def _market_field_provenance(
    *,
    live_market: dict[str, Any],
    field_name: str,
) -> dict[str, Any]:
    execution = dict(live_market.get("provider_execution") or {})
    has_value = live_market.get(field_name) is not None
    is_price_field = field_name == "price"
    usable_truth = execution.get("usable_truth")
    sufficiency_state = execution.get("sufficiency_state")
    authority_level = execution.get("authority_level")
    data_mode = execution.get("data_mode")
    provenance_strength = execution.get("provenance_strength")
    if is_price_field and has_value:
        usable_truth = True
        sufficiency_state = "price_present"
        authority_level = "live_authoritative"
        data_mode = str(execution.get("live_or_cache") or data_mode or "live")
        provenance_strength = "cache_continuity" if str(execution.get("live_or_cache") or "").strip() == "cache" else "live_authoritative"
    return {
        "authority_kind": "live_authoritative" if has_value else "unavailable",
        "source_family": str(execution.get("source_family") or "quote_latest"),
        "provider": live_market.get("provider_name") or execution.get("provider_name"),
        "path": live_market.get("retrieval_path") or execution.get("path_used"),
        "live_or_cache": execution.get("live_or_cache"),
        "usable_truth": usable_truth if is_price_field else has_value,
        "sufficiency_state": sufficiency_state if has_value else "insufficient",
        "data_mode": data_mode,
        "authority_level": authority_level,
        "observed_at": live_market.get("as_of_utc") or execution.get("observed_at") or live_market.get("observed_at"),
        "provenance_strength": provenance_strength,
        "insufficiency_reason": execution.get("insufficiency_reason") if not has_value else None,
        "truth_envelope": dict(live_market.get("truth_envelope") or {}) or None,
    }


def get_instrument_truth(ticker: str, *, allow_live_fetch: bool = True) -> "InstrumentTruth":
    """Returns InstrumentTruth for a given ticker. Wraps blueprint candidate donors."""
    with _connection() as conn:
        ensure_candidate_registry_tables(conn)
        donor = SQLiteBlueprintDonor(conn)
        candidates = donor.list_candidates()
        if not candidates:
            seed_default_candidate_registry(conn)
            candidates = export_live_candidate_registry(conn)

        candidate = _pick_candidate(candidates, ticker)
        if candidate is None:
            symbol = _normalize_identifier(ticker) or "UNKNOWN"
            return translate(
                {
                    "symbol": symbol,
                    "ticker": symbol,
                    "name": symbol,
                    "asset_class": "unknown",
                    "vehicle_type": "unknown",
                }
            )

        sleeve_key = str(candidate.get("sleeve_key") or "").strip()
        benchmark_assignment = (
            donor.resolve_benchmark_assignment(candidate=candidate, sleeve_key=sleeve_key)
            if sleeve_key
            else resolve_benchmark_assignment(conn, candidate=candidate, sleeve_key="")
        )
        extra = dict(candidate.get("extra") or {})
        issuer_payload = {}
        if allow_live_fetch:
            try:
                payload = get_issuer_adapter().fetch(str(candidate.get("symbol") or ""))
                if isinstance(payload, dict):
                    issuer_payload = payload
            except Exception:
                issuer_payload = {}
        provider_reference_meta: dict[str, Any] = {}
        provider_fundamentals: dict[str, Any] = {}
        symbol = str(candidate.get("symbol") or "")
        provider_reference_meta = _cached_routed_payload(
            conn,
            surface_name="blueprint",
            endpoint_family="reference_meta",
            identifier=symbol,
            allow_live_fetch=allow_live_fetch,
        )
        provider_fundamentals = _cached_routed_payload(
            conn,
            surface_name="blueprint",
            endpoint_family="fundamentals",
            identifier=symbol,
            allow_live_fetch=allow_live_fetch,
        )
        reference_fields = _extract_provider_candidate_fields("reference_meta", provider_reference_meta)
        fundamental_fields = _extract_provider_candidate_fields("fundamentals", provider_fundamentals)
        live_market = _cached_routed_payload(
            conn,
            surface_name="blueprint",
            endpoint_family="quote_latest",
            identifier=symbol,
            allow_live_fetch=allow_live_fetch,
        )
        if allow_live_fetch and (not live_market or live_market.get("price") is None):
            try:
                fallback_market = get_market_adapter().fetch(symbol, surface_name="blueprint")
                if isinstance(fallback_market, dict):
                    live_market = dict(fallback_market)
            except Exception:
                live_market = live_market or {}
        live_price = live_market.get("price")
        live_change_pct = live_market.get("change_pct_1d")
        resolved_exchange = extra.get("primary_listing_exchange") or reference_fields.get("primary_listing_exchange")
        resolved_currency = extra.get("primary_trading_currency") or reference_fields.get("primary_trading_currency") or issuer_payload.get("base_currency") or "USD"
        resolved_aum = (
            fundamental_fields.get("aum")
            or reference_fields.get("aum")
            or issuer_payload.get("aum_usd")
            or extra.get("aum_usd")
            or candidate.get("aum_usd")
            or candidate.get("aum")
        )
        resolved_holdings_count = fundamental_fields.get("holdings_count") or reference_fields.get("holdings_count")
        resolved_expense_ratio = fundamental_fields.get("expense_ratio")
        if resolved_expense_ratio is None:
            resolved_expense_ratio = reference_fields.get("expense_ratio")
        if resolved_expense_ratio is None and issuer_payload.get("ter") is not None:
            resolved_expense_ratio = issuer_payload.get("ter")
        if resolved_expense_ratio is None:
            resolved_expense_ratio = candidate.get("expense_ratio")
        field_provenance = {
            "price": _market_field_provenance(live_market=live_market, field_name="price"),
            "change_pct_1d": _market_field_provenance(live_market=live_market, field_name="change_pct_1d"),
            "primary_listing_exchange": {
                "authority_kind": "local_authoritative" if extra.get("primary_listing_exchange") else "live_authoritative" if reference_fields.get("primary_listing_exchange") else "unavailable",
                "source_family": "blueprint_candidate_truth" if extra.get("primary_listing_exchange") else "reference_meta" if reference_fields.get("primary_listing_exchange") else None,
                "provider": None if extra.get("primary_listing_exchange") else provider_reference_meta.get("provider_name"),
            },
            "primary_trading_currency": {
                "authority_kind": "local_authoritative"
                if extra.get("primary_trading_currency")
                else "live_authoritative"
                if reference_fields.get("primary_trading_currency")
                else "doc_authoritative"
                if issuer_payload.get("base_currency")
                else "unavailable",
                "source_family": "blueprint_candidate_truth"
                if extra.get("primary_trading_currency")
                else "reference_meta"
                if reference_fields.get("primary_trading_currency")
                else "issuer_factsheet"
                if issuer_payload.get("base_currency")
                else None,
                "provider": None if extra.get("primary_trading_currency") else provider_reference_meta.get("provider_name"),
            },
            "aum": {
                "authority_kind": "live_authoritative" if fundamental_fields.get("aum") or reference_fields.get("aum") else "doc_authoritative",
                "source_family": "fundamentals"
                if fundamental_fields.get("aum")
                else "reference_meta"
                if reference_fields.get("aum")
                else "issuer_factsheet",
                "provider": provider_fundamentals.get("provider_name") or provider_reference_meta.get("provider_name"),
            },
            "holdings_count": {
                "authority_kind": "live_authoritative" if resolved_holdings_count not in {None, ""} else "unavailable",
                "source_family": "fundamentals" if fundamental_fields.get("holdings_count") not in {None, ""} else "reference_meta",
                "provider": provider_fundamentals.get("provider_name") or provider_reference_meta.get("provider_name"),
            },
            "expense_ratio": {
                "authority_kind": "live_authoritative"
                if fundamental_fields.get("expense_ratio") is not None or reference_fields.get("expense_ratio") is not None
                else "local_authoritative"
                if candidate.get("expense_ratio") is not None
                else "doc_authoritative",
                "source_family": "fundamentals"
                if fundamental_fields.get("expense_ratio") is not None
                else "reference_meta"
                if reference_fields.get("expense_ratio") is not None
                else "blueprint_candidate_truth"
                if candidate.get("expense_ratio") is not None
                else "issuer_factsheet",
                "provider": provider_fundamentals.get("provider_name") or provider_reference_meta.get("provider_name"),
            },
            "replication_method": {
                "authority_kind": "doc_authoritative" if candidate.get("replication_method") or issuer_payload.get("vehicle_type") else "unavailable",
                "source_family": "issuer_factsheet",
            },
            "bid_ask_spread_proxy": {
                "authority_kind": "local_authoritative" if extra.get("bid_ask_spread_proxy") is not None else "unavailable",
                "source_family": "blueprint_candidate_truth",
            },
            "premium_discount_behavior": {
                "authority_kind": "doc_authoritative" if extra.get("premium_discount_behavior") else "unavailable",
                "source_family": "issuer_factsheet",
            },
        }
        translated = translate(
            {
                "symbol": candidate.get("symbol"),
                "ticker": candidate.get("symbol"),
                "name": reference_fields.get("fund_name") or issuer_payload.get("name") or candidate.get("name"),
                "asset_class": issuer_payload.get("asset_class") or candidate.get("asset_class") or candidate.get("instrument_type") or "unknown",
                "vehicle_type": issuer_payload.get("vehicle_type") or candidate.get("instrument_type"),
                "benchmark_id": benchmark_assignment.get("benchmark_key") or issuer_payload.get("benchmark_id") or candidate.get("benchmark_key"),
                "domicile": issuer_payload.get("domicile") or candidate.get("domicile"),
                "base_currency": resolved_currency,
                "issuer": reference_fields.get("issuer") or issuer_payload.get("issuer") or candidate.get("issuer"),
                "ter": resolved_expense_ratio,
                "aum_usd": resolved_aum,
                "inception_date": issuer_payload.get("inception_date") or extra.get("launch_date") or extra.get("inception_date"),
                "factsheet_date": issuer_payload.get("factsheet_date"),
                "docs": issuer_payload.get("docs"),
                "primary_documents": issuer_payload.get("primary_documents"),
                "verification_missing": issuer_payload.get("verification_missing"),
            }
        )
        return translated.model_copy(
            update={
                "metrics": {
                    **translated.metrics,
                    "issuer": issuer_payload.get("issuer") or candidate.get("issuer"),
                    "expense_ratio": resolved_expense_ratio,
                    "liquidity_score": candidate.get("liquidity_score"),
                    "source_state": candidate.get("source_state"),
                    "sleeve_affiliation": sleeve_key,
                    "sleeve_key": sleeve_key,
                    "price": live_price,
                    "change_pct_1d": live_change_pct,
                    "holdings_count": resolved_holdings_count,
                    "source_provenance": field_provenance,
                    "benchmark_authority_level": "direct"
                    if str(benchmark_assignment.get("benchmark_confidence") or "").strip().lower() == "high"
                    else "bounded",
                    "benchmark_label": benchmark_assignment.get("benchmark_label"),
                    "replication_method": candidate.get("replication_method") or issuer_payload.get("vehicle_type"),
                    "primary_listing_exchange": resolved_exchange,
                    "primary_trading_currency": resolved_currency,
                    "aum": resolved_aum,
                    "bid_ask_spread_proxy": extra.get("bid_ask_spread_proxy"),
                    "premium_discount_behavior": extra.get("premium_discount_behavior"),
                    "distribution_policy": reference_fields.get("distribution_type") or reference_fields.get("share_class") or extra.get("distribution_type") or candidate.get("share_class"),
                    "launch_date": issuer_payload.get("inception_date") or extra.get("launch_date") or extra.get("inception_date"),
                    "fund_age_years": extra.get("fund_age_years"),
                    "tracking_difference_1y": extra.get("tracking_difference_1y"),
                    "tracking_difference_3y": extra.get("tracking_difference_3y"),
                    "tracking_difference_5y": extra.get("tracking_difference_5y"),
                    "tracking_difference_evidence_class": "issuer_or_registry_seed"
                    if any(extra.get(key) is not None for key in ("tracking_difference_1y", "tracking_difference_3y", "tracking_difference_5y"))
                    else "missing",
                    "fund_domicile": issuer_payload.get("domicile") or candidate.get("domicile"),
                    "primary_documents": translated.metrics.get("primary_documents"),
                    "verification_missing": translated.metrics.get("verification_missing"),
                    "implementation_quality_fields_present": [
                        key
                        for key, value in {
                            "expense_ratio": resolved_expense_ratio,
                            "replication_method": candidate.get("replication_method"),
                            "primary_listing_exchange": resolved_exchange,
                            "primary_trading_currency": resolved_currency,
                            "aum": resolved_aum,
                            "bid_ask_spread_proxy": extra.get("bid_ask_spread_proxy"),
                        }.items()
                        if value not in {None, ""}
                    ],
                }
            }
        )
