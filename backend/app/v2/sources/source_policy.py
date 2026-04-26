from __future__ import annotations

import re
import sqlite3
from dataclasses import asdict, dataclass
from typing import Any, Literal

from app.services.blueprint_benchmark_registry import resolve_benchmark_assignment
from app.services.blueprint_candidate_registry import export_live_candidate_registry, list_live_candidate_symbols
from app.services.portfolio_ingest import latest_snapshot_rows
from app.v2.core.market_strip_registry import daily_brief_targets


AuthorityKind = Literal["live_authoritative", "local_authoritative", "doc_authoritative", "derived", "unavailable"]


@dataclass(frozen=True, slots=True)
class FieldSourcePolicy:
    authority_kind: AuthorityKind
    source_family: str
    primary_provider_order: tuple[str, ...]
    secondary_provider_order: tuple[str, ...] = ()
    cache_policy: str = "continuity_only"
    semantic_sufficiency: str = "field_present"


_FIELD_POLICIES: dict[str, dict[str, FieldSourcePolicy]] = {
    "daily_brief": {
        "market_state_cards.current_value": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="market_strip",
            primary_provider_order=("benchmark_proxy", "quote_latest"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="movement_capable_market_truth",
        ),
        "market_state_cards.change_pct_1d": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="market_strip",
            primary_provider_order=("benchmark_proxy", "quote_latest"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="movement_capable_market_truth",
        ),
        "market_state_cards.movement_state": FieldSourcePolicy(
            authority_kind="derived",
            source_family="market_strip",
            primary_provider_order=("benchmark_proxy", "quote_latest"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="movement_semantics_required",
        ),
        "market_state_cards.runtime_provenance": FieldSourcePolicy(
            authority_kind="derived",
            source_family="market_strip",
            primary_provider_order=("benchmark_proxy", "quote_latest"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="runtime_trace_present",
        ),
        "what_changed.macro_signal": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="macro",
            primary_provider_order=("fred",),
            secondary_provider_order=("csv_cache",),
            cache_policy="fallback_only",
            semantic_sufficiency="value_and_reference_period",
        ),
        "what_changed.news_signal": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="news",
            primary_provider_order=("finnhub",),
            secondary_provider_order=("gdelt",),
            cache_policy="not_used",
            semantic_sufficiency="headline_and_timestamp",
        ),
    },
    "blueprint": {
        "candidate.price": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="quote_latest",
            primary_provider_order=("polygon", "finnhub", "twelve_data", "alpha_vantage"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="price_present",
        ),
        "candidate.change_pct_1d": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="quote_latest",
            primary_provider_order=("polygon", "finnhub", "twelve_data", "alpha_vantage"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="movement_present_or_derived",
        ),
        "candidate.primary_listing_exchange": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="reference_meta",
            primary_provider_order=("fmp", "finnhub", "eodhd"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="field_present",
        ),
        "candidate.primary_trading_currency": FieldSourcePolicy(
            authority_kind="local_authoritative",
            source_family="blueprint_candidate_truth",
            primary_provider_order=("sqlite_truth",),
            cache_policy="authoritative_local",
            semantic_sufficiency="field_present",
        ),
        "candidate.aum": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="fundamentals",
            primary_provider_order=("fmp", "finnhub"),
            secondary_provider_order=("reference_meta", "cache"),
            semantic_sufficiency="field_present",
        ),
        "candidate.holdings_count": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="reference_meta",
            primary_provider_order=("fmp", "eodhd", "finnhub"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="field_present",
        ),
        "candidate.expense_ratio": FieldSourcePolicy(
            authority_kind="local_authoritative",
            source_family="blueprint_candidate_truth",
            primary_provider_order=("sqlite_truth",),
            cache_policy="authoritative_local",
            semantic_sufficiency="field_present",
        ),
        "candidate.replication_method": FieldSourcePolicy(
            authority_kind="doc_authoritative",
            source_family="issuer_factsheet",
            primary_provider_order=("issuer_document_manifest",),
            cache_policy="authoritative_doc",
            semantic_sufficiency="field_present",
        ),
        "candidate.bid_ask_spread_proxy": FieldSourcePolicy(
            authority_kind="local_authoritative",
            source_family="blueprint_candidate_truth",
            primary_provider_order=("sqlite_truth",),
            cache_policy="authoritative_local",
            semantic_sufficiency="field_present",
        ),
        "candidate.premium_discount_behavior": FieldSourcePolicy(
            authority_kind="doc_authoritative",
            source_family="issuer_factsheet",
            primary_provider_order=("issuer_document_manifest",),
            cache_policy="authoritative_doc",
            semantic_sufficiency="field_present",
        ),
        "candidate.primary_documents": FieldSourcePolicy(
            authority_kind="doc_authoritative",
            source_family="issuer_factsheet",
            primary_provider_order=("issuer_document_manifest",),
            cache_policy="authoritative_doc",
            semantic_sufficiency="document_present",
        ),
    },
    "candidate_report": {
        "instrument.price": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="quote_latest",
            primary_provider_order=("polygon", "finnhub", "twelve_data", "alpha_vantage"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="price_present",
        ),
        "instrument.change_pct_1d": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="quote_latest",
            primary_provider_order=("polygon", "finnhub", "twelve_data", "alpha_vantage"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="movement_present_or_derived",
        ),
        "benchmark.current_value": FieldSourcePolicy(
            authority_kind="live_authoritative",
            source_family="quote_latest",
            primary_provider_order=("market_price_adapter",),
            secondary_provider_order=("cache",),
            semantic_sufficiency="price_present",
        ),
        "benchmark.ytd_return_pct": FieldSourcePolicy(
            authority_kind="derived",
            source_family="ohlcv_history",
            primary_provider_order=("tiingo", "alpha_vantage", "polygon"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="history_points_present",
        ),
        "benchmark.one_year_return_pct": FieldSourcePolicy(
            authority_kind="derived",
            source_family="ohlcv_history",
            primary_provider_order=("tiingo", "alpha_vantage", "polygon"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="history_points_present",
        ),
        "market_history_block.field_provenance": FieldSourcePolicy(
            authority_kind="derived",
            source_family="candidate_report",
            primary_provider_order=("quote_latest", "ohlcv_history", "reference_meta"),
            secondary_provider_order=("cache",),
            semantic_sufficiency="runtime_trace_present",
        ),
    },
    "evidence_workspace": {
        "documents.primary_documents": FieldSourcePolicy(
            authority_kind="doc_authoritative",
            source_family="issuer_factsheet",
            primary_provider_order=("issuer_document_manifest",),
            cache_policy="authoritative_doc",
            semantic_sufficiency="document_present",
        ),
        "documents.field_support_map": FieldSourcePolicy(
            authority_kind="derived",
            source_family="evidence_workspace",
            primary_provider_order=("source_records",),
            cache_policy="persisted",
            semantic_sufficiency="mapped_support_present",
        ),
    },
}


def source_policy_map() -> dict[str, dict[str, dict[str, Any]]]:
    return {
        surface: {
            field_name: asdict(policy)
            for field_name, policy in sorted(fields.items())
        }
        for surface, fields in sorted(_FIELD_POLICIES.items())
    }


def blueprint_targets_from_policy(conn: sqlite3.Connection) -> dict[str, list[str]]:
    symbols: list[str] = []
    for symbol in list_live_candidate_symbols(conn):
        normalized = str(symbol).strip().upper()
        if normalized and re.fullmatch(r"[A-Z0-9.^\\-]{1,16}", normalized) and normalized not in symbols:
            symbols.append(normalized)
    holdings_optional = []
    for row in latest_snapshot_rows(conn):
        holding_symbol = str(row.get("symbol") or row.get("ticker") or "").strip().upper()
        if holding_symbol and re.fullmatch(r"[A-Z0-9.^\\-]{1,16}", holding_symbol):
            holdings_optional.append(holding_symbol)
    for holding_symbol in holdings_optional:
        if holding_symbol not in symbols:
            symbols.append(holding_symbol)
    benchmark_proxies: list[str] = []
    for candidate in export_live_candidate_registry(conn):
        sleeve_key = str(candidate.get("sleeve_key") or "").strip()
        assignment = resolve_benchmark_assignment(conn, candidate=candidate, sleeve_key=sleeve_key)
        proxy_symbol = str(assignment.get("benchmark_proxy_symbol") or "").strip().upper()
        if proxy_symbol and re.fullmatch(r"[A-Z0-9.^\\-]{1,16}", proxy_symbol) and proxy_symbol not in benchmark_proxies:
            benchmark_proxies.append(proxy_symbol)
    # Merge benchmark proxies into ohlcv_history targets so ytd_return_pct etc. are fetchable
    ohlcv_symbols = list(dict.fromkeys(symbols[:24] + benchmark_proxies[:8]))
    return {
        "quote_latest": symbols[:24],
        "reference_meta": symbols[:40],
        "fundamentals": symbols[:24],
        "ohlcv_history": ohlcv_symbols,
        "benchmark_proxy": benchmark_proxies[:16],
        "etf_profile": symbols[:24],
        "etf_holdings": symbols[:24],
    }


def daily_brief_targets_from_policy() -> dict[str, list[str]]:
    return daily_brief_targets()
