from __future__ import annotations

from app.v2.sources.benchmark_truth_adapter import BenchmarkTruthAdapter
from app.v2.sources.freshness_registry import FreshnessRegistry
from app.v2.sources.issuer_factsheet_adapter import IssuerFactsheetAdapter
from app.v2.sources.macro_adapter import MacroAdapter
from app.v2.sources.market_price_adapter import MarketPriceAdapter
from app.v2.sources.news_adapter import NewsAdapter
from app.v2.sources.types import FreshnessPolicy, SourceDefinition


# ---------------------------------------------------------------------------
# Source definition registry
# Keys must match the source_key strings used in translators/source_records.py
# and in surface contract builders.
# ---------------------------------------------------------------------------

_SOURCE_REGISTRY: dict[str, SourceDefinition] = {
    "market_price": SourceDefinition(
        key="market_price",
        name="Market Price",
        tier="1A",
        surface="shared",
        donor="MarketPriceAdapter",
        connector_kind="service",
        authoritative_fields=("price", "change_pct_1d", "previous_close"),
        freshness_policy=FreshnessPolicy(fresh_seconds=900, stale_seconds=86_400),
    ),
    "benchmark_truth": SourceDefinition(
        key="benchmark_truth",
        name="Benchmark Truth",
        tier="1A",
        surface="blueprint",
        donor="BenchmarkTruthAdapter",
        connector_kind="service",
        authoritative_fields=("current_value", "ytd_return_pct", "one_year_return_pct"),
        freshness_policy=FreshnessPolicy(fresh_seconds=43_200, stale_seconds=172_800),
    ),
    "macro": SourceDefinition(
        key="macro",
        name="Macro Series",
        tier="1B",
        surface="daily_brief",
        donor="MacroAdapter",
        connector_kind="service",
        authoritative_fields=("value", "reference_period", "release_date"),
        freshness_policy=FreshnessPolicy(fresh_seconds=86_400, stale_seconds=2_592_000),
    ),
    "news": SourceDefinition(
        key="news",
        name="Market News",
        tier="1B",
        surface="daily_brief",
        donor="NewsAdapter",
        connector_kind="service",
        authoritative_fields=("headline", "published_utc", "url"),
        freshness_policy=FreshnessPolicy(fresh_seconds=3_600, stale_seconds=86_400),
    ),
    "issuer_factsheet": SourceDefinition(
        key="issuer_factsheet",
        name="Issuer Factsheet",
        tier="1A",
        surface="blueprint",
        donor="IssuerFactsheetAdapter",
        connector_kind="service",
        authoritative_fields=("factsheet_date", "primary_documents", "issuer"),
        freshness_policy=FreshnessPolicy(fresh_seconds=604_800, stale_seconds=2_592_000),
    ),
    "blueprint_candidate_registry": SourceDefinition(
        key="blueprint_candidate_registry",
        name="Blueprint Candidate Registry",
        tier="1A",
        surface="blueprint",
        donor="SQLiteBlueprintDonor",
        connector_kind="database",
        authoritative_fields=("symbol", "sleeve_key", "asset_class"),
        freshness_policy=FreshnessPolicy(not_applicable=True, notes="Updated on registry seed; not time-bounded."),
    ),
    "blueprint_candidate_truth": SourceDefinition(
        key="blueprint_candidate_truth",
        name="Blueprint Candidate Truth",
        tier="1A",
        surface="blueprint",
        donor="SQLiteBlueprintDonor",
        connector_kind="database",
        authoritative_fields=("expense_ratio", "benchmark_key", "liquidity_score", "aum"),
        freshness_policy=FreshnessPolicy(fresh_seconds=86_400, stale_seconds=604_800),
    ),
    "blueprint_benchmark_assignment": SourceDefinition(
        key="blueprint_benchmark_assignment",
        name="Blueprint Benchmark Assignment",
        tier="1A",
        surface="blueprint",
        donor="SQLiteBlueprintDonor",
        connector_kind="database",
        authoritative_fields=("benchmark_key", "validation_status"),
        freshness_policy=FreshnessPolicy(not_applicable=True, notes="Static mapping; changes only on manual update."),
    ),
    "etf_document_verification": SourceDefinition(
        key="etf_document_verification",
        name="ETF Document Verification",
        tier="1A",
        surface="blueprint",
        donor="SQLiteEtfDonor",
        connector_kind="database",
        authoritative_fields=("factsheet_date", "verified", "doc_url"),
        freshness_policy=FreshnessPolicy(fresh_seconds=604_800, stale_seconds=2_592_000),
    ),
    "etf_market_state": SourceDefinition(
        key="etf_market_state",
        name="ETF Market State",
        tier="1A",
        surface="blueprint",
        donor="SQLiteEtfDonor",
        connector_kind="service",
        authoritative_fields=("price", "change_pct_1d", "aum", "holdings_count"),
        freshness_policy=FreshnessPolicy(fresh_seconds=3_600, stale_seconds=86_400),
    ),
    "provider_surface_context": SourceDefinition(
        key="provider_surface_context",
        name="Provider Surface Context",
        tier="2",
        surface="shared",
        donor="SQLiteProviderDonor",
        connector_kind="database",
        authoritative_fields=("providers", "snapshots"),
        freshness_policy=FreshnessPolicy(fresh_seconds=43_200, stale_seconds=172_800),
    ),
    "portfolio_holdings": SourceDefinition(
        key="portfolio_holdings",
        name="Portfolio Holdings",
        tier="2",
        surface="shared",
        donor="SQLitePortfolioDonor",
        connector_kind="database",
        authoritative_fields=("symbol", "weight", "cost_basis"),
        freshness_policy=FreshnessPolicy(fresh_seconds=86_400, stale_seconds=604_800),
    ),
    "portfolio_snapshot": SourceDefinition(
        key="portfolio_snapshot",
        name="Portfolio Snapshot",
        tier="2",
        surface="shared",
        donor="SQLitePortfolioDonor",
        connector_kind="database",
        authoritative_fields=("created_at", "total_value", "currency"),
        freshness_policy=FreshnessPolicy(fresh_seconds=86_400, stale_seconds=604_800),
    ),
}


def get_source_definition(key: str) -> SourceDefinition:
    """Return the SourceDefinition for the given source key.

    Raises KeyError with a clear message if the key is not registered.
    """
    try:
        return _SOURCE_REGISTRY[key]
    except KeyError:
        registered = sorted(_SOURCE_REGISTRY)
        raise KeyError(
            f"Source key {key!r} is not registered in the source registry. "
            f"Registered keys: {registered}"
        ) from None


_freshness_registry = FreshnessRegistry()

_issuer_adapter = IssuerFactsheetAdapter()
_benchmark_adapter = BenchmarkTruthAdapter()
_market_adapter = MarketPriceAdapter()
_news_adapter = NewsAdapter()
_macro_adapter = MacroAdapter()

_freshness_registry.register_source("issuer_factsheet", _issuer_adapter)
_freshness_registry.register_source("benchmark_truth", _benchmark_adapter)
_freshness_registry.register_source("market_price", _market_adapter)
_freshness_registry.register_source("news", _news_adapter)
_freshness_registry.register_source("macro", _macro_adapter)


def get_issuer_adapter() -> IssuerFactsheetAdapter:
    return _issuer_adapter


def get_benchmark_adapter() -> BenchmarkTruthAdapter:
    return _benchmark_adapter


def get_market_adapter() -> MarketPriceAdapter:
    return _market_adapter


def get_news_adapter() -> NewsAdapter:
    return _news_adapter


def get_macro_adapter() -> MacroAdapter:
    return _macro_adapter


def get_freshness_registry() -> FreshnessRegistry:
    return _freshness_registry
