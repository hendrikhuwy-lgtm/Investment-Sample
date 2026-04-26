from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest


def test_source_policy_map_declares_investor_facing_provenance() -> None:
    from app.v2.sources.source_policy import source_policy_map

    policies = source_policy_map()

    assert policies["daily_brief"]["market_state_cards.change_pct_1d"]["authority_kind"] == "live_authoritative"
    assert policies["daily_brief"]["market_state_cards.movement_state"]["authority_kind"] == "derived"
    assert policies["daily_brief"]["market_state_cards.runtime_provenance"]["authority_kind"] == "derived"
    assert policies["blueprint"]["candidate.price"]["source_family"] == "quote_latest"
    assert policies["blueprint"]["candidate.expense_ratio"]["authority_kind"] == "local_authoritative"
    assert policies["candidate_report"]["benchmark.ytd_return_pct"]["source_family"] == "ohlcv_history"
    assert policies["candidate_report"]["market_history_block.field_provenance"]["authority_kind"] == "derived"
    assert policies["evidence_workspace"]["documents.primary_documents"]["authority_kind"] == "doc_authoritative"


def test_blueprint_targets_derive_from_surface_demand(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "policy_targets.sqlite3"))

    from app.config import get_db_path
    from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, seed_default_candidate_registry
    from app.v2.sources.source_policy import blueprint_targets_from_policy

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_candidate_registry_tables(conn)
    seed_default_candidate_registry(conn)

    targets = blueprint_targets_from_policy(conn)

    assert {"quote_latest", "reference_meta", "fundamentals", "ohlcv_history", "benchmark_proxy", "etf_profile", "etf_holdings"} <= set(targets)
    assert targets["quote_latest"]
    assert targets["reference_meta"]
    assert targets["etf_holdings"]
    assert "ACWI" in targets["benchmark_proxy"] or targets["benchmark_proxy"]


def test_benchmark_truth_uses_quote_and_history_families(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.sources.benchmark_truth_adapter as adapter

    monkeypatch.setattr(
        adapter,
        "fetch_market_price",
        lambda symbol, surface_name=None: {
            "price": 121.0,
            "provider_name": "polygon",
            "retrieval_path": "direct_live",
            "as_of_utc": "2026-04-06T00:00:00+00:00",
        },
    )
    monkeypatch.setattr(adapter, "_connection", lambda: sqlite3.connect(":memory:"))
    monkeypatch.setattr(
        adapter,
        "_fetch_history_payload",
        lambda symbol, surface_name=None: {
            "provider_name": "tiingo",
            "retrieval_path": "routed_live",
            "cache_status": "miss",
            "freshness_state": "current",
            "series": [
                {"date": "2025-01-02T00:00:00+00:00", "close": 100.0},
                {"date": "2025-04-06T00:00:00+00:00", "close": 110.0},
                {"date": "2026-04-06T00:00:00+00:00", "close": 121.0},
            ],
        },
    )

    payload = adapter.fetch("FTSE_ALL_WORLD", surface_name="candidate_report")

    assert payload["current_value"] == 121.0
    assert payload["ytd_return_pct"] is not None
    assert payload["one_year_return_pct"] is not None
    assert payload["field_provenance"]["current_value"]["source_family"] == "quote_latest"
    assert payload["field_provenance"]["ytd_return_pct"]["source_family"] == "ohlcv_history"
    assert payload["field_provenance"]["current_value"]["usable_truth"] is True
    assert payload["field_provenance"]["ytd_return_pct"]["sufficiency_state"] == "history_capable"


def test_benchmark_history_falls_through_to_direct_provider_when_routed_path_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.sources.benchmark_truth_adapter as adapter

    monkeypatch.setattr(adapter, "_cached_family_payload", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        adapter,
        "_connection",
        lambda: sqlite3.connect(":memory:"),
    )
    monkeypatch.setattr(
        "app.services.provider_refresh.fetch_routed_family",
        lambda *args, **kwargs: {
            "provider_name": None,
            "cache_status": "unavailable",
            "error_state": "history_cache_missing",
            "retrieval_path": "routed_unavailable",
        },
    )
    monkeypatch.setattr(adapter, "routed_provider_candidates", lambda *args, **kwargs: ["tiingo", "polygon"])
    monkeypatch.setattr(adapter, "provider_support_status", lambda *args, **kwargs: (True, None))

    def _direct(provider: str, endpoint_family: str, identifier: str) -> dict[str, object]:
        if provider == "tiingo":
            raise adapter.ProviderAdapterError(provider, endpoint_family, "rate limited", error_class="rate_limited")
        return {
            "provider_name": "polygon",
            "series": [
                {"date": "2025-01-02T00:00:00+00:00", "close": 100.0},
                {"date": "2026-04-06T00:00:00+00:00", "close": 121.0},
            ],
        }

    monkeypatch.setattr(adapter, "fetch_provider_data", _direct)

    payload = adapter._fetch_history_payload("MCHI", surface_name="candidate_report")

    assert payload["provider_name"] == "polygon"
    assert payload["retrieval_path"] == "direct_live"
    assert len(payload["series"]) == 2


def test_benchmark_history_falls_back_to_official_proxy_summary_when_series_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.sources.benchmark_truth_adapter as adapter

    monkeypatch.setattr(
        adapter,
        "_fetch_history_payload",
        lambda symbol, surface_name=None: {
            "provider_name": None,
            "cache_status": "unavailable",
            "error_state": "history_missing",
            "retrieval_path": "routed_unavailable",
        },
    )
    monkeypatch.setattr(
        adapter,
        "_official_proxy_performance_fallback",
        lambda proxy_symbol: {
            "provider_name": "ssga",
            "retrieval_path": "official_summary_live",
            "freshness_state": "current",
            "observed_at": "2026-04-02T00:00:00+00:00",
            "ytd_return_pct": 0.8985,
            "one_year_return_pct": 4.1545,
            "provider_execution": {
                "provider_name": "ssga",
                "source_family": "benchmark_proxy",
                "path_used": "official_summary_live",
                "live_or_cache": "live",
                "usable_truth": True,
                "sufficiency_state": "summary_return_available",
                "data_mode": "live",
                "authority_level": "proxy",
                "observed_at": "2026-04-02T00:00:00+00:00",
                "provenance_strength": "live_authoritative",
            },
        },
    )

    payload = adapter._history_fields("BIL", surface_name="candidate_report")

    assert payload["ytd_return_pct"] == pytest.approx(0.8985)
    assert payload["one_year_return_pct"] == pytest.approx(4.1545)
    assert payload["field_provenance"]["ytd_return_pct"]["source_family"] == "benchmark_proxy"
    assert payload["field_provenance"]["ytd_return_pct"]["sufficiency_state"] == "summary_return_available"


def test_instrument_truth_partially_live_hydrates_without_fake_completeness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "instrument_truth.sqlite3"))

    from app.config import get_db_path
    from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, seed_default_candidate_registry
    import app.v2.donors.instrument_truth as donor

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_candidate_registry_tables(conn)
    seed_default_candidate_registry(conn)
    conn.close()

    class StubIssuer:
        def fetch(self, symbol: str) -> dict[str, object]:
            return {
                "name": f"{symbol} issuer payload",
                "issuer": "Issuer Co",
                "asset_class": "equity",
                "vehicle_type": "etf",
                "domicile": "IE",
                "base_currency": "USD",
                "ter": 0.22,
                "primary_documents": [],
            }

    class StubMarket:
        def fetch(self, symbol: str, *, surface_name: str | None = None) -> dict[str, object]:
            return {
                "price": 101.5,
                "change_pct_1d": 1.2,
                "provider_name": "polygon",
                "retrieval_path": "direct_live",
            }

    monkeypatch.setattr(donor, "get_issuer_adapter", lambda: StubIssuer())
    monkeypatch.setattr(donor, "get_market_adapter", lambda: StubMarket())

    def _fake_routed(conn, *, endpoint_family: str, **kwargs):
        if endpoint_family == "reference_meta":
            return {
                "provider_name": "fmp",
                "value": {
                    "companyName": "VWRA",
                    "exchange": "NYSEARCA",
                    "currency": "USD",
                    "ETF_Data": {"Holdings_Count": 3500, "TotalAssets": 5000000000},
                },
            }
        return {
            "provider_name": "fmp",
            "value": {
                "aum": 6000000000,
            },
        }

    monkeypatch.setattr(donor, "fetch_routed_family", _fake_routed)

    truth = donor.get_instrument_truth("VWRA")

    assert truth.metrics["price"] == 101.5
    assert truth.metrics["change_pct_1d"] == 1.2
    assert truth.metrics["primary_listing_exchange"] == "NYSEARCA"
    assert truth.metrics["holdings_count"] == 3500
    assert truth.metrics["source_provenance"]["price"]["authority_kind"] == "live_authoritative"
    assert truth.metrics["source_provenance"]["premium_discount_behavior"]["authority_kind"] == "unavailable"


def test_runtime_truth_rows_support_direct_and_routed_sources() -> None:
    from app.v2.sources.runtime_truth import record_runtime_truth, reset_runtime_truth, runtime_truth_rows

    reset_runtime_truth()
    record_runtime_truth(
        source_id="market_price",
        source_family="quote_latest",
        field_name="price",
        symbol_or_entity="ACWI",
        provider_used="polygon",
        path_used="direct_live",
        live_or_cache="live",
        usable_truth=True,
        freshness="current",
        insufficiency_reason=None,
        semantic_grade="movement_capable",
        investor_surface="daily_brief",
        attempt_succeeded=True,
    )
    record_runtime_truth(
        source_id="routed_provider:reference_meta",
        source_family="reference_meta",
        field_name="reference_meta",
        symbol_or_entity="VWRA",
        provider_used="fmp",
        path_used="routed_live",
        live_or_cache="live",
        usable_truth=True,
        freshness="current",
        insufficiency_reason=None,
        semantic_grade="field_present",
        investor_surface="blueprint",
        attempt_succeeded=True,
    )

    rows = runtime_truth_rows(default_sources=[{"source_id": "macro", "source_family": "macro", "configured": True, "current_status_reason": "fresh"}])
    source_ids = {row["source_id"] for row in rows}

    assert "market_price" in source_ids
    assert "routed_provider:reference_meta" in source_ids
    assert "macro" in source_ids


def test_dxy_quote_routing_prefers_twelve_data_then_alpha_vantage() -> None:
    from app.services.provider_registry import routed_provider_candidates

    candidates = routed_provider_candidates("quote_latest", identifier="DXY")

    assert candidates[:2] == ["twelve_data", "alpha_vantage"]


def test_daily_brief_signal_runtime_provenance_marks_proxy_semantics() -> None:
    from app.v2.surfaces.daily_brief.contract_builder import _signal_runtime_provenance

    provenance = _signal_runtime_provenance(
        {
            "signal_kind": "market",
            "symbol": "DXY",
            "retrieval_path": "fallback_derived",
            "movement_state": "proxy",
            "provider_used": "frankfurter",
            "freshness_detail": "stored_valid_context",
            "provider_execution": {
                "provider_name": "frankfurter",
                "source_family": "usd_strength_fallback",
                "path_used": "fallback_derived",
                "live_or_cache": "fallback",
                "usable_truth": True,
                "freshness_class": "stored_valid_context",
                "semantic_grade": "derived_proxy",
                "sufficiency_state": "proxy_bounded",
                "data_mode": "derived",
                "authority_level": "derived",
                "provenance_strength": "derived_or_proxy",
            },
            "truth_envelope": {
                "source_authority": "Frankfurter",
                "acquisition_mode": "fallback",
                "degradation_reason": "proxy_only",
            },
        }
    )

    assert provenance["source_family"] == "usd_strength_fallback"
    assert provenance["live_or_cache"] == "fallback"
    assert provenance["provenance_strength"] == "derived_or_proxy"
    assert provenance["derived_or_proxy"] is True
    assert provenance["usable_truth"] is True
    assert provenance["sufficiency_state"] == "proxy_bounded"


def test_candidate_report_market_history_helper_emits_field_provenance() -> None:
    from app.v2.surfaces.blueprint.report_contract_builder import _market_history_block

    truth = SimpleNamespace(
        name="VWRA",
        metrics={
            "source_provenance": {
                "price": {
                    "authority_kind": "live_authoritative",
                    "source_family": "quote_latest",
                    "provider": "polygon",
                    "path": "direct_live",
                    "usable_truth": True,
                    "sufficiency_state": "movement_capable",
                    "data_mode": "live",
                    "authority_level": "direct",
                    "observed_at": "2026-04-06T00:00:00+00:00",
                    "provenance_strength": "live_authoritative",
                },
                "change_pct_1d": {
                    "authority_kind": "live_authoritative",
                    "source_family": "quote_latest",
                    "provider": "polygon",
                    "path": "direct_live",
                    "usable_truth": True,
                    "sufficiency_state": "movement_capable",
                    "data_mode": "live",
                    "authority_level": "direct",
                    "observed_at": "2026-04-06T00:00:00+00:00",
                    "provenance_strength": "live_authoritative",
                },
            }
        },
    )
    market = SimpleNamespace(
        points=[
            SimpleNamespace(at="2026-04-05T00:00:00+00:00", value=100.0),
            SimpleNamespace(at="2026-04-06T00:00:00+00:00", value=101.0),
        ],
        evidence=[
            SimpleNamespace(
                facts={
                    "freshness_state": {"freshness_class": "current"},
                    "truth_envelope": {"acquisition_mode": "live", "source_authority": "Polygon"},
                    "provider_execution": {
                        "provider_name": "polygon",
                        "path_used": "direct_live",
                        "live_or_cache": "live",
                        "usable_truth": True,
                        "sufficiency_state": "movement_capable",
                        "data_mode": "live",
                        "authority_level": "direct",
                        "observed_at": "2026-04-06T00:00:00+00:00",
                        "provenance_strength": "live_authoritative",
                    },
                }
            )
        ],
    )
    benchmark_truth_obj = SimpleNamespace(
        evidence=[
            SimpleNamespace(
                facts={
                    "freshness_state": {"freshness_class": "current"},
                    "field_provenance": {
                        "current_value": {
                            "authority_kind": "live_authoritative",
                            "source_family": "quote_latest",
                            "provider": "polygon",
                            "path": "direct_live",
                            "usable_truth": True,
                            "sufficiency_state": "price_present",
                            "data_mode": "live",
                            "authority_level": "direct",
                            "observed_at": "2026-04-06T00:00:00+00:00",
                            "provenance_strength": "live_authoritative",
                        },
                        "ytd_return_pct": {
                            "authority_kind": "derived",
                            "source_family": "ohlcv_history",
                            "provider": "tiingo",
                            "path": "routed_live",
                            "usable_truth": True,
                            "sufficiency_state": "history_capable",
                            "data_mode": "live",
                            "authority_level": "derived",
                            "observed_at": "2026-04-06T00:00:00+00:00",
                            "provenance_strength": "derived_or_proxy",
                        },
                    },
                }
            )
        ]
    )

    block = _market_history_block(
        truth,
        market,
        [{"summary": "cash", "verdict": "cash"}, {"summary": "incumbent", "verdict": "incumbent"}, {"summary": "benchmark", "verdict": "support"}],
        benchmark_truth_obj=benchmark_truth_obj,
    )

    assert "field_provenance" in block
    assert block["field_provenance"]["instrument.price"]["provenance_strength"] == "live_authoritative"
    assert block["field_provenance"]["benchmark.ytd_return_pct"]["provenance_strength"] == "derived_or_proxy"
    assert block["field_provenance"]["instrument.price"]["usable_truth"] is True
    assert block["field_provenance"]["benchmark.ytd_return_pct"]["sufficiency_state"] == "history_capable"


def test_candidate_report_market_history_helper_preserves_field_level_price_sufficiency() -> None:
    from app.v2.surfaces.blueprint.report_contract_builder import _market_history_block

    truth = SimpleNamespace(
        name="IWDP",
        metrics={
            "source_provenance": {
                "price": {
                    "authority_kind": "live_authoritative",
                    "source_family": "quote_latest",
                    "provider": "finnhub",
                    "path": "routed_cache",
                    "live_or_cache": "cache",
                    "usable_truth": True,
                    "sufficiency_state": "price_present",
                    "data_mode": "price_only",
                    "authority_level": "live_authoritative",
                    "observed_at": "2026-04-07T00:00:00+00:00",
                    "provenance_strength": "live_authoritative",
                },
                "change_pct_1d": {
                    "authority_kind": "unavailable",
                    "source_family": "quote_latest",
                    "provider": "finnhub",
                    "path": "routed_cache",
                    "live_or_cache": "cache",
                    "usable_truth": False,
                    "sufficiency_state": "insufficient",
                    "data_mode": "unavailable",
                    "authority_level": "unavailable",
                    "observed_at": "2026-04-07T00:00:00+00:00",
                    "provenance_strength": "degraded",
                },
            }
        },
    )
    market = SimpleNamespace(
        points=[
            SimpleNamespace(at="2026-04-06T00:00:00+00:00", value=100.0),
            SimpleNamespace(at="2026-04-07T00:00:00+00:00", value=101.0),
        ],
        evidence=[
            SimpleNamespace(
                facts={
                    "freshness_state": {"freshness_class": "fresh_full_rebuild"},
                    "truth_envelope": {"acquisition_mode": "cached", "source_authority": "market_price"},
                    "provider_execution": {
                        "provider_name": "finnhub",
                        "path_used": "routed_cache",
                        "live_or_cache": "cache",
                        "usable_truth": False,
                        "sufficiency_state": "insufficient",
                        "data_mode": "unavailable",
                        "authority_level": "unavailable",
                        "observed_at": "2026-04-07T00:00:00+00:00",
                        "provenance_strength": "degraded",
                    },
                }
            )
        ],
    )

    block = _market_history_block(
        truth,
        market,
        [{"summary": "cash", "verdict": "cash"}, {"summary": "incumbent", "verdict": "incumbent"}, {"summary": "benchmark", "verdict": "support"}],
        benchmark_truth_obj=None,
    )

    assert block["field_provenance"]["instrument.price"]["provider_used"] == "finnhub"
    assert block["field_provenance"]["instrument.price"]["usable_truth"] is True
    assert block["field_provenance"]["instrument.price"]["sufficiency_state"] == "price_present"
    assert block["field_provenance"]["instrument.change_pct_1d"]["usable_truth"] is False


def test_runtime_provenance_does_not_promote_source_authority_to_provider_name() -> None:
    from app.v2.surfaces.common import runtime_provenance

    provenance = runtime_provenance(
        source_family="quote_latest",
        provider_execution={},
        truth_envelope={"source_authority": "market_price", "acquisition_mode": "cached"},
    )

    assert provenance["provider_used"] is None


def test_candidate_seed_truth_reads_extra_launch_and_tracking_fields() -> None:
    from app.v2.truth.candidate_quality import _candidate_seed_truth_item

    candidate = {
        "symbol": "IWDP",
        "extra": {
            "launch_date": "2007-11-20",
            "tracking_difference_1y": -0.0003,
        },
    }

    launch = _candidate_seed_truth_item(candidate, "launch_date")
    tracking = _candidate_seed_truth_item(candidate, "tracking_difference_1y")

    assert launch["resolved_value"] == "2007-11-20"
    assert tracking["resolved_value"] == -0.0003


def test_explorer_market_runtime_provenance_uses_cached_quote_provenance_when_market_truth_is_thin() -> None:
    from app.v2.surfaces.blueprint.explorer_contract_builder import _market_runtime_provenance

    market = SimpleNamespace(
        evidence=[
            SimpleNamespace(
                facts={
                    "freshness_state": {"freshness_class": "fresh_full_rebuild"},
                    "truth_envelope": {"source_authority": "market_price", "acquisition_mode": "cached"},
                }
            )
        ]
    )
    fallback = {
        "source_family": "quote_latest",
        "provider_used": "finnhub",
        "path_used": "routed_cache",
        "live_or_cache": "cache",
        "usable_truth": True,
        "sufficiency_state": "price_present",
        "data_mode": "cache",
        "authority_level": "live_authoritative",
        "provenance_strength": "cache_continuity",
    }

    provenance = _market_runtime_provenance(market, fallback_provenance=fallback)

    assert provenance["provider_used"] == "finnhub"
    assert provenance["path_used"] == "routed_cache"
    assert provenance["usable_truth"] is True
    assert provenance["sufficiency_state"] == "price_present"


def test_provider_capability_matrix_matches_real_adapter_scope() -> None:
    from app.services.provider_registry import capability_matrix, provider_supports_family, provider_support_status

    matrix = capability_matrix()["providers"]

    assert provider_supports_family("frankfurter", "fx_reference") is True
    assert provider_supports_family("polygon", "fx") is False
    assert provider_supports_family("tiingo", "reference_meta") is False
    assert provider_support_status("frankfurter", "fx_reference", "USD/SGD") == (True, None)
    assert matrix["twelve_data"]["families"]["fx"]["priority"] == "primary"
    assert matrix["alpha_vantage"]["families"]["ohlcv_history"]["priority"] == "secondary"
    assert matrix["fmp"]["families"]["quote_latest"]["commercial_status"] == "blocked_by_plan"
    assert provider_support_status("eodhd", "quote_latest", "SPY") == (False, "provider_blocked_by_plan")


def test_runtime_truth_rows_include_provenance_strength() -> None:
    from app.v2.sources.runtime_truth import record_runtime_truth, reset_runtime_truth, runtime_truth_rows

    reset_runtime_truth()
    record_runtime_truth(
        source_id="macro",
        source_family="macro",
        field_name="value",
        symbol_or_entity="CPIAUCSL",
        provider_used="fred",
        path_used="fred_live",
        live_or_cache="live",
        usable_truth=True,
        freshness="current",
        insufficiency_reason=None,
        semantic_grade="field_present",
        investor_surface="daily_brief",
        attempt_succeeded=True,
    )

    row = next(item for item in runtime_truth_rows() if item["source_id"] == "macro")

    assert row["last_provenance_strength"] == "live_authoritative"
    assert row["latest_trace"]["provenance_strength"] == "live_authoritative"


def test_effective_family_orders_promote_twelve_data_fx_and_alpha_history() -> None:
    from app.services.provider_registry import providers_for_family

    assert providers_for_family("fx")[:2] == ["twelve_data", "alpha_vantage"]
    assert providers_for_family("ohlcv_history")[:2] == ["tiingo", "alpha_vantage"]
    assert providers_for_family("fx_reference") == ["frankfurter"]


def test_frankfurter_is_registered_only_for_fx_reference_support() -> None:
    from app.services.provider_registry import capability_matrix, providers_for_family

    matrix = capability_matrix()

    assert "frankfurter" in matrix["providers"]
    assert "fx_reference" in matrix["providers"]["frankfurter"]["families"]
    assert providers_for_family("fx_reference") == ["frankfurter"]
    assert "frankfurter" not in providers_for_family("quote_latest")
    assert "frankfurter" not in providers_for_family("benchmark_proxy")
    assert "frankfurter" not in providers_for_family("ohlcv_history")


def test_benchmark_truth_prefers_runtime_capable_proxy_for_report_depth(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.sources.benchmark_truth_adapter as adapter

    monkeypatch.setattr(
        adapter,
        "_benchmark_rows",
        lambda: [
            {
                "benchmark_id": "MSCI_EM_IMI",
                "name": "Emerging markets proxy",
                "proxy_symbol": "EEM",
                "mapped_tickers": ["EIMI"],
            }
        ],
    )
    monkeypatch.setattr(
        adapter,
        "_performance_fields",
        lambda proxy_symbol, surface_name=None: {
            "current_value": 70.14 if proxy_symbol == "IEMG" else 57.11,
            "ytd_return_pct": 0.0 if proxy_symbol == "IEMG" else None,
            "one_year_return_pct": 0.0 if proxy_symbol == "IEMG" else None,
            "history_provider_name": "polygon" if proxy_symbol == "IEMG" else None,
            "history_error": None if proxy_symbol == "IEMG" else "eodhd:provider_error",
            "field_provenance": {
                "current_value": {"source_family": "quote_latest"},
                "ytd_return_pct": {"source_family": "ohlcv_history"},
                "one_year_return_pct": {"source_family": "ohlcv_history"},
            },
        },
    )

    payload = adapter.fetch("MSCI_EM_IMI", surface_name="candidate_report")

    assert payload["proxy_symbol"] == "IEMG"
    assert payload["ytd_return_pct"] == pytest.approx(0.0)
    assert payload["one_year_return_pct"] == pytest.approx(0.0)


def test_benchmark_warm_history_cache_retries_until_history_is_usable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.sources.benchmark_truth_adapter as adapter

    monkeypatch.setattr(
        adapter,
        "_benchmark_rows",
        lambda: [
            {
                "benchmark_id": "MSCI_EM_IMI",
                "name": "Emerging markets proxy",
                "proxy_symbol": "EEM",
                "mapped_tickers": ["EIMI"],
            }
        ],
    )
    calls = {"IEMG": 0}

    def _history_fields(proxy_symbol: str, *, surface_name: str | None = None) -> dict[str, object]:
        if proxy_symbol == "IEMG":
            calls["IEMG"] += 1
            if calls["IEMG"] == 1:
                return {
                    "ytd_return_pct": None,
                    "one_year_return_pct": None,
                    "history_provider_name": None,
                    "history_error": "eodhd:provider_error",
                }
            return {
                "ytd_return_pct": 0.0,
                "one_year_return_pct": 0.0,
                "history_provider_name": "polygon",
                "history_error": None,
            }
        return {
            "ytd_return_pct": None,
            "one_year_return_pct": None,
            "history_provider_name": None,
            "history_error": "eodhd:provider_error",
        }

    monkeypatch.setattr(adapter, "_history_fields", _history_fields)

    warmed = adapter.warm_history_cache(surface_name="candidate_report", benchmark_ids=["MSCI_EM_IMI"])

    assert warmed == [
        {
            "benchmark_id": "MSCI_EM_IMI",
            "proxy_symbol": "IEMG",
            "usable_history": True,
            "history_error": None,
        }
    ]
    assert calls["IEMG"] == 2
