from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import patch

from app.services.blueprint_candidate_truth import (
    compute_candidate_completeness,
    ensure_candidate_truth_tables,
    list_required_fields,
    reconcile_field_observations,
    persist_sleeve_no_pick_reason,
    resolve_candidate_field_truth,
    seed_required_field_matrix,
    upsert_field_observation,
)
from app.services.blueprint_candidate_registry import (
    LIVE_OBJECT_TYPE,
    _reconcile_preferred_observations,
    _structural_primary_trading_currency,
    ensure_candidate_registry_tables,
    list_active_candidate_registry,
    refresh_registry_candidate_truth,
    seed_default_candidate_registry,
)
from app.services.ingest_etf_data import (
    _ensure_etf_tables,
    fetch_configured_market_data,
    fetch_ishares_holdings,
    get_preferred_market_exchange,
    refresh_etf_data,
    sync_configured_etf_data_sources,
)
from app.services.portfolio_blueprint import build_portfolio_blueprint_payload
from app.services.provider_refresh import _extract_provider_candidate_fields
from app.services.provider_refresh import _provider_field_allowed
from app.services.provider_cache import put_provider_snapshot


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_candidate_truth_tables(conn)
    seed_required_field_matrix(conn)
    _ensure_etf_tables(conn)
    ensure_candidate_registry_tables(conn)
    seed_default_candidate_registry(conn)
    return conn


def _default_value(field_name: str) -> Any:
    if field_name in {"ucits_status", "max_loss_known"}:
        return True
    if field_name in {"margin_required", "short_options"}:
        return False
    if field_name in {
        "expense_ratio",
        "aum",
        "tracking_difference_1y",
        "tracking_difference_3y",
        "tracking_difference_5y",
        "tracking_error_1y",
        "us_weight",
        "em_weight",
        "top_10_concentration",
        "sector_concentration_proxy",
        "effective_duration",
        "average_maturity",
        "yield_proxy",
        "interest_rate_sensitivity_proxy",
    }:
        return 1.23
    return f"value_for_{field_name}"


def _populate_required_fields(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
    sleeve_key: str,
    instrument_type: str,
    provenance_level: str = "verified_official",
    skip_fields: set[str] | None = None,
) -> None:
    skip = set(skip_fields or set())
    for row in list_required_fields(conn, sleeve_key):
        rule = str(row["applicability_rule"])
        if rule == "etf_like" and instrument_type not in {"etf_ucits", "etf_us"}:
            continue
        field_name = str(row["field_name"])
        if field_name in skip:
            continue
        upsert_field_observation(
            conn,
            candidate_symbol=candidate_symbol,
            sleeve_key=sleeve_key,
            field_name=field_name,
            value=_default_value(field_name),
            source_name="test_source",
            observed_at=datetime.now(UTC).date().isoformat(),
            provenance_level=provenance_level,
            confidence_label="high",
            parser_method="test_fixture",
        )
    resolve_candidate_field_truth(conn, candidate_symbol=candidate_symbol, sleeve_key=sleeve_key)


def test_verified_official_beats_proxy_and_equal_provenance_uses_fresher_value() -> None:
    conn = _conn()
    try:
        upsert_field_observation(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            field_name="primary_trading_currency",
            value="SGD",
            source_name="proxy_market",
            observed_at=(datetime.now(UTC) - timedelta(days=1)).date().isoformat(),
            provenance_level="proxy",
            confidence_label="medium",
        )
        upsert_field_observation(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            field_name="primary_trading_currency",
            value="USD",
            source_name="issuer_factsheet",
            observed_at=(datetime.now(UTC) - timedelta(days=20)).date().isoformat(),
            provenance_level="verified_official",
            confidence_label="high",
        )
        upsert_field_observation(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            field_name="benchmark_confidence",
            value="medium",
            source_name="benchmark_registry",
            observed_at=(datetime.now(UTC) - timedelta(days=10)).date().isoformat(),
            provenance_level="verified_nonissuer",
            confidence_label="medium",
        )
        upsert_field_observation(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            field_name="benchmark_confidence",
            value="high",
            source_name="benchmark_registry",
            observed_at=datetime.now(UTC).date().isoformat(),
            provenance_level="verified_nonissuer",
            confidence_label="high",
        )
        current = resolve_candidate_field_truth(conn, candidate_symbol="VWRA", sleeve_key="global_equity_core")
        assert current["primary_trading_currency"]["resolved_value"] == "USD"
        assert current["benchmark_confidence"]["resolved_value"] == "high"
    finally:
        conn.close()


def test_reconcile_field_observations_demotes_weaker_conflicting_domicile() -> None:
    conn = _conn()
    try:
        upsert_field_observation(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            field_name="domicile",
            value="IRELAND",
            source_name="issuer_doc_registry",
            observed_at=(datetime.now(UTC) - timedelta(days=5)).date().isoformat(),
            provenance_level="verified_nonissuer",
            confidence_label="high",
        )
        upsert_field_observation(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            field_name="domicile",
            value="UNITED STATES",
            source_name="twelve_data",
            observed_at=datetime.now(UTC).date().isoformat(),
            provenance_level="verified_nonissuer",
            confidence_label="medium",
        )
        reconcile_field_observations(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            field_name="domicile",
            trusted_value="IRELAND",
            trusted_source_names={"issuer_doc_registry"},
            reason="issuer_registry_preferred",
        )
        current = resolve_candidate_field_truth(conn, candidate_symbol="VWRA", sleeve_key="global_equity_core")
        assert current["domicile"]["resolved_value"] == "IRELAND"
    finally:
        conn.close()


def test_reconcile_preferred_observations_uses_registry_domicile_when_doc_parse_is_garbage() -> None:
    conn = _conn()
    try:
        upsert_field_observation(
            conn,
            candidate_symbol="VFEA",
            sleeve_key="emerging_markets",
            field_name="domicile",
            value="Where Such An Offer Orsolicitation Is Against The Law",
            source_name="issuer_doc_parser",
            observed_at=datetime.now(UTC).date().isoformat(),
            provenance_level="verified_official",
            confidence_label="high",
        )
        upsert_field_observation(
            conn,
            candidate_symbol="VFEA",
            sleeve_key="emerging_markets",
            field_name="domicile",
            value="Ireland",
            source_name="issuer_doc_registry",
            observed_at=datetime.now(UTC).date().isoformat(),
            provenance_level="verified_nonissuer",
            confidence_label="high",
        )
        _reconcile_preferred_observations(
            conn,
            candidate_symbol="VFEA",
            sleeve_key="emerging_markets",
            doc_extracted={"domicile": "Where Such An Offer Orsolicitation Is Against The Law"},
            doc_registry_expected_isin="",
            doc_registry_domicile="Ireland",
            doc_registry_issuer=None,
            candidate_registry_currency=None,
            benchmark_assignment={},
            canonical_benchmark_key=None,
            cached_provider_aum=None,
            factsheet_summary=None,
            recovery_run_id="test_run",
        )
        current = resolve_candidate_field_truth(conn, candidate_symbol="VFEA", sleeve_key="emerging_markets")
        assert current["domicile"]["resolved_value"] == "Ireland"
    finally:
        conn.close()


def test_reconcile_preferred_observations_prefers_registry_issuer_and_canonical_benchmark_without_assignment() -> None:
    conn = _conn()
    try:
        upsert_field_observation(
            conn,
            candidate_symbol="SGOV",
            sleeve_key="cash_bills",
            field_name="issuer",
            value="will not be able to make principal and interest payments.",
            source_name="issuer_doc_parser",
            observed_at="2026-04-20",
            provenance_level="verified_official",
            confidence_label="high",
        )
        upsert_field_observation(
            conn,
            candidate_symbol="SGOV",
            sleeve_key="cash_bills",
            field_name="benchmark_name",
            value="S&P 500",
            source_name="issuer_doc_parser",
            observed_at="2026-04-20",
            provenance_level="verified_official",
            confidence_label="high",
        )
        _reconcile_preferred_observations(
            conn,
            candidate_symbol="SGOV",
            sleeve_key="cash_bills",
            doc_extracted={"issuer": "will not be able to make principal and interest payments.", "benchmark_name": "S&P 500"},
            doc_registry_expected_isin="",
            doc_registry_domicile="",
            doc_registry_issuer="BlackRock",
            candidate_registry_currency="USD",
            benchmark_assignment={"benchmark_key": "SHORT_TBILL", "benchmark_label": "0-1Y Treasury bills proxy"},
            canonical_benchmark_key="SHORT_TBILL",
            cached_provider_aum=None,
            factsheet_summary=None,
            recovery_run_id="test_run",
        )
        current = resolve_candidate_field_truth(conn, candidate_symbol="SGOV", sleeve_key="cash_bills")
        assert current["issuer"]["resolved_value"] == "BlackRock"
        assert current["benchmark_name"]["resolved_value"] == "0-1Y Treasury bills proxy"
    finally:
        conn.close()


def test_reconcile_preferred_observations_demotes_conflicting_currency_when_doc_currency_exists() -> None:
    conn = _conn()
    try:
        upsert_field_observation(
            conn,
            candidate_symbol="IWDA",
            sleeve_key="global_equity_core",
            field_name="primary_trading_currency",
            value="USD",
            source_name="issuer_doc_parser",
            observed_at="2026-01-31",
            provenance_level="verified_official",
            confidence_label="high",
        )
        td_obs_id = upsert_field_observation(
            conn,
            candidate_symbol="IWDA",
            sleeve_key="global_equity_core",
            field_name="primary_trading_currency",
            value="EUR",
            source_name="Twelve Data",
            observed_at="2026-04-07T07:09:24.366621+00:00",
            provenance_level="verified_nonissuer",
            confidence_label="medium",
        )
        _reconcile_preferred_observations(
            conn,
            candidate_symbol="IWDA",
            sleeve_key="global_equity_core",
            doc_extracted={"primary_trading_currency": "USD"},
            doc_registry_expected_isin="",
            doc_registry_domicile="",
            doc_registry_issuer=None,
            candidate_registry_currency=None,
            benchmark_assignment={},
            canonical_benchmark_key=None,
            cached_provider_aum=None,
            factsheet_summary=None,
            recovery_run_id="test_run",
        )
        current = resolve_candidate_field_truth(conn, candidate_symbol="IWDA", sleeve_key="global_equity_core")
        assert current["primary_trading_currency"]["resolved_value"] == "USD"
        annotation = conn.execute(
            "select override_annotation_json from candidate_field_observations where observation_id = ?",
            (td_obs_id,),
        ).fetchone()
        assert "reconciled_state" in str(annotation[0])
    finally:
        conn.close()


def test_reconcile_preferred_observations_prefers_fresher_factsheet_aum_over_doc_aum() -> None:
    conn = _conn()
    try:
        upsert_field_observation(
            conn,
            candidate_symbol="IWDP",
            sleeve_key="real_assets",
            field_name="aum",
            value=111.0,
            source_name="issuer_doc_parser",
            observed_at="2026-01-31",
            provenance_level="verified_official",
            confidence_label="high",
        )
        _reconcile_preferred_observations(
            conn,
            candidate_symbol="IWDP",
            sleeve_key="real_assets",
            doc_extracted={"aum_usd": 111.0},
            doc_registry_expected_isin="",
            doc_registry_domicile="",
            doc_registry_issuer=None,
            candidate_registry_currency=None,
            benchmark_assignment={},
            canonical_benchmark_key=None,
            cached_provider_aum={"value": 222.0, "source_name": "fmp", "freshness_state": "current"},
            factsheet_summary={"latest_aum_usd": 333.0},
            recovery_run_id="test_run",
        )
        current = resolve_candidate_field_truth(conn, candidate_symbol="IWDP", sleeve_key="real_assets")
        assert current["aum"]["resolved_value"] == 333.0
    finally:
        conn.close()


def test_reconcile_preferred_observations_prefers_candidate_registry_currency_when_doc_currency_conflicts() -> None:
    conn = _conn()
    try:
        upsert_field_observation(
            conn,
            candidate_symbol="IWDP",
            sleeve_key="real_assets",
            field_name="primary_trading_currency",
            value="USD",
            source_name="issuer_doc_parser",
            observed_at="2026-01-31",
            provenance_level="verified_official",
            confidence_label="high",
        )
        upsert_field_observation(
            conn,
            candidate_symbol="IWDP",
            sleeve_key="real_assets",
            field_name="primary_trading_currency",
            value="GBP",
            source_name="candidate_registry",
            observed_at="2026-01-31",
            provenance_level="seeded_fallback",
            confidence_label="medium",
        )
        _reconcile_preferred_observations(
            conn,
            candidate_symbol="IWDP",
            sleeve_key="real_assets",
            doc_extracted={"primary_trading_currency": "USD"},
            doc_registry_expected_isin="",
            doc_registry_domicile="",
            doc_registry_issuer=None,
            candidate_registry_currency="GBP",
            benchmark_assignment={},
            canonical_benchmark_key=None,
            cached_provider_aum=None,
            factsheet_summary=None,
            recovery_run_id="test_run",
        )
        current = resolve_candidate_field_truth(conn, candidate_symbol="IWDP", sleeve_key="real_assets")
        assert current["primary_trading_currency"]["resolved_value"] == "GBP"
        assert current["primary_trading_currency"]["source_name"] == "candidate_registry"
    finally:
        conn.close()


def test_structural_primary_trading_currency_prefers_selected_line_truth_over_quote_currency() -> None:
    selected = _structural_primary_trading_currency(
        doc_extracted={},
        official_citation_extracted={"primary_trading_currency": "USD"},
        extra={"primary_trading_currency": "USD"},
        yfinance_quote={"currency": "GBP"},
    )
    assert selected == "USD"


def test_resolve_candidate_field_truth_prefers_candidate_registry_over_payload_for_aum() -> None:
    conn = _conn()
    try:
        upsert_field_observation(
            conn,
            candidate_symbol="VWRL",
            sleeve_key="global_equity_core",
            field_name="aum",
            value=59490000000.0,
            source_name="candidate_payload",
            observed_at="2026-04-07",
            provenance_level="inferred",
            confidence_label="medium",
        )
        upsert_field_observation(
            conn,
            candidate_symbol="VWRL",
            sleeve_key="global_equity_core",
            field_name="aum",
            value=59490000000.0,
            source_name="candidate_registry",
            observed_at="2026-03-31",
            provenance_level="seeded_fallback",
            confidence_label="medium",
        )
        current = resolve_candidate_field_truth(conn, candidate_symbol="VWRL", sleeve_key="global_equity_core")
        assert current["aum"]["resolved_value"] == 59490000000.0
        assert current["aum"]["source_name"] == "candidate_registry"
    finally:
        conn.close()


def test_core_passive_proxy_critical_fields_do_not_reach_recommendation_ready() -> None:
    conn = _conn()
    try:
        _populate_required_fields(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            instrument_type="etf_ucits",
            provenance_level="verified_official",
            skip_fields={"benchmark_confidence"},
        )
        upsert_field_observation(
            conn,
            candidate_symbol="VWRA",
            sleeve_key="global_equity_core",
            field_name="benchmark_confidence",
            value="medium",
            source_name="proxy_registry",
            observed_at=datetime.now(UTC).date().isoformat(),
            provenance_level="proxy",
            confidence_label="medium",
        )
        resolve_candidate_field_truth(conn, candidate_symbol="VWRA", sleeve_key="global_equity_core")
        snapshot = compute_candidate_completeness(
            conn,
            candidate={
                "symbol": "VWRA",
                "sleeve_key": "global_equity_core",
                "instrument_type": "etf_ucits",
                "eligibility": {"eligibility_state": "eligible"},
            },
        )
        assert snapshot["proxy_only_count"] >= 1
        assert snapshot["readiness_level"] == "shortlist_ready"
    finally:
        conn.close()


def test_convex_candidate_is_not_penalized_for_non_applicable_etf_fields() -> None:
    conn = _conn()
    try:
        _populate_required_fields(
            conn,
            candidate_symbol="SPX_LONG_PUT",
            sleeve_key="convex",
            instrument_type="long_put_overlay_strategy",
            provenance_level="verified_nonissuer",
        )
        snapshot = compute_candidate_completeness(
            conn,
            candidate={
                "symbol": "SPX_LONG_PUT",
                "sleeve_key": "convex",
                "instrument_type": "long_put_overlay_strategy",
                "eligibility": {"eligibility_state": "eligible"},
            },
        )
        assert snapshot["required_fields_total"] >= 5
        assert snapshot["source_gap_missing_count"] == 0
        assert snapshot["readiness_level"] == "recommendation_ready"
    finally:
        conn.close()


def test_no_pick_reason_becomes_specific_for_benchmark_blockers() -> None:
    conn = _conn()
    try:
        reason = persist_sleeve_no_pick_reason(
            conn,
            snapshot_id="snapshot_1",
            sleeve_key="global_equity_core",
            candidates=[
                {
                    "symbol": "VWRA",
                    "data_completeness": {"readiness_level": "review_ready"},
                    "decision_readiness": {
                        "top_blockers": [
                            {"label": "Benchmark confidence missing", "severity": "critical"},
                            {"label": "Benchmark validation missing", "severity": "critical"},
                        ]
                    },
                    "investment_quality": {"composite_score": 75.0},
                }
            ],
        )
        assert reason["reason_code"] == "benchmark_readiness"
        assert "benchmark readiness" in reason["reason_text"].lower()
    finally:
        conn.close()


def test_preferred_market_exchange_uses_configured_listing() -> None:
    conn = _conn()
    try:
        assert get_preferred_market_exchange("VWRA", conn) == "LSE"
    finally:
        conn.close()


def test_fetch_configured_market_data_persists_listing_aware_market_row() -> None:
    conn = _conn()
    try:
        with patch("app.services.ingest_etf_data.fetch_yahoo_market_data") as mocked:
            mocked.return_value = {
                "status": "success",
                "ticker": "VWRA",
                "source": "yahoo_finance",
                "source_url": "https://finance.yahoo.com/quote/VWRA.L",
                "retrieved_at": "2026-03-08T00:00:00+00:00",
                "market_data": {
                    "last_price": 100.0,
                    "bid_price": 99.9,
                    "ask_price": 100.1,
                    "bid_ask_spread_abs": 0.2,
                    "bid_ask_spread_bps": 20.0,
                    "volume_day": 1000.0,
                    "volume_avg_30d": 50000.0,
                    "volume_avg_10d": 45000.0,
                    "liquidity_score": 0.44,
                },
            }
            result = fetch_configured_market_data("VWRA", conn)
            assert result["status"] == "success"
            row = conn.execute(
                "SELECT exchange, bid_ask_spread_bps, volume_30d_avg FROM etf_market_data WHERE etf_symbol = ? LIMIT 1",
                ("VWRA",),
            ).fetchone()
            assert row is not None
            assert str(row["exchange"]) == "LSE"
            assert float(row["bid_ask_spread_bps"]) == 20.0
            assert float(row["volume_30d_avg"]) == 50000.0
    finally:
        conn.close()


def test_registry_refresh_uses_cached_provider_aum_when_doc_aum_missing() -> None:
    conn = _conn()
    try:
        put_provider_snapshot(
            conn,
            provider_name="fmp",
            endpoint_family="fundamentals",
            cache_key="HMCH",
            surface_name="blueprint",
            payload={
                "identifier": "HMCH",
                "observed_at": "2026-04-18T00:00:00+00:00",
                "value": {"aum": 987654321.0},
            },
            freshness_state="current",
            confidence_tier="secondary",
            source_ref="test",
            ttl_seconds=86400,
            cache_status="hit",
        )
        with patch("app.services.blueprint_candidate_registry.get_etf_factsheet_history_summary", return_value=None), patch(
            "app.services.blueprint_candidate_registry.fetch_candidate_docs",
            return_value={"factsheet": None, "extracted": {}},
        ):
            result = refresh_registry_candidate_truth(conn, symbol="HMCH", recovery_run_id="test_run")
        assert result["updated"] is True
        current = resolve_candidate_field_truth(conn, candidate_symbol="HMCH", sleeve_key="china_satellite")
        assert float(current["aum"]["resolved_value"]) == 987654321.0
        assert current["aum"]["source_name"] == "fmp"
    finally:
        conn.close()


def test_fetch_ishares_holdings_accepts_non_utf8_csv() -> None:
    conn = _conn()
    try:
        sync_configured_etf_data_sources(conn)

        csv_payload = (
            'as of,"03/11/2026"\r\n'
            'Ticker,Name,Weight (%),Shares,Market Value,Sector,Location,Asset Class\r\n'
            'SGOV,"U.S. Treasury Bill – 3M",10.5,100,1000,Government,United States,Fixed Income\r\n'
        ).encode("cp1252")

        class _FakeResponse:
            def __init__(self, content: bytes) -> None:
                self.content = content

            def raise_for_status(self) -> None:
                return None

        class _FakeClient:
            def __enter__(self) -> "_FakeClient":
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                return None

            def get(self, url: str, headers: dict[str, str] | None = None) -> _FakeResponse:
                return _FakeResponse(csv_payload)

        with patch("app.services.ingest_etf_data.httpx.Client", return_value=_FakeClient()):
            result = fetch_ishares_holdings("SGOV", conn)

        assert result["status"] == "success"
        row = conn.execute(
            "SELECT security_name, country FROM etf_holdings WHERE etf_symbol = ? LIMIT 1",
            ("SGOV",),
        ).fetchone()
        assert row is not None
        assert "Treasury Bill" in str(row["security_name"])
        assert str(row["country"]) == "United States"
    finally:
        conn.close()


def test_fetch_ishares_holdings_accepts_us_header_without_ticker_column() -> None:
    conn = _conn()
    try:
        sync_configured_etf_data_sources(conn)

        csv_payload = (
            'iShares 0-3 Month Treasury Bond ETF\r\n'
            'Fund Holdings as of,"03/10/2026"\r\n'
            'Name,Sector,Asset Class,Market Value,Weight (%),Notional Value,Par Value,CUSIP,ISIN,SEDOL,Price,Location,Exchange,Currency,Duration,YTM (%),FX Rate,Maturity\r\n'
            '"TREASURY BILL","Cash and/or Derivatives","Cash","8247520695.59","10.58","8247520695.59","8293466500.00","912797TL1","US912797TL15","BSD5S14","99.45","United States","-","USD","0.15","3.70","1.00","May 05, 2026"\r\n'
        ).encode("utf-8")

        class _FakeResponse:
            def __init__(self, content: bytes) -> None:
                self.content = content

            def raise_for_status(self) -> None:
                return None

        class _FakeClient:
            def __enter__(self) -> "_FakeClient":
                return self

            def __exit__(self, exc_type, exc, tb) -> None:
                return None

            def get(self, url: str, headers: dict[str, str] | None = None) -> _FakeResponse:
                return _FakeResponse(csv_payload)

        with patch("app.services.ingest_etf_data.httpx.Client", return_value=_FakeClient()):
            result = fetch_ishares_holdings("SGOV", conn)

        assert result["status"] == "success"
        row = conn.execute(
            "SELECT security_name, security_ticker, asset_class, country FROM etf_holdings WHERE etf_symbol = ? LIMIT 1",
            ("SGOV",),
        ).fetchone()
        assert row is not None
        assert str(row["security_name"]) == "TREASURY BILL"
        assert row["security_ticker"] in {None, ""}
        assert str(row["asset_class"]) == "Cash"
        assert str(row["country"]) == "United States"
    finally:
        conn.close()


def test_refresh_etf_data_routes_ishares_pdf_extract_to_document_parser() -> None:
    conn = _conn()
    try:
        sync_configured_etf_data_sources(conn)
        conn.close()
        with patch("app.services.ingest_etf_data.fetch_document_factsheet_metrics") as fetch_doc:
            with patch("app.services.ingest_etf_data.fetch_ishares_factsheet_metrics") as fetch_api:
                with patch("app.services.ingest_etf_data.fetch_configured_market_data") as fetch_market:
                    with patch("app.services.ingest_etf_data.fetch_ishares_holdings") as fetch_holdings:
                        fetch_doc.return_value = {"status": "success"}
                        fetch_api.return_value = {"status": "success"}
                        fetch_market.return_value = {"status": "success"}
                        fetch_holdings.return_value = {"status": "success"}
                        result = refresh_etf_data("SGOV")

        assert result["factsheet"]["status"] == "success"
        assert fetch_doc.called
        assert not fetch_api.called
    finally:
        try:
            conn.close()
        except Exception:
            pass


def test_core_source_expansion_reduces_manual_static_and_aum_blockers() -> None:
    payload = build_portfolio_blueprint_payload()
    by_symbol = {}
    for sleeve in payload["sleeves"]:
        for candidate in sleeve["candidates"]:
            by_symbol[candidate["symbol"]] = candidate

    aum_blocked = []
    for symbol in ("SSAC", "VWRL", "VEVE", "EIMI", "VFEA"):
        candidate = by_symbol[symbol]
        readiness = dict(candidate.get("decision_readiness") or {})
        assert candidate.get("source_state") == "source_validated"
        assert readiness.get("primary_blocker") != "candidate source state is manual static for a core sleeve"
        if readiness.get("primary_blocker") == "Aum missing":
            aum_blocked.append(symbol)
    assert len(aum_blocked) <= 1


def test_refresh_registry_candidate_truth_uses_market_data_from_configured_listing() -> None:
    conn = _conn()
    try:
        conn.execute(
            """
            INSERT INTO etf_market_data (
                market_data_id, etf_symbol, exchange, asof_date, asof_time, last_price,
                bid_price, ask_price, bid_ask_spread_abs, bid_ask_spread_bps, volume_day,
                volume_30d_avg, volume_90d_avg, nav, premium_discount_pct, retrieved_at,
                source_url, source_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "mkt_vwra_test",
                "VWRA",
                "LSE",
                "2026-03-08",
                "12:00:00",
                100.0,
                99.9,
                100.1,
                0.2,
                20.0,
                1000.0,
                50000.0,
                48000.0,
                None,
                None,
                "2026-03-08T12:00:00+00:00",
                "https://finance.yahoo.com/quote/VWRA.L",
                "market_vwra_lse",
            ),
        )
        conn.commit()
        truth = refresh_registry_candidate_truth(conn, symbol="VWRA")
        assert truth["market_data_asof"] == "2026-03-08"
        current = conn.execute(
            """
            SELECT resolved_value_json
            FROM candidate_field_current
            WHERE candidate_symbol = ? AND sleeve_key = ? AND field_name = ?
            """,
            ("VWRA", "global_equity_core", "primary_listing_exchange"),
        ).fetchone()
        assert current is not None
        assert any(token in str(current["resolved_value_json"]) for token in ("LSE", "London Stock Exchange"))
    finally:
        conn.close()


def test_vwra_payload_exposes_materially_more_field_truth() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    field_truth = dict(candidate.get("field_truth") or {})

    assert field_truth
    assert field_truth["benchmark_key"]["resolved_value"]
    assert field_truth["primary_trading_currency"]["resolved_value"] == "USD"
    assert field_truth["us_weight"]["resolved_value"] is not None
    assert field_truth["top_10_concentration"]["resolved_value"] is not None
    assert field_truth["holdings_count"]["resolved_value"] is not None
    assert field_truth["withholding_tax_posture"]["resolved_value"] is not None
    completeness = dict(candidate.get("data_completeness") or {})
    assert completeness["required_fields_complete_count"] >= 12
    assert completeness["readiness_level"] in {"review_ready", "shortlist_ready", "recommendation_ready"}


def test_ig_bond_source_expansion_populates_duration_truth() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "ig_bonds")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "AGGU")
    field_truth = dict(candidate.get("field_truth") or {})

    assert field_truth["effective_duration"]["missingness_reason"] == "populated"
    assert field_truth["effective_duration"]["resolved_value"] is not None


def test_a35_average_maturity_not_disclosed_is_treated_as_not_applicable() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "ig_bonds")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "A35")
    completeness = dict(candidate.get("data_completeness") or {})
    by_key = {entry["key"]: entry for entry in completeness.get("requirements", [])}

    assert by_key["average_maturity"]["status"] == "not_applicable"
    assert "Average maturity" not in list(completeness.get("dominant_missing_categories") or [])


def test_seed_registry_upserts_live_cash_candidates_into_existing_db() -> None:
    conn = _conn()
    try:
        conn.execute("DELETE FROM blueprint_sleeve_candidate_memberships WHERE symbol IN (?, ?)", ("SGOV", "BILS"))
        conn.execute("DELETE FROM blueprint_canonical_instruments WHERE symbol IN (?, ?)", ("SGOV", "BILS"))
        conn.commit()

        seeded = seed_default_candidate_registry(conn)
        assert seeded > 0

        rows = list_active_candidate_registry(conn)
        cash_symbols = {row["symbol"]: row for row in rows if row["sleeve_key"] == "cash_bills"}
        assert "SGOV" in cash_symbols
        assert "BILS" in cash_symbols
        assert cash_symbols["SGOV"]["object_type"] == LIVE_OBJECT_TYPE
        assert cash_symbols["BILS"]["object_type"] == LIVE_OBJECT_TYPE
        assert cash_symbols["SGOV"]["source_links"]
        assert cash_symbols["BILS"]["source_links"]
    finally:
        conn.close()


def test_source_config_expands_holdings_and_factsheet_coverage_for_priority_blueprint_symbols() -> None:
    from app.services.ingest_etf_data import get_etf_source_config, sync_configured_etf_data_sources

    conn = _conn()
    try:
        sync_configured_etf_data_sources(conn)
        vwra_url = conn.execute(
            "SELECT source_url_template FROM etf_data_sources WHERE etf_symbol = 'VWRA' AND data_type = 'holdings' LIMIT 1"
        ).fetchone()
        assert vwra_url is not None
        assert "api.vanguard.com" in str(vwra_url["source_url_template"])

        for symbol in ("SSAC", "EIMI", "AGGU", "IB01"):
            config = get_etf_source_config(symbol)
            assert config is not None
            data_sources = dict(config.get("data_sources") or {})
            assert "holdings" in data_sources
            assert "factsheet" in data_sources
    finally:
        conn.close()


def test_candidate_field_truth_exposes_provenance_and_completeness_metadata() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "SSAC")
    field = dict(candidate.get("field_truth", {}).get("top_10_concentration") or {})

    assert field["source_type"] in {"issuer_holdings_primary", "issuer_factsheet_secondary", "internal_fallback"}
    assert field["evidence_class"]
    assert field["as_of"]
    assert field["completeness_state"] in {"complete", "weak_or_partial", "incomplete", "unavailable", "not_applicable"}


def test_runtime_truth_exposes_implementation_proofs_for_core_candidate() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    field_truth = dict(candidate.get("field_truth") or {})

    share_class = dict(field_truth.get("share_class_proven") or {})
    benchmark_proof = dict(field_truth.get("benchmark_assignment_proof") or {})

    assert share_class["resolved_value"] is True
    assert share_class["source_type"] == "issuer_factsheet_secondary"
    assert share_class["missingness_reason"] == "populated"
    assert benchmark_proof["resolved_value"]
    assert benchmark_proof["missingness_reason"] == "populated"
    assert "benchmark" in str(benchmark_proof["resolved_value"]).lower()


def test_runtime_field_truth_includes_volume_30d_avg_when_market_history_exists() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    volume_field = dict(candidate.get("field_truth", {}).get("volume_30d_avg") or {})

    assert volume_field["missingness_reason"] == "populated"
    assert volume_field["resolved_value"] is not None
    assert candidate.get("volume_30d_avg") is not None


def test_source_gap_diagnostics_are_classified_as_evidence_or_ingest_gaps() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "cash_bills")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "BIL")
    memo = dict(candidate.get("usability_memo") or {})
    top_blockers = list(dict(candidate.get("decision_readiness") or {}).get("top_blockers") or [])

    assert any("holdings-source coverage" in str(item).lower() for item in list(memo.get("confidence_reducers") or []))
    assert any(str(item.get("source") or "") == "missing_but_fetchable" for item in top_blockers)


def test_family_completeness_is_emitted_for_each_evidence_family() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "global_equity_core")
    candidate = next(item for item in sleeve["candidates"] if item["symbol"] == "VWRA")
    families = dict(dict(candidate.get("data_completeness") or {}).get("family_completeness") or {})

    assert families == {
        "holdings_exposure_completeness": families["holdings_exposure_completeness"],
        "implementation_truth_completeness": families["implementation_truth_completeness"],
        "benchmark_history_completeness": families["benchmark_history_completeness"],
        "performance_metric_completeness": families["performance_metric_completeness"],
        "liquidity_evidence_completeness": families["liquidity_evidence_completeness"],
        "sleeve_specific_support_completeness": families["sleeve_specific_support_completeness"],
    }
    assert all(value in {"complete", "partial", "weak", "missing", "not_applicable"} for value in families.values())


def test_cash_sleeve_fields_use_cash_specific_support_truth() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = next(item for item in payload["sleeves"] if item["sleeve_key"] == "cash_bills")
    ib01 = next(item for item in sleeve["candidates"] if item["symbol"] == "IB01")
    sgov = next(item for item in sleeve["candidates"] if item["symbol"] == "SGOV")

    ib01_truth = dict(ib01.get("field_truth") or {})
    sgov_truth = dict(sgov.get("field_truth") or {})

    assert dict(ib01_truth.get("yield_proxy") or {}).get("missingness_reason") == "populated"
    assert dict(ib01_truth.get("weighted_average_maturity") or {}).get("missingness_reason") == "populated"
    assert dict(ib01_truth.get("portfolio_quality_summary") or {}).get("missingness_reason") == "populated"
    assert dict(sgov_truth.get("share_class_proven") or {}).get("missingness_reason") == "populated"
    assert dict(sgov_truth.get("underlying_currency_exposure") or {}).get("resolved_value") == "USD"
    assert dict(dict(sgov.get("data_completeness") or {}).get("family_completeness") or {}).get("sleeve_specific_support_completeness") in {
        "partial",
        "complete",
    }


def test_generic_provider_fundamentals_do_not_treat_market_cap_as_etf_aum() -> None:
    fields = _extract_provider_candidate_fields(
        "fundamentals",
        {"value": {"marketCap": 1234567890.0, "mktCap": 1234567890.0, "lastDiv": 1.2}},
    )
    assert "aum" not in fields
    assert fields["yield_proxy"] == 1.2


def test_etf_specific_reference_meta_fallback_extracts_exposure_as_verified_nonissuer_candidate_fields() -> None:
    fields = _extract_provider_candidate_fields(
        "reference_meta",
        {
            "value": {
                "name": "ETF Example",
                "exchange": "NYSEARCA",
                "currency": "USD",
                "ETF_Data": {
                    "TotalAssets": 100000000.0,
                    "Holdings_Count": 120,
                    "Country_Weights": {"United States": 60.0, "India": 5.0, "Japan": 10.0},
                    "Sector_Weights": {"Information Technology": 28.0, "Financials": 12.0},
                    "Top_10_Holdings": [{"Weight": 4.0}, {"Weight": 3.0}, {"Weight": 2.0}],
                },
            }
        },
    )
    assert fields["aum"] == 100000000.0
    assert fields["holdings_count"] == 120
    assert fields["us_weight"] == 60.0
    assert fields["em_weight"] == 5.0
    assert fields["sector_concentration_proxy"] == 28.0
    assert fields["top_10_concentration"] == 9.0


def test_non_etf_reference_meta_payload_does_not_backfill_holdings_truth() -> None:
    fields = _extract_provider_candidate_fields(
        "reference_meta",
        {
            "value": {
                "name": "Ordinary Company",
                "exchange": "NYSE",
                "currency": "USD",
                "marketCap": 500000000.0,
                "sector": "Technology",
            }
        },
    )
    assert "holdings_count" not in fields
    assert "us_weight" not in fields
    assert "top_10_concentration" not in fields


def test_refresh_registry_candidate_truth_can_activate_market_series() -> None:
    conn = _conn()
    calls: list[tuple[str, bool]] = []

    def _fake_refresh_candidate_series(connection, *, candidate_id: str, stale_only: bool = True):
        calls.append((candidate_id, stale_only))
        return {"candidate_id": candidate_id}

    with (
        patch("app.v2.blueprint_market.series_refresh_service.refresh_candidate_series", _fake_refresh_candidate_series),
        patch("app.services.blueprint_candidate_registry.fetch_candidate_docs", return_value={"status": "failed", "extracted": {}}),
        patch("app.services.blueprint_candidate_registry.get_etf_factsheet_history_summary", return_value=None),
        patch("app.services.blueprint_candidate_registry.get_preferred_market_history_summary", return_value=None),
        patch("app.services.blueprint_candidate_registry.get_preferred_latest_market_data", return_value=None),
        patch("app.services.blueprint_candidate_registry.get_latest_etf_fetch_status", return_value={"status": "unknown", "entries": []}),
        patch("app.services.blueprint_candidate_registry.get_etf_holdings_profile", return_value=None),
    ):
        result = refresh_registry_candidate_truth(conn, symbol="CSPX", activate_market_series=True)

    assert calls == [("candidate_instrument_cspx", True)]
    assert result["symbol"] == "CSPX"


def test_yfinance_symbol_candidates_add_exchange_suffixes() -> None:
    from app.services.blueprint_candidate_registry import _yfinance_symbol_candidates

    assert _yfinance_symbol_candidates("A35", primary_listing_exchange="SGX") == ["A35.SI", "A35"]
    assert _yfinance_symbol_candidates("VWRA", primary_listing_exchange="LSE") == ["VWRA.L", "VWRA"]


def test_refresh_registry_candidate_truth_can_promote_yfinance_aum_and_currency() -> None:
    conn = _conn()

    class _FakeTicker:
        def __init__(self, symbol: str):
            self.info = {"totalAssets": 123456789.0, "currency": "GBp", "fundInceptionDate": 1715299200}
            self.fast_info = {"currency": "GBp"}

    with (
        patch("yfinance.Ticker", _FakeTicker),
        patch("app.services.blueprint_candidate_registry.fetch_candidate_docs", return_value={"status": "failed", "extracted": {}}),
        patch("app.services.blueprint_candidate_registry.get_etf_factsheet_history_summary", return_value=None),
        patch("app.services.blueprint_candidate_registry.get_preferred_market_history_summary", return_value=None),
        patch("app.services.blueprint_candidate_registry.get_preferred_latest_market_data", return_value=None),
        patch("app.services.blueprint_candidate_registry.get_latest_etf_fetch_status", return_value={"status": "unknown", "entries": []}),
        patch("app.services.blueprint_candidate_registry.get_etf_holdings_profile", return_value=None),
    ):
        refresh_registry_candidate_truth(conn, symbol="TAIL")

    aum = conn.execute(
        "select source_name, resolved_value_json from candidate_field_current where candidate_symbol='TAIL' and sleeve_key='convex' and field_name='aum'"
    ).fetchone()
    currency = conn.execute(
        "select source_name, resolved_value_json from candidate_field_current where candidate_symbol='TAIL' and sleeve_key='convex' and field_name='primary_trading_currency'"
    ).fetchone()
    launch_date = conn.execute(
        "select source_name, resolved_value_json from candidate_field_current where candidate_symbol='TAIL' and sleeve_key='convex' and field_name='launch_date'"
    ).fetchone()

    assert aum is not None
    assert aum[0] == "Yahoo Finance"
    assert currency is not None
    assert currency[0] == "Yahoo Finance"
    assert currency[1] == '"GBP"'
    assert launch_date is not None
    assert launch_date[0] == "Yahoo Finance"
    assert launch_date[1] == '"2024-05-10"'


def test_refresh_registry_candidate_truth_keeps_structural_currency_over_quote_currency_for_vwrl() -> None:
    conn = _conn()

    class _FakeTicker:
        def __init__(self, symbol: str):
            self.info = {"currency": "GBP"}
            self.fast_info = {"currency": "GBP"}

    with (
        patch("yfinance.Ticker", _FakeTicker),
        patch("app.services.blueprint_candidate_registry.fetch_candidate_docs", return_value={"status": "failed", "extracted": {}}),
        patch(
            "app.services.blueprint_candidate_registry.get_etf_factsheet_history_summary",
            return_value={"citation": {"source_url": "https://fund-docs.vanguard.com/FTSE_All-World_UCITS_ETF_USD_Distributing_9505_EU_INT_UK_EN.pdf"}},
        ),
        patch(
            "app.services.blueprint_candidate_registry.fetch_and_parse_etf_doc",
            return_value={
                "status": "success",
                "extracted": {
                    "primary_trading_currency": "USD",
                    "primary_listing_exchange": "LSE",
                },
            },
        ),
        patch("app.services.blueprint_candidate_registry.get_preferred_market_history_summary", return_value=None),
        patch("app.services.blueprint_candidate_registry.get_preferred_latest_market_data", return_value=None),
        patch("app.services.blueprint_candidate_registry.get_latest_etf_fetch_status", return_value={"status": "unknown", "entries": []}),
        patch("app.services.blueprint_candidate_registry.get_etf_holdings_profile", return_value=None),
    ):
        refresh_registry_candidate_truth(conn, symbol="VWRL")

    currency = conn.execute(
        """
        select source_name, resolved_value_json
        from candidate_field_current
        where candidate_symbol='VWRL' and sleeve_key='global_equity_core' and field_name='primary_trading_currency'
        """
    ).fetchone()

    assert currency is not None
    assert currency[1] == '"USD"'
    assert currency[0] != "Yahoo Finance"


def test_provider_refresh_rejects_non_us_reference_currency_for_lse_ucits() -> None:
    allowed = _provider_field_allowed(
        {
            "symbol": "VWRL",
            "sleeve_key": "global_equity_core",
            "instrument_type": "etf_ucits",
            "extra": {
                "isin": "IE00B3RBWM25",
                "primary_listing_exchange": "LSE",
                "domicile": "Ireland",
            },
        },
        provider_name="Twelve Data",
        endpoint_family="reference_meta",
        field_name="primary_trading_currency",
        field_value="EUR",
    )
    assert allowed is False
