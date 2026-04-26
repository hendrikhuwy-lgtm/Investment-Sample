from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest


def _series_payload(days: int = 320) -> dict[str, object]:
    start = datetime.now(UTC) - timedelta(days=days)
    series = []
    for offset in range(days):
        current = start + timedelta(days=offset)
        close = 100.0 + offset * 0.15
        series.append(
            {
                "date": current.date().isoformat(),
                "datetime": current.date().isoformat(),
                "open": close - 0.4,
                "high": close + 0.6,
                "low": close - 0.8,
                "close": close,
                "volume": 100000.0 + offset,
            }
        )
    return {"series": list(reversed(series))}


def test_twelve_data_resolution_prefers_exchange_alias_for_ucits(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "symbol_resolution.sqlite3"))

    from app.config import get_db_path
    from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, seed_default_candidate_registry
    from app.services.symbol_resolution import resolve_provider_identifiers
    from app.v2.blueprint_market.market_identity import ensure_candidate_market_identities

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_candidate_registry_tables(conn)
    seed_default_candidate_registry(conn)

    resolution = resolve_provider_identifiers(
        conn,
        provider_name="twelve_data",
        endpoint_family="ohlcv_history",
        identifier="IWDA",
    )

    assert resolution["provider_symbol"] == "IWDA.LSE"

    identities = ensure_candidate_market_identities(conn, "candidate_instrument_iwda")
    direct = next(item for item in identities if item["series_role"] == "direct")
    assert direct["provider_symbol"] == "IWDA.LSE"


def test_tiingo_resolution_promotes_ranked_exchange_alias_for_ucits(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "symbol_resolution_ranked.sqlite3"))

    from app.config import get_db_path
    from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, seed_default_candidate_registry
    from app.services.symbol_resolution import resolve_provider_identifiers

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_candidate_registry_tables(conn)
    seed_default_candidate_registry(conn)

    resolution = resolve_provider_identifiers(
        conn,
        provider_name="tiingo",
        endpoint_family="ohlcv_history",
        identifier="VWRA",
    )

    assert resolution["provider_symbol"] == "VWRA.LSE"
    assert resolution["provider_identifier_strategy"] in {"provider_alias", "exchange_qualified_alias"}


def test_market_series_refresh_uses_secondary_history_recovery_when_twelve_data_fails(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "series_recovery.sqlite3"))

    from app.config import get_db_path
    from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, seed_default_candidate_registry
    from app.services.provider_adapters import ProviderAdapterError
    import app.v2.blueprint_market.series_refresh_service as refresh_service
    from app.v2.blueprint_market.series_store import load_price_series

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_candidate_registry_tables(conn)
    seed_default_candidate_registry(conn)

    monkeypatch.setattr(
        refresh_service.TwelveDataPriceClient,
        "fetch_daily_ohlcv",
        lambda self, symbol: (_ for _ in ()).throw(
            ProviderAdapterError("twelve_data", "ohlcv_history", "upstream unavailable", error_class="upstream_error")
        ),
    )
    monkeypatch.setattr(
        refresh_service,
        "fetch_provider_data",
        lambda provider_name, endpoint_family, identifier: _series_payload(),
    )

    result = refresh_service.backfill_candidate_series(
        conn,
        candidate_id="candidate_instrument_vwra",
        force_refresh=True,
    )

    assert any(item["status"] == "recovered_secondary" for item in result["series_roles"])
    rows = load_price_series(
        conn,
        candidate_id="candidate_instrument_vwra",
        series_role="direct",
        interval="1day",
        ascending=True,
    )
    assert rows
    assert rows[-1]["provider"] in {"polygon", "fmp"}


def test_candidate_report_route_emits_explicit_cache_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.router as router

    stable_candidate_id = "candidate_instrument_vwra"
    source_snapshot_id = "surface_snapshot_test_source"
    source_contract_version = "0.3.1"
    cache_key = router._report_binding_cache_key(
        stable_candidate_id,
        source_snapshot_id=source_snapshot_id,
        source_contract_version=source_contract_version,
    )
    with router._MEM_LOCK:
        router._MEM_CACHE.pop(cache_key, None)
    router._mem_set(
        cache_key,
        {
            "surface_id": "candidate_report",
            "candidate_id": stable_candidate_id,
            "generated_at": "2026-04-16T00:00:00+00:00",
            "bound_source_snapshot_id": source_snapshot_id,
            "source_contract_version": source_contract_version,
            "report_loading_hint": {
                "strategy": "summary_then_sections",
                "summary": "Load summary strip first.",
            },
        },
    )

    payload = router.candidate_report(
        "VWRA",
        source_snapshot_id=source_snapshot_id,
        source_contract_version=source_contract_version,
    )

    assert payload["report_cache_state"] == "memory"
    assert payload["route_cache_state"]["state"] == "memory"
    assert payload["report_loading_hint"]["route_cache_state"] == "memory"


def test_candidate_report_cache_key_is_binding_specific() -> None:
    import app.v2.router as router

    left = router._report_binding_cache_key(
        "candidate_instrument_vwra",
        source_snapshot_id="surface_snapshot_a",
        source_contract_version="0.3.1",
    )
    right = router._report_binding_cache_key(
        "candidate_instrument_vwra",
        source_snapshot_id="surface_snapshot_b",
        source_contract_version="0.3.1",
    )

    assert left != right


def test_candidate_report_missing_bound_snapshot_returns_pending(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.router as router

    kicked: list[dict[str, str | None]] = []
    monkeypatch.setattr(router, "_cached_report_from_memory", lambda **kwargs: None)
    monkeypatch.setattr(router, "_cached_report_from_snapshot", lambda **kwargs: None)
    monkeypatch.setattr(router, "_embedded_report_from_explorer_snapshot", lambda **kwargs: None)
    monkeypatch.setattr(router, "_latest_stale_candidate_report", lambda **kwargs: None)
    monkeypatch.setattr(router, "_source_snapshot_exists", lambda source_snapshot_id: True)
    monkeypatch.setattr(router, "_kick_candidate_report_build", lambda **kwargs: kicked.append(kwargs))

    payload = router.candidate_report(
        "VWRA",
        source_snapshot_id="surface_snapshot_missing_report",
        source_contract_version="0.3.1",
    )

    assert payload["status"] == "report_pending"
    assert payload["retry_after_ms"] == 1500
    assert kicked and kicked[0]["source_snapshot_id"] == "surface_snapshot_missing_report"


def test_candidate_report_explicit_refresh_uses_refresh_mode_background(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.router as router

    kicked: list[dict[str, str | None]] = []
    monkeypatch.setattr(
        router,
        "_cached_report_from_memory",
        lambda **kwargs: (
            {
                "surface_id": "candidate_report",
                "candidate_id": "candidate_instrument_vwra",
                "generated_at": "2026-04-16T00:00:00+00:00",
                "bound_source_snapshot_id": "surface_snapshot_refresh",
                "source_contract_version": "0.3.1",
            },
            router._report_cache_state(state="memory", summary="Served from memory cache."),
        ),
    )
    monkeypatch.setattr(router, "_cached_report_from_snapshot", lambda **kwargs: None)
    monkeypatch.setattr(router, "_embedded_report_from_explorer_snapshot", lambda **kwargs: None)
    monkeypatch.setattr(router, "_kick_candidate_report_build", lambda **kwargs: kicked.append(kwargs))

    payload = router.candidate_report(
        "VWRA",
        source_snapshot_id="surface_snapshot_refresh",
        source_contract_version="0.3.1",
        refresh=True,
    )

    assert payload["route_cache_state"]["revalidating"] is True
    assert kicked and kicked[0]["report_build_mode"] == "refresh"


def test_candidate_report_does_not_serve_mismatched_bound_memory() -> None:
    import app.v2.router as router

    stable_candidate_id = "candidate_instrument_vwra"
    wrong_snapshot_id = "surface_snapshot_old"
    cache_key = router._report_binding_cache_key(
        stable_candidate_id,
        source_snapshot_id="surface_snapshot_new",
        source_contract_version="0.3.1",
    )
    with router._MEM_LOCK:
        router._MEM_CACHE.pop(cache_key, None)
    router._mem_set(
        cache_key,
        {
            "surface_id": "candidate_report",
            "candidate_id": stable_candidate_id,
            "generated_at": "2026-04-16T00:00:00+00:00",
            "bound_source_snapshot_id": wrong_snapshot_id,
            "source_contract_version": "0.3.1",
        },
    )

    result = router._cached_report_from_memory(
        stable_candidate_id=stable_candidate_id,
        source_snapshot_id="surface_snapshot_new",
        source_generated_at=None,
        source_contract_version="0.3.1",
    )

    assert result is None


def test_candidate_report_snapshot_store_dedupes_identical_bound_report(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "report_snapshot_dedupe.sqlite3"))

    from app.v2.storage.surface_snapshot_store import record_surface_snapshot

    contract = {
        "surface_id": "candidate_report",
        "candidate_id": "candidate_instrument_vwra",
        "generated_at": "2026-04-16T00:00:00+00:00",
        "report_generated_at": "2026-04-16T00:00:00+00:00",
        "bound_source_snapshot_id": "surface_snapshot_source_a",
        "source_contract_version": "0.3.1",
        "investment_case": "Case text.",
    }
    first = record_surface_snapshot(
        surface_id="candidate_report",
        object_id="candidate_instrument_vwra",
        snapshot_kind="recommendation_state",
        state_label="review",
        data_confidence="mixed",
        decision_confidence="medium",
        generated_at=contract["generated_at"],
        contract=contract,
        input_summary={"candidate_symbol": "VWRA"},
    )
    second = record_surface_snapshot(
        surface_id="candidate_report",
        object_id="candidate_instrument_vwra",
        snapshot_kind="recommendation_state",
        state_label="review",
        data_confidence="mixed",
        decision_confidence="medium",
        generated_at="2026-04-16T00:01:00+00:00",
        contract={**contract, "generated_at": "2026-04-16T00:01:00+00:00"},
        input_summary={"candidate_symbol": "VWRA"},
    )

    assert second == first


def test_blueprint_refresh_scope_returns_already_running_for_second_claim() -> None:
    from app.services.blueprint_refresh_monitor import (
        BLUEPRINT_WRITE_REFRESH_SCOPE,
        claim_refresh_scope,
        release_refresh_scope,
    )

    first = claim_refresh_scope(BLUEPRINT_WRITE_REFRESH_SCOPE, owner="test_first")
    try:
        second = claim_refresh_scope(BLUEPRINT_WRITE_REFRESH_SCOPE, owner="test_second")
        assert first["acquired"] is True
        assert second["acquired"] is False
        assert second["status"] == "already_running"
    finally:
        if first.get("acquired"):
            release_refresh_scope(BLUEPRINT_WRITE_REFRESH_SCOPE, owner="test_first", result_status="finished")


def test_reconcile_stale_blueprint_refresh_runs_marks_old_running_rows_abandoned(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "blueprint_refresh_cleanup.sqlite3"))

    from app.config import get_db_path
    from app.services.blueprint_refresh_monitor import (
        ensure_blueprint_refresh_tables,
        reconcile_stale_blueprint_refresh_runs,
    )

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_blueprint_refresh_tables(conn)
    stale_started_at = (datetime.now(UTC) - timedelta(minutes=45)).isoformat()
    conn.execute(
        """
        INSERT INTO blueprint_refresh_runs (
          run_id, trigger_source, started_at, status, candidate_count, details_json
        ) VALUES (?, ?, ?, 'running', 0, '{}')
        """,
        ("bp_refresh_stale_case", "test_suite", stale_started_at),
    )
    conn.commit()

    summary = reconcile_stale_blueprint_refresh_runs(conn)
    row = conn.execute(
        "SELECT status, finished_at, details_json FROM blueprint_refresh_runs WHERE run_id = ?",
        ("bp_refresh_stale_case",),
    ).fetchone()

    assert summary["reconciled_count"] >= 1
    assert row["status"] == "abandoned"
    assert row["finished_at"] is not None
    assert "stale_running_row_reconciled" in str(row["details_json"])


def test_resolution_prefers_sgx_alias_for_a35(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "a35_symbol_resolution.sqlite3"))

    from app.config import get_db_path
    from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, seed_default_candidate_registry
    from app.services.symbol_resolution import resolve_provider_identifiers

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_candidate_registry_tables(conn)
    seed_default_candidate_registry(conn)

    resolution = resolve_provider_identifiers(
        conn,
        provider_name="twelve_data",
        endpoint_family="ohlcv_history",
        identifier="A35",
    )

    assert resolution["provider_symbol"] == "A35.SG"
    assert resolution["resolution_reason"] == "sgx_exchange_qualified_preferred"


def test_route_hint_beats_failure_rank_for_iwdp(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "iwdp_route_hint.sqlite3"))

    from app.config import get_db_path
    from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, seed_default_candidate_registry
    from app.services.symbol_resolution import ensure_symbol_resolution_tables, resolve_provider_identifiers

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_candidate_registry_tables(conn)
    seed_default_candidate_registry(conn)
    ensure_symbol_resolution_tables(conn)
    now_iso = datetime.now(UTC).isoformat()
    conn.execute(
        """
        INSERT INTO symbol_resolution_failures (
          canonical_symbol, provider_name, endpoint_family, provider_symbol, error_class,
          failure_count, first_failed_at, last_failed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("IWDP", "twelve_data", "ohlcv_history", "IWDP.LSE", "transient_error", 7, now_iso, now_iso),
    )
    conn.execute(
        """
        INSERT INTO symbol_resolution_failures (
          canonical_symbol, provider_name, endpoint_family, provider_symbol, error_class,
          failure_count, first_failed_at, last_failed_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ("IWDP", "twelve_data", "ohlcv_history", "IWDP.SW", "transient_error", 0, now_iso, now_iso),
    )
    conn.commit()

    resolution = resolve_provider_identifiers(
        conn,
        provider_name="twelve_data",
        endpoint_family="ohlcv_history",
        identifier="IWDP",
    )

    assert resolution["provider_symbol"] == "IWDP.LSE"
    assert resolution["resolution_reason"] == "ucits_exchange_qualified_preferred"


def test_non_us_quote_routing_prefers_route_capable_providers_for_ucits() -> None:
    from app.services.provider_registry import routed_provider_candidates

    providers = routed_provider_candidates("quote_latest", identifier="VWRA")

    assert providers[:2] == ["twelve_data", "yahoo_finance"]


def test_candidate_series_current_success_is_credited_to_ohlcv_family(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "series_family_credit.sqlite3"))

    from app.config import get_db_path
    from app.services.blueprint_candidate_registry import ensure_candidate_registry_tables, seed_default_candidate_registry
    import app.v2.blueprint_market.series_refresh_service as refresh_service
    from app.v2.blueprint_market.series_refresh_service import backfill_candidate_series, TwelveDataPriceClient

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_candidate_registry_tables(conn)
    seed_default_candidate_registry(conn)

    monkeypatch.setattr(TwelveDataPriceClient, "fetch_daily_ohlcv", lambda self, symbol: _series_payload())
    monkeypatch.setattr(
        refresh_service,
        "ensure_candidate_market_identities",
        lambda _conn, cid: [
            {
                "candidate_id": cid,
                "instrument_id": "instrument_vwra",
                "series_role": "direct",
                "provider_symbol": "VWRA.LSE",
                "primary_interval": "1day",
                "adjustment_mode": "adjusted",
                "symbol": "VWRA",
            }
        ],
    )

    result = backfill_candidate_series(conn, candidate_id="candidate_instrument_vwra", force_refresh=True)
    row = conn.execute(
        """
        SELECT current_snapshot_count, current_terminal_state
        FROM provider_family_success
        WHERE provider_name = 'twelve_data' AND surface_name = 'blueprint' AND family_name = 'ohlcv_history'
        LIMIT 1
        """
    ).fetchone()

    assert result["status"] == "succeeded"
    assert row is not None
    assert int(row["current_snapshot_count"] or 0) >= 1
    assert str(row["current_terminal_state"] or "") == "current_success"


def test_blueprint_source_diversity_treats_stale_context_family_as_degraded_not_unavailable() -> None:
    from app.services.provider_refresh import _surface_issue_summary, _surface_source_diversity

    payload = {
        "active_targets": {
            "quote_latest": ["CSPX"],
            "reference_meta": ["CSPX"],
            "ohlcv_history": ["CSPX"],
            "benchmark_proxy": ["SPY"],
        },
        "blueprint_context": [],
        "family_success": [
            {
                "surface_name": "blueprint",
                "family_name": "benchmark_proxy_history",
                "provider_name": "yahoo_finance",
                "stale_snapshot_count": 5,
                "current_snapshot_count": 0,
                "current_terminal_state": "stale_context_only",
            },
            {
                "surface_name": "blueprint",
                "family_name": "latest_quote",
                "provider_name": "finnhub",
                "stale_snapshot_count": 7,
                "current_snapshot_count": 0,
                "current_terminal_state": "stale_context_only",
            },
            {
                "surface_name": "blueprint",
                "family_name": "ohlcv_history",
                "provider_name": "polygon",
                "stale_snapshot_count": 4,
                "current_snapshot_count": 0,
                "current_terminal_state": "stale_context_only",
            },
            {
                "surface_name": "blueprint",
                "family_name": "etf_reference_metadata",
                "provider_name": "finnhub",
                "stale_snapshot_count": 3,
                "current_snapshot_count": 0,
                "current_terminal_state": "stale_context_only",
            },
        ],
    }

    diversity = _surface_source_diversity(payload, "blueprint")
    states = {item["family"]: item["state"] for item in diversity["critical_families"]}

    assert states["benchmark_proxy_history"] == "stale_context_only"
    assert states["latest_quote"] == "stale_context_only"
    assert diversity["unavailable_count"] == 0

    issues = _surface_issue_summary(
        surface_name="blueprint",
        governance={"current_count": 1, "gap_count": 0},
        source_diversity=diversity,
    )
    assert "benchmark_proxy_history:stale_context_only" in issues["issues"]


def test_canonical_family_bridge_aggregates_impl_aliases() -> None:
    from app.services.provider_refresh import _canonical_family_name

    assert _canonical_family_name("quote_latest") == "latest_quote"
    assert _canonical_family_name("reference_meta") == "etf_reference_metadata"
    assert _canonical_family_name("benchmark_proxy") == "benchmark_proxy_history"


def test_collect_family_payloads_skips_budget_blocked_provider_before_fetch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "family_budget_skip.sqlite3"))

    from app.config import get_db_path
    from app.services.provider_family_success import (
        ensure_provider_family_success_tables,
        record_provider_family_event,
        recompute_provider_family_success,
    )
    import app.services.provider_refresh as provider_refresh

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_provider_family_success_tables(conn)
    record_provider_family_event(
        conn,
        provider_name="polygon",
        surface_name="blueprint",
        family_name="latest_quote",
        identifier="VWRA",
        target_universe=["VWRA"],
        success=False,
        error_class="budget_block",
        cache_hit=False,
        freshness_state="unavailable",
        fallback_used=False,
        age_seconds=None,
        root_error_class="provider_budget_block",
        effective_error_class="provider_budget_block",
        triggered_by_job="test",
    )
    recompute_provider_family_success(conn, surface_name="blueprint", family_name="latest_quote")

    attempts: list[str] = []

    monkeypatch.setattr(
        provider_refresh,
        "routed_provider_candidates",
        lambda family, *, identifier=None: ["polygon", "twelve_data", "yahoo_finance"],
    )

    def _fake_fetch(*_args, provider_name: str, **_kwargs):
        attempts.append(provider_name)
        return {
            "provider_name": provider_name,
            "identifier": "VWRA",
            "cache_status": "miss",
            "observed_at": "2026-04-20T00:00:00+00:00",
            "governance": {
                "operational_freshness_state": "current",
                "terminal_state": "current_success",
                "terminal_cause": None,
            },
        }

    monkeypatch.setattr(provider_refresh, "fetch_with_cache", _fake_fetch)

    payloads = provider_refresh._collect_family_payloads(
        conn,
        surface_name="blueprint",
        endpoint_family="quote_latest",
        identifiers=["VWRA"],
        triggered_by_job="test",
        force_refresh=True,
    )

    assert attempts
    assert attempts[0] == "twelve_data"
    assert payloads


def test_collect_family_payloads_uses_primary_and_single_rescue(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "provider_bounds.sqlite3"))

    from app.config import get_db_path
    from app.services.provider_adapters import ProviderAdapterError
    import app.services.provider_refresh as provider_refresh

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row

    attempts: list[str] = []

    monkeypatch.setattr(provider_refresh, "_ranked_provider_candidates", lambda *_args, **_kwargs: ["one", "two", "three"])
    monkeypatch.setattr(provider_refresh, "routed_provider_candidates", lambda *_args, **_kwargs: ["one", "two", "three"])
    monkeypatch.setattr(provider_refresh, "provider_support_status", lambda *_args, **_kwargs: (True, None))

    def _fake_fetch_with_cache(*_args, provider_name: str, **_kwargs):
        attempts.append(provider_name)
        if provider_name == "one":
            raise ProviderAdapterError(provider_name, "ohlcv_history", "empty", error_class="empty_response")
        return {
            "freshness_state": "fresh",
            "cache_status": "miss",
            "governance": {"terminal_state": "current_success", "terminal_cause": None},
        }

    monkeypatch.setattr(provider_refresh, "fetch_with_cache", _fake_fetch_with_cache)
    monkeypatch.setattr(provider_refresh, "record_provider_family_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(provider_refresh, "recompute_provider_family_success", lambda *args, **kwargs: None)
    monkeypatch.setattr(provider_refresh, "compare_family_providers", lambda *args, **kwargs: {})

    payloads = provider_refresh._collect_family_payloads(
        conn,
        surface_name="blueprint",
        endpoint_family="ohlcv_history",
        identifiers=["CSPX"],
        triggered_by_job="test_suite",
        force_refresh=False,
    )

    assert attempts == ["one", "two"]
    assert len(payloads) == 1
