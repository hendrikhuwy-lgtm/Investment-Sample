from __future__ import annotations

from datetime import UTC, datetime, timedelta
import sqlite3

from app.services.blueprint_candidate_truth import ensure_candidate_truth_tables, seed_required_field_matrix, upsert_field_observation
from app.services.provider_family_success import _terminal_result
from app.services.provider_registry import canonical_blueprint_family_id
from app.services.source_truth_registry import truth_family_registry
from app.v2.donors.source_freshness import FreshnessClass
from app.v2.truth.candidate_quality import (
    _normalize_reconciliation_value,
    build_data_quality_summary,
    build_failure_class_summary,
    build_implementation_profile,
    build_identity_state,
    enrich_score_decomposition_with_market_path_support,
    build_reconciliation_report,
    build_reconciliation_summary,
    build_recommendation_gate,
    build_score_decomposition,
    build_source_completion_summary,
    build_source_authority_map,
)
from app.v2.truth.envelopes import (
    build_macro_truth_envelope,
    build_market_session_context,
    classify_market_quote_freshness,
)
from app.v2.storage.surface_snapshot_store import latest_surface_snapshot, record_surface_snapshot


def test_market_quote_freshness_is_session_aware_during_regular_hours() -> None:
    now = datetime(2026, 4, 1, 18, 0, tzinfo=UTC)  # 14:00 New York, regular session
    fresh = classify_market_quote_freshness(
        as_of="2026-04-01T17:55:00+00:00",
        exchange="NYSEARCA",
        asset_class="etf",
        identifier="SPY",
        now=now,
    )
    stored = classify_market_quote_freshness(
        as_of="2026-04-01T16:00:00+00:00",
        exchange="NYSEARCA",
        asset_class="etf",
        identifier="SPY",
        now=now,
    )
    assert fresh == FreshnessClass.FRESH_FULL_REBUILD
    assert stored == FreshnessClass.STORED_VALID_CONTEXT


def test_macro_truth_envelope_carries_reference_period_release_and_vintage() -> None:
    envelope = build_macro_truth_envelope(
        series_id="CPIAUCSL",
        observation_date="2026-03-01",
        source_authority="fred_api",
        acquisition_mode="live",
        retrieved_at_utc="2026-04-04T00:00:00+00:00",
        release_date="2026-03-12",
        availability_date="2026-03-12",
        realtime_start="2026-03-12",
        realtime_end="9999-12-31",
        vintage_class="realtime_release",
        revision_state="first_release",
        release_semantics_state="fred_realtime_vintage",
    )
    assert envelope["reference_period"] == "2026-03"
    assert envelope["release_date"] == "2026-03-12"
    assert envelope["availability_date"] == "2026-03-12"
    assert envelope["vintage_class"] == "realtime_release"
    assert envelope["revision_state"] == "first_release"
    assert envelope["period_clock_class"] == "monthly_release"
    assert envelope["recommendation_critical"] is True


def test_recommendation_gate_blocks_on_critical_missing_fields() -> None:
    gate = build_recommendation_gate(
        completeness={
            "critical_required_fields_missing": ["expense_ratio", "benchmark_key"],
            "stale_required_count": 0,
            "proxy_only_count": 0,
            "readiness_level": "recommendation_ready",
        },
        implementation_profile={"execution_suitability": "execution_mixed"},
        reconciliation={"status": "verified", "summary": "ok"},
    )
    assert gate["gate_state"] == "blocked"
    assert sorted(gate["critical_missing_fields"]) == ["benchmark_key", "expense_ratio"]
    assert gate["blocked_reasons"]


def test_recommendation_gate_can_be_admissible_when_complete_and_clean() -> None:
    gate = build_recommendation_gate(
        completeness={
            "critical_required_fields_missing": [],
            "stale_required_count": 0,
            "proxy_only_count": 0,
            "readiness_level": "recommendation_ready",
        },
        implementation_profile={"execution_suitability": "execution_efficient"},
        reconciliation={"status": "verified", "summary": "clean"},
    )
    assert gate["gate_state"] == "admissible"
    assert gate["data_confidence"] == "high"


def test_source_truth_registry_serializes_freshness_policy_compatibly() -> None:
    registry = truth_family_registry()
    assert registry["sources"]["market_price"]["freshness_policy"]


def test_canonical_blueprint_family_id_aliases_surface_and_impl_ids() -> None:
    assert canonical_blueprint_family_id("quote_latest") == "latest_quote"
    assert canonical_blueprint_family_id("reference_meta") == "etf_reference_metadata"
    assert canonical_blueprint_family_id("benchmark_proxy") == "benchmark_proxy_history"


def test_recommendation_gate_blocks_when_identity_state_is_blocking() -> None:
    gate = build_recommendation_gate(
        completeness={
            "critical_required_fields_missing": [],
            "stale_required_count": 0,
            "proxy_only_count": 0,
            "readiness_level": "recommendation_ready",
        },
        implementation_profile={"execution_suitability": "execution_efficient"},
        reconciliation={"status": "verified", "summary": "clean"},
        identity_state={
            "state": "conflict",
            "blocking": True,
            "summary": "Identity conflict remains unresolved.",
        },
        source_authority_map=[],
        reconciliation_report=[],
    )
    assert gate["gate_state"] == "blocked"
    assert any("identity" in reason.lower() or "conflict" in reason.lower() for reason in gate["blocked_reasons"])


def test_recommendation_gate_keeps_execution_weak_as_review_when_route_and_history_are_strong() -> None:
    gate = build_recommendation_gate(
        completeness={
            "critical_required_fields_missing": [],
            "stale_required_count": 0,
            "proxy_only_count": 0,
            "readiness_level": "recommendation_ready",
        },
        implementation_profile={
            "execution_suitability": "execution_weak",
            "route_validity_state": "proxy_ready",
            "history_depth_state": "strong",
            "execution_confidence": "degraded",
        },
        reconciliation={"status": "verified", "summary": "clean"},
        source_authority_map=[],
        reconciliation_report=[],
    )
    assert gate["gate_state"] == "review_only"
    assert gate["execution_blocking"] is False


def test_reconciliation_normalization_collapses_equivalent_expense_ratio_values() -> None:
    assert _normalize_reconciliation_value("expense_ratio", 0.0007) == _normalize_reconciliation_value("expense_ratio", 0.0007000000000000001)


def test_reconciliation_normalization_collapses_physical_replication_variants() -> None:
    assert _normalize_reconciliation_value("replication_method", "physical") == "PHYSICAL"
    assert _normalize_reconciliation_value("replication_method", "Physical (optimized sampling)") == "PHYSICAL"
    assert _normalize_reconciliation_value("replication_method", "Physical-Full") == "PHYSICAL"


def test_source_completion_summary_exposes_clean_flags_and_conflict_fields() -> None:
    summary = build_source_completion_summary(
        source_authority_map=[
            {
                "field_name": "expense_ratio",
                "authority_class": "issuer_secondary",
                "freshness_state": "current",
                "source_name": "issuer_doc_parser",
                "resolved_value": 0.0007,
            },
            {
                "field_name": "benchmark_name",
                "authority_class": "verified_current_truth",
                "freshness_state": "current",
                "source_name": "benchmark_registry",
                "resolved_value": "S&P 500 Index",
            },
        ],
        implementation_profile={},
        reconciliation_report=[
            {"field_name": "expense_ratio", "status": "hard_conflict"},
            {"field_name": "benchmark_name", "status": "verified"},
        ],
    )
    assert summary["state"] == "incomplete"
    assert summary["conflict_fields"] == ["expense_ratio"]
    assert summary["authority_clean"] is True
    assert summary["freshness_clean"] is True
    assert summary["conflict_clean"] is False
    assert summary["completeness_clean"] is True


def test_parse_domicile_ignores_legal_disclaimer_text() -> None:
    from app.services.etf_doc_parser import _parse_domicile

    text = "Domicile Where Such An Offer Orsolicitation Is Against The Law"
    assert _parse_domicile(text) is None


def test_parse_domicile_ignores_access_disclaimer_text() -> None:
    from app.services.etf_doc_parser import _parse_domicile

    text = "Domicile In Which It Is Being Accessed"
    assert _parse_domicile(text) is None


def test_parse_issuer_name_ignores_sentence_like_garbage() -> None:
    from app.services.etf_doc_parser import _parse_issuer_name

    text = "Issuer: rating may be used to determine index classification. Bloomberg Index breakdowns are"
    assert _parse_issuer_name(text) is None


def test_parse_launch_date_supports_fund_inception_label() -> None:
    from app.services.etf_doc_parser import _parse_launch_date

    assert _parse_launch_date("Fund Inception 09/23/2020") == "2020-09-23"


def test_parse_aum_usd_supports_total_assets_million_pattern() -> None:
    from app.services.etf_doc_parser import _parse_aum_usd

    assert _parse_aum_usd("Total assets (million) $5,655 | Share class assets (million) $654") == 5_655_000_000.0


def test_parse_tracking_difference_from_total_return_table() -> None:
    from app.services.etf_doc_parser import _parse_tracking_differences

    text = """
    QTD YTD 1-year 3-year 5-year Since fund inception
    NAV 1.00 2.00 5.00 6.00 7.00 8.00
    Market Value 1.10 2.10 5.10 6.10 7.10 8.10
    Index 0.90 1.90 4.80 5.50 6.50 7.50
    """
    tracking = _parse_tracking_differences(text)
    assert tracking["tracking_difference_1y"] == 0.002
    assert tracking["tracking_difference_3y"] == 0.005
    assert tracking["tracking_difference_5y"] == 0.005


def test_parse_tracking_difference_from_annualized_table() -> None:
    from app.services.etf_doc_parser import _parse_tracking_differences

    text = """
    1y 3y 5y 10y Since Inception
    NAV 5.00 6.00 7.00 8.00 9.00
    Market Price 5.10 6.10 7.10 8.10 9.10
    Benchmark 4.90 5.80 6.50 7.00 8.00
    """
    tracking = _parse_tracking_differences(text)
    assert tracking["tracking_difference_1y"] == 0.001
    assert tracking["tracking_difference_3y"] == 0.002
    assert tracking["tracking_difference_5y"] == 0.005


def test_market_session_context_marks_early_close_and_after_hours() -> None:
    now = datetime(2026, 11, 27, 19, 30, tzinfo=UTC)  # 14:30 New York, day after Thanksgiving
    session = build_market_session_context(
        as_of="2026-11-27T18:30:00+00:00",
        exchange="NYSEARCA",
        asset_class="etf",
        identifier="SPY",
        now=now,
    )
    assert session is not None
    assert session["is_early_close"] is True
    assert session["session_state"] == "after_hours"
    assert session["extended_hours_state"] == "after_hours"


def test_market_session_context_supports_major_international_exchange() -> None:
    now = datetime(2026, 4, 7, 9, 0, tzinfo=UTC)  # 10:00 London, regular session
    session = build_market_session_context(
        as_of="2026-04-07T08:55:00+00:00",
        exchange="XLON",
        asset_class="etf",
        identifier="VWRA",
        now=now,
    )
    assert session is not None
    assert session["calendar_scope"] == "uk_equities"
    assert session["calendar_precision"] == "full"
    assert session["session_state"] == "regular_hours"


def test_recommendation_gate_keeps_stale_execution_proxy_review_only() -> None:
    gate = build_recommendation_gate(
        completeness={
            "critical_required_fields_missing": [],
            "stale_required_count": 0,
            "proxy_only_count": 0,
            "readiness_level": "recommendation_ready",
        },
        implementation_profile={"execution_suitability": "execution_efficient"},
        reconciliation={"status": "verified", "summary": "clean"},
        source_authority_map=[
            {
                "field_name": "bid_ask_spread_proxy",
                "freshness_state": "stale",
                "authority_class": "provider_or_market_summary",
                "recommendation_critical": True,
            }
        ],
        reconciliation_report=[
            {
                "field_name": "bid_ask_spread_proxy",
                "status": "stale",
                "blocking_effect": "review",
                "recommendation_critical": True,
            }
        ],
    )
    assert gate["gate_state"] == "review_only"
    assert "review-only" in gate["summary"].lower()


def test_source_authority_map_uses_verification_metadata_factsheet_support() -> None:
    authority = build_source_authority_map(
        {
            "symbol": "IWDA",
            "verification_metadata": {
                "factsheet_summary": {
                    "citation": {"source_url": "https://issuer.example/factsheet.pdf"}
                }
            },
            "source_links": ["https://issuer.example/factsheet.pdf"],
        },
        resolved_truth={
            "benchmark_name": {
                "resolved_value": "MSCI World Index",
                "source_name": "benchmark_registry",
                "source_type": "mapping_authority",
                "provenance_level": "verified_mapping",
                "missingness_reason": "populated",
                "value_type": "verified",
                "observed_at": "2026-04-20",
                "source_url": None,
            }
        },
        benchmark_assignment={"benchmark_key": "MSCI_WORLD", "benchmark_label": "MSCI World Index"},
        applicable_field_names={"benchmark_name"},
    )
    benchmark_row = next(item for item in authority if item["field_name"] == "benchmark_name")
    assert benchmark_row["document_support_state"] == "backed"


def test_data_quality_summary_does_not_count_verified_mapping_as_document_gap() -> None:
    summary = build_data_quality_summary(
        source_authority_map=[
            {
                "field_name": "benchmark_name",
                "recommendation_critical": True,
                "authority_class": "verified_current_truth",
                "document_support_state": "missing",
                "freshness_state": "current",
            }
        ],
        recommendation_gate={"data_confidence": "high"},
        reconciliation={"status": "verified"},
        reconciliation_report=[],
    )
    assert summary["document_gap_count"] == 0


def test_recommendation_gate_downgrades_spread_proxy_conflict_to_review_only() -> None:
    gate = build_recommendation_gate(
        completeness={
            "critical_required_fields_missing": [],
            "stale_required_count": 0,
            "proxy_only_count": 0,
            "readiness_level": "recommendation_ready",
        },
        implementation_profile={"execution_suitability": "execution_efficient"},
        reconciliation={"status": "soft_drift", "summary": "Execution proxy fields still show bounded drift."},
        source_authority_map=[],
        reconciliation_report=[
            {
                "field_name": "bid_ask_spread_proxy",
                "status": "execution_review_required",
                "blocking_effect": "review",
                "recommendation_critical": True,
            }
        ],
    )
    assert gate["gate_state"] == "review_only"
    assert any("execution proxy support still needs review" in reason.lower() for reason in gate["blocked_reasons"])


def test_execution_profile_uses_route_and_market_evidence_not_missing_aum() -> None:
    profile = build_implementation_profile(
        None,
        {
            "symbol": "DBMF",
            "liquidity_score": 0.82,
        },
        resolved_truth={
            "benchmark_name": {"resolved_value": "SG CTA Index"},
            "primary_listing_exchange": {"resolved_value": "NYSEARCA"},
            "primary_trading_currency": {"resolved_value": "USD"},
            "bid_ask_spread_proxy": {"resolved_value": 12.0},
            "market_data_as_of": {"resolved_value": datetime.now(UTC).isoformat()},
            "volume_30d_avg": {"resolved_value": 250000.0},
            "direct_history_depth": {"resolved_value": 840},
            "route_validity_state": {"resolved_value": "direct_ready"},
            "liquidity_proxy": {
                "resolved_value": "direct_history_backed",
                "source_name": "market_route_runtime",
                "source_type": "derived_from_validated_history",
            },
        },
    )
    assert profile["execution_confidence"] in {"strong", "usable"}
    assert profile["execution_suitability"] != "execution_weak"
    assert profile["liquidity_support_state"] in {"usable", "strong"}


def test_execution_profile_uses_stronger_proxy_history_when_direct_history_is_thin() -> None:
    profile = build_implementation_profile(
        None,
        {
            "symbol": "SSAC",
            "liquidity_score": 0.7,
        },
        resolved_truth={
            "benchmark_name": {"resolved_value": "MSCI ACWI Index"},
            "replication_method": {"resolved_value": "physical"},
            "primary_listing_exchange": {"resolved_value": "LSE"},
            "primary_trading_currency": {"resolved_value": "USD"},
            "bid_ask_spread_proxy": {"resolved_value": 5.9},
            "volume_30d_avg": {"resolved_value": 61545.0},
            "liquidity_proxy": {
                "resolved_value": "proxy_history_backed",
                "source_name": "market_route_runtime",
                "source_type": "derived_from_validated_history",
            },
            "aum": {"resolved_value": 27273730000.0},
            "domicile": {"resolved_value": "IRELAND"},
            "distribution_type": {"resolved_value": "accumulating"},
            "issuer": {"resolved_value": "BlackRock"},
            "market_data_as_of": {"resolved_value": "2026-04-06T00:00:00.000Z"},
            "route_validity_state": {"resolved_value": "proxy_ready"},
            "direct_history_depth": {"resolved_value": 34},
            "proxy_history_depth": {"resolved_value": 4538},
        },
    )
    assert profile["history_depth_state"] == "strong"
    assert profile["execution_confidence"] in {"usable", "degraded"}


def test_execution_profile_uses_stored_series_when_truth_depths_are_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "series_depths.sqlite3"))

    from app.config import get_db_path
    from app.v2.blueprint_market.series_store import ensure_blueprint_market_tables, upsert_price_series_rows

    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    ensure_blueprint_market_tables(conn)
    now = datetime.now(UTC)
    upsert_price_series_rows(
        conn,
        [
            {
                "candidate_id": "candidate_instrument_caos",
                "instrument_id": "instrument_caos",
                "series_role": "direct",
                "timestamp_utc": (now.replace(microsecond=0) - timedelta(days=index)).isoformat(),
                "interval": "1day",
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1000.0,
                "provider": "twelve_data",
                "provider_symbol": "CAOS",
                "adjusted_flag": 1,
                "freshness_ts": now.isoformat(),
                "quality_flags_json": "[]",
                "series_quality_summary_json": "{}",
                "ingest_run_id": "ingest_test",
            }
            for index in range(300)
        ],
    )
    profile = build_implementation_profile(
        conn,
        {
            "symbol": "CAOS",
            "liquidity_score": 0.8,
        },
        resolved_truth={
            "benchmark_name": {"resolved_value": "S&P 500 Index"},
            "primary_trading_currency": {"resolved_value": "USD"},
            "bid_ask_spread_proxy": {"resolved_value": 8.0},
            "market_data_as_of": {"resolved_value": datetime.now(UTC).isoformat()},
            "liquidity_proxy": {
                "resolved_value": "US ETF with moderate turnover",
                "source_name": "candidate_payload",
                "source_type": "registry_seed",
            },
        },
    )
    assert profile["route_validity_state"] == "direct_ready"
    assert profile["history_depth_state"] in {"usable", "strong"}
    assert profile["execution_suitability"] != "execution_weak"


def test_failure_class_summary_uses_precise_missing_history_reason_for_hard_execution_block() -> None:
    summary = build_failure_class_summary(
        recommendation_gate={
            "execution_blocking": True,
            "critical_missing_fields": [],
        },
        reconciliation_report=[],
        source_authority_map=[],
        identity_state={"blocking": False},
        implementation_profile={
            "route_validity_state": "missing_history",
            "execution_evidence_summary": {
                "direct_history_depth": 0,
                "proxy_history_depth": 0,
            },
        },
    )
    execution_item = next(item for item in summary["items"] if item["class_id"] == "execution_invalid")
    assert "stable route is resolved" in execution_item["summary"].lower()
    assert "no usable direct or proxy market history" in execution_item["summary"].lower()
    assert execution_item["fields"] == ["direct_history_depth", "proxy_history_depth"]


def test_identity_state_prefers_trusted_isin_over_stale_conflicting_seed_rows() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_candidate_truth_tables(conn)
    seed_required_field_matrix(conn)
    upsert_field_observation(
        conn,
        candidate_symbol="SGLN",
        sleeve_key="real_assets",
        field_name="isin",
        value="IE00B579F325",
        source_name="issuer_doc_parser",
        observed_at="2026-04-18T00:00:00+00:00",
        provenance_level="verified_official",
        confidence_label="high",
    )
    upsert_field_observation(
        conn,
        candidate_symbol="SGLN",
        sleeve_key="real_assets",
        field_name="isin",
        value="US0000000001",
        source_name="candidate_registry",
        observed_at="2026-03-01T00:00:00+00:00",
        provenance_level="seeded_fallback",
        confidence_label="low",
    )
    state = build_identity_state(
        conn,
        candidate={"symbol": "SGLN", "extra": {"isin": "IE00B579F325"}},
        candidate_symbol="SGLN",
        sleeve_key="real_assets",
        resolved_truth={
            "isin": {
                "resolved_value": "IE00B579F325",
                "source_name": "issuer_doc_parser",
                "provenance_level": "verified_official",
            }
        },
    )
    assert state["blocking"] is False
    assert state["resolved_isin"] == "IE00B579F325"


def test_reconciliation_summary_normalizes_equivalent_domicile_values() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_candidate_truth_tables(conn)
    seed_required_field_matrix(conn)
    upsert_field_observation(
        conn,
        candidate_symbol="IWDA",
        sleeve_key="global_equity_core",
        field_name="domicile",
        value="IE",
        source_name="candidate_registry",
        observed_at="2026-04-18T00:00:00+00:00",
        provenance_level="seeded_fallback",
        confidence_label="low",
    )
    upsert_field_observation(
        conn,
        candidate_symbol="IWDA",
        sleeve_key="global_equity_core",
        field_name="domicile",
        value="Ireland",
        source_name="issuer_doc_parser",
        observed_at="2026-04-19T00:00:00+00:00",
        provenance_level="verified_official",
        confidence_label="high",
    )
    upsert_field_observation(
        conn,
        candidate_symbol="IWDA",
        sleeve_key="global_equity_core",
        field_name="domicile",
        value="And",
        source_name="issuer_doc_parser",
        observed_at="2026-04-17T00:00:00+00:00",
        provenance_level="verified_official",
        confidence_label="low",
    )

    summary = build_reconciliation_summary(
        conn,
        candidate_symbol="IWDA",
        sleeve_key="global_equity_core",
    )

    assert summary["status"] == "verified"
    assert "domicile" not in summary["hard_conflicts"]


def test_reconciliation_report_treats_liquidity_route_evidence_as_review_not_hard_conflict() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_candidate_truth_tables(conn)
    seed_required_field_matrix(conn)
    upsert_field_observation(
        conn,
        candidate_symbol="DBMF",
        sleeve_key="convex_protection",
        field_name="liquidity_proxy",
        value="provider_history_backed",
        source_name="Tiingo",
        observed_at="2026-04-19T00:00:00+00:00",
        provenance_level="derived_market_evidence",
        confidence_label="medium",
    )
    upsert_field_observation(
        conn,
        candidate_symbol="DBMF",
        sleeve_key="convex_protection",
        field_name="liquidity_proxy",
        value="US ETF with consistent daily creation-redemption activity",
        source_name="candidate_registry",
        observed_at="2026-04-18T00:00:00+00:00",
        provenance_level="seeded_fallback",
        confidence_label="low",
    )

    report = build_reconciliation_report(
        conn,
        candidate={"symbol": "DBMF"},
        candidate_symbol="DBMF",
        sleeve_key="convex_protection",
        resolved_truth={
            "liquidity_proxy": {
                "resolved_value": "provider_history_backed",
                "source_name": "market_route_runtime",
                "source_type": "derived_market_evidence",
                "observed_at": "2026-04-19T00:00:00+00:00",
            }
        },
    )
    liquidity_row = next(item for item in report if item["field_name"] == "liquidity_proxy")

    assert liquidity_row["status"] == "verified"
    assert liquidity_row["blocking_effect"] == "none"


def test_reconciliation_summary_ignores_quarantined_conflicts() -> None:
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    ensure_candidate_truth_tables(conn)
    seed_required_field_matrix(conn)
    upsert_field_observation(
        conn,
        candidate_symbol="VWRA",
        sleeve_key="global_equity_core",
        field_name="domicile",
        value="IRELAND",
        source_name="issuer_doc_registry",
        observed_at="2026-04-18T00:00:00+00:00",
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
        observed_at="2026-04-18T00:00:00+00:00",
        provenance_level="verified_nonissuer",
        confidence_label="medium",
        override_annotation={"reconciled_state": "quarantined", "reconciled_reason": "issuer_doc_preferred"},
    )
    summary = build_reconciliation_summary(
        conn,
        candidate_symbol="VWRA",
        sleeve_key="global_equity_core",
    )
    assert summary["status"] == "verified"
    assert summary["hard_conflicts"] == []


def test_terminal_result_distinguishes_quarantine_and_no_eligible_route() -> None:
    assert _terminal_result(
        success=False,
        freshness_state="unavailable",
        root_error_class="budget_block",
        effective_error_class="budget_block",
        suppression_reason="all routes quarantined",
    ) == ("quarantined", "all_routes_quarantined")
    assert _terminal_result(
        success=False,
        freshness_state="unavailable",
        root_error_class="budget_block",
        effective_error_class="budget_block",
        suppression_reason="no eligible route for current symbol family",
    ) == ("current_failure", "no_eligible_route")


def test_score_decomposition_exposes_explicit_rubric_fields() -> None:
    score = build_score_decomposition(
        candidate={
            "symbol": "VWRA",
            "name": "Vanguard FTSE All-World UCITS ETF",
            "expense_ratio": 0.0022,
            "domicile": "IE",
            "distribution_type": "accumulating",
            "issuer": "Vanguard",
            "role_in_portfolio": "Broad global equity core holding.",
        },
        resolved_truth={
            "expense_ratio": {"resolved_value": 0.0022},
            "tracking_difference_1y": {"resolved_value": 0.0018},
            "aum": {"resolved_value": 5_000_000_000},
            "domicile": {"resolved_value": "IE"},
            "distribution_type": {"resolved_value": "accumulating"},
            "issuer": {"resolved_value": "Vanguard"},
            "replication_method": {"resolved_value": "physical"},
            "launch_date": {"resolved_value": "2014-01-01"},
            "benchmark_key": {"resolved_value": "FTSE_ALL_WORLD"},
            "benchmark_name": {"resolved_value": "FTSE All-World Index"},
        },
        institutional_facts={"exposure_summary": "Global equity core exposure."},
        implementation_profile={
            "execution_score": 82,
            "execution_suitability": "execution_efficient",
            "summary": "Implementation stays clean enough for the sleeve.",
            "quote_freshness_state": "fresh",
            "history_depth_state": "strong",
            "spread_support_state": "usable",
            "liquidity_support_state": "strong",
            "volume_support_state": "usable",
            "route_validity_state": "direct_ready",
        },
        gate={
            "gate_state": "review_only",
            "summary": "Recommendation remains review-only while evidence is still bounded.",
        },
        data_quality_summary={
            "data_confidence": "mixed",
            "summary": "Source integrity remains usable but still mixed.",
        },
        identity_state={
            "state": "verified",
            "blocking": False,
            "summary": "Identity is clean enough to rely on.",
        },
        source_authority_map=[
            {
                "field_name": "expense_ratio",
                "authority_class": "issuer_secondary",
                "freshness_state": "current",
                "document_support_state": "backed",
            },
            {
                "field_name": "benchmark_key",
                "authority_class": "verified_current_truth",
                "freshness_state": "current",
                "document_support_state": "backed",
            },
            {
                "field_name": "benchmark_name",
                "authority_class": "verified_current_truth",
                "freshness_state": "current",
                "document_support_state": "backed",
            },
            {
                "field_name": "domicile",
                "authority_class": "verified_current_truth",
                "freshness_state": "current",
            },
            {
                "field_name": "issuer",
                "authority_class": "issuer_secondary",
                "freshness_state": "current",
            },
            {
                "field_name": "aum",
                "authority_class": "issuer_secondary",
                "freshness_state": "current",
            },
        ],
        reconciliation_report=[],
        blocker_category=None,
        sleeve_key="global_equity_core",
    )
    assert "source_integrity_score" in score
    assert "benchmark_fidelity_score" in score
    assert "long_horizon_quality_score" in score
    assert "instrument_quality_score" in score
    assert "portfolio_fit_score" in score
    assert score["score_model_version"] == "recommendation_score_v3"
    assert "deployment_score" in score
    assert "recommendation_score" in score
    assert "recommendation_merit_score" in score
    assert "investment_merit_score" in score
    assert "deployability_score" in score
    assert "truth_confidence_score" in score
    assert "source_completion_score" in score
    assert "freshness_cleanliness_score" in score
    assert "conflict_cleanliness_score" in score
    assert "truth_confidence_band" in score
    assert "truth_confidence_summary" in score
    assert "optimality_score" in score
    assert "readiness_score" in score
    assert "admissibility_identity_score" in score
    assert score["total_score"] == score["recommendation_score"]
    component_ids = {item["component_id"] for item in score["components"]}
    assert "source_integrity" in component_ids
    assert "benchmark_fidelity" in component_ids
    assert "long_horizon_quality" in component_ids
    assert "instrument_quality" in component_ids
    assert "portfolio_fit" in component_ids
    implementation = next(item for item in score["components"] if item["component_id"] == "implementation")
    assert implementation["band"] in {"strong", "good", "review", "weak", "blocked"}
    assert isinstance(implementation["confidence"], int)
    assert isinstance(implementation["reasons"], list)


def test_deployment_score_caps_reviewable_candidates_below_action_ready_band() -> None:
    score = build_score_decomposition(
        candidate={
            "symbol": "VWRA",
            "name": "Vanguard FTSE All-World UCITS ETF",
            "expense_ratio": 0.0022,
            "domicile": "IE",
            "distribution_type": "accumulating",
            "issuer": "Vanguard",
            "role_in_portfolio": "Broad global equity core holding.",
        },
        resolved_truth={
            "expense_ratio": {"resolved_value": 0.0022},
            "tracking_difference_1y": {"resolved_value": 0.0018},
            "aum": {"resolved_value": 5_000_000_000},
            "domicile": {"resolved_value": "IE"},
            "distribution_type": {"resolved_value": "accumulating"},
            "issuer": {"resolved_value": "Vanguard"},
            "replication_method": {"resolved_value": "physical"},
            "launch_date": {"resolved_value": "2014-01-01"},
            "benchmark_key": {"resolved_value": "FTSE_ALL_WORLD"},
            "benchmark_name": {"resolved_value": "FTSE All-World Index"},
        },
        institutional_facts={"exposure_summary": "Global equity core exposure."},
        implementation_profile={
            "execution_suitability": "execution_efficient",
            "quote_freshness_state": "fresh",
            "history_depth_state": "strong",
            "spread_support_state": "usable",
            "liquidity_support_state": "strong",
            "volume_support_state": "usable",
            "route_validity_state": "direct_ready",
        },
        gate={"gate_state": "review_only", "blocked_reasons": []},
        data_quality_summary={"data_confidence": "mixed", "document_gap_count": 0},
        identity_state={"state": "verified", "blocking": False},
        source_authority_map=[
            {"field_name": "expense_ratio", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "benchmark_key", "authority_class": "verified_current_truth", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "benchmark_name", "authority_class": "verified_current_truth", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "domicile", "authority_class": "verified_current_truth", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "distribution_type", "authority_class": "verified_current_truth", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "issuer", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "aum", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
        ],
        reconciliation_report=[],
        sleeve_key="global_equity_core",
    )
    assert score["readiness_posture"] == "reviewable"
    assert score["recommendation_score"] <= 74
    assert score["total_score"] == score["recommendation_score"]


def test_deployment_score_caps_blocked_gate_at_or_below_35() -> None:
    score = build_score_decomposition(
        candidate={
            "symbol": "VWRA",
            "name": "Vanguard FTSE All-World UCITS ETF",
            "expense_ratio": 0.0022,
            "domicile": "IE",
            "distribution_type": "accumulating",
            "issuer": "Vanguard",
            "role_in_portfolio": "Broad global equity core holding.",
        },
        resolved_truth={
            "expense_ratio": {"resolved_value": 0.0022},
            "tracking_difference_1y": {"resolved_value": 0.0018},
            "aum": {"resolved_value": 5_000_000_000},
            "domicile": {"resolved_value": "IE"},
            "distribution_type": {"resolved_value": "accumulating"},
            "issuer": {"resolved_value": "Vanguard"},
            "replication_method": {"resolved_value": "physical"},
            "launch_date": {"resolved_value": "2014-01-01"},
            "benchmark_key": {"resolved_value": "FTSE_ALL_WORLD"},
            "benchmark_name": {"resolved_value": "FTSE All-World Index"},
        },
        institutional_facts={"exposure_summary": "Global equity core exposure."},
        implementation_profile={
            "execution_suitability": "execution_efficient",
            "quote_freshness_state": "fresh",
            "history_depth_state": "strong",
            "spread_support_state": "usable",
            "liquidity_support_state": "strong",
            "volume_support_state": "usable",
            "route_validity_state": "direct_ready",
        },
        gate={"gate_state": "blocked", "blocked_reasons": ["Execution still blocked."]},
        data_quality_summary={"data_confidence": "mixed", "document_gap_count": 0},
        identity_state={"state": "verified", "blocking": False},
        source_authority_map=[
            {"field_name": "expense_ratio", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "benchmark_key", "authority_class": "verified_current_truth", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "benchmark_name", "authority_class": "verified_current_truth", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "issuer", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "aum", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
        ],
        reconciliation_report=[],
        sleeve_key="global_equity_core",
    )
    assert score["readiness_posture"] == "blocked"
    assert score["recommendation_score"] <= 35


def test_score_decomposition_caps_missing_aum_on_instrument_and_long_horizon() -> None:
    score = build_score_decomposition(
        candidate={
            "symbol": "VWRA",
            "name": "Vanguard FTSE All-World UCITS ETF",
            "expense_ratio": 0.0022,
            "domicile": "IE",
            "distribution_type": "accumulating",
            "issuer": "Vanguard",
        },
        resolved_truth={
            "expense_ratio": {"resolved_value": 0.0022},
            "tracking_difference_1y": {"resolved_value": 0.0018},
            "domicile": {"resolved_value": "IE"},
            "distribution_type": {"resolved_value": "accumulating"},
            "issuer": {"resolved_value": "Vanguard"},
            "replication_method": {"resolved_value": "physical"},
            "launch_date": {"resolved_value": "2014-01-01"},
            "benchmark_key": {"resolved_value": "FTSE_ALL_WORLD"},
            "benchmark_name": {"resolved_value": "FTSE All-World Index"},
        },
        institutional_facts={"exposure_summary": "Global equity core exposure."},
        implementation_profile={
            "execution_score": 82,
            "quote_freshness_state": "fresh",
            "history_depth_state": "strong",
            "spread_support_state": "usable",
            "liquidity_support_state": "strong",
            "volume_support_state": "usable",
            "route_validity_state": "direct_ready",
        },
        gate={"gate_state": "review_only"},
        data_quality_summary={"data_confidence": "mixed", "document_gap_count": 0},
        identity_state={"state": "verified", "blocking": False},
        source_authority_map=[
            {"field_name": "expense_ratio", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "benchmark_key", "authority_class": "verified_current_truth", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "benchmark_name", "authority_class": "verified_current_truth", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "replication_method", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "issuer", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "aum", "authority_class": "missing", "freshness_state": "missing", "document_support_state": "missing"},
        ],
        reconciliation_report=[],
        sleeve_key="global_equity_core",
    )
    assert score["instrument_quality_score"] <= 68
    assert score["long_horizon_quality_score"] <= 72


def test_portfolio_fit_is_not_decision_state_derived() -> None:
    kwargs = dict(
        candidate={
            "symbol": "VWRA",
            "name": "Vanguard FTSE All-World UCITS ETF",
            "expense_ratio": 0.0022,
            "domicile": "IE",
            "distribution_type": "accumulating",
            "issuer": "Vanguard",
            "role_in_portfolio": "Broad global equity core holding.",
        },
        resolved_truth={
            "expense_ratio": {"resolved_value": 0.0022},
            "tracking_difference_1y": {"resolved_value": 0.0018},
            "aum": {"resolved_value": 5_000_000_000},
            "domicile": {"resolved_value": "IE"},
            "distribution_type": {"resolved_value": "accumulating"},
            "issuer": {"resolved_value": "Vanguard"},
            "replication_method": {"resolved_value": "physical"},
            "launch_date": {"resolved_value": "2014-01-01"},
            "benchmark_key": {"resolved_value": "FTSE_ALL_WORLD"},
            "benchmark_name": {"resolved_value": "FTSE All-World Index"},
        },
        institutional_facts={"exposure_summary": "Global equity core exposure."},
        implementation_profile={
            "execution_score": 82,
            "quote_freshness_state": "fresh",
            "history_depth_state": "strong",
            "spread_support_state": "usable",
            "liquidity_support_state": "strong",
            "volume_support_state": "usable",
            "route_validity_state": "direct_ready",
        },
        data_quality_summary={"data_confidence": "mixed", "document_gap_count": 0},
        identity_state={"state": "verified", "blocking": False},
        source_authority_map=[
            {"field_name": "expense_ratio", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "benchmark_key", "authority_class": "verified_current_truth", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "benchmark_name", "authority_class": "verified_current_truth", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "replication_method", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "issuer", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
            {"field_name": "aum", "authority_class": "issuer_secondary", "freshness_state": "current", "document_support_state": "backed"},
        ],
        reconciliation_report=[],
        sleeve_key="global_equity_core",
    )
    review_score = build_score_decomposition(gate={"gate_state": "review_only"}, **kwargs)
    admissible_score = build_score_decomposition(gate={"gate_state": "admissible"}, **kwargs)
    assert review_score["portfolio_fit_score"] == admissible_score["portfolio_fit_score"]


def test_enriched_score_decomposition_adds_backend_market_path_component() -> None:
    base = build_score_decomposition(
        candidate={"symbol": "VWRA", "name": "VWRA", "expense_ratio": 0.0022, "domicile": "IE", "distribution_type": "accumulating", "issuer": "Vanguard"},
        resolved_truth={"benchmark_key": {"resolved_value": "FTSE_ALL_WORLD"}, "benchmark_name": {"resolved_value": "FTSE All-World Index"}},
        institutional_facts={"exposure_summary": "Global equity core exposure."},
        implementation_profile={"execution_score": 82, "quote_freshness_state": "fresh", "history_depth_state": "strong", "spread_support_state": "usable", "liquidity_support_state": "strong", "volume_support_state": "usable", "route_validity_state": "direct_ready"},
        gate={"gate_state": "review_only"},
        data_quality_summary={"data_confidence": "mixed", "document_gap_count": 0},
        identity_state={"state": "verified", "blocking": False},
        source_authority_map=[],
        reconciliation_report=[],
        sleeve_key="global_equity_core",
    )
    enriched = enrich_score_decomposition_with_market_path_support(
        base,
        {
            "route_state": "unavailable",
            "usefulness_label": "suppressed",
            "freshness_state": "unavailable",
            "candidate_implication": "No usable market-path support.",
            "truth_manifest": {"model_family": "kronos"},
            "sampling_summary": {"sample_path_count": 16},
            "series_quality_summary": {"bars_present": 0, "quality_label": "broken"},
        },
    )
    assert enriched is not None
    assert enriched["market_path_support_score"] <= 25
    assert enriched["score_model_version"] == "recommendation_score_v3"
    assert enriched["recommendation_score"] == enriched["total_score"]
    assert enriched["recommendation_score"] <= 69
    component_ids = {item["component_id"] for item in enriched["components"]}
    assert "market_path_support" in component_ids


def test_source_authority_map_marks_missing_and_provider_backed_fields() -> None:
    rows = build_source_authority_map(
        {
            "symbol": "VWRA",
            "primary_documents": [
                {
                    "doc_type": "factsheet",
                    "doc_url": "https://issuer.example/factsheet.pdf",
                    "status": "verified",
                    "authority_class": "issuer_secondary",
                    "retrieved_at": "2026-04-01T00:00:00+00:00",
                    "document_fingerprint": "doc123",
                }
            ],
        },
        resolved_truth={
            "expense_ratio": {
                "resolved_value": 0.0022,
                "source_name": "issuer_factsheet",
                "source_url": "https://issuer.example/factsheet.pdf",
                "source_type": "issuer_factsheet_secondary",
                "observed_at": "2026-04-01T00:00:00+00:00",
                "value_type": "current",
                "missingness_reason": "populated",
            },
            "benchmark_key": {
                "resolved_value": "FTSE_AW",
                "source_name": "registry_seed",
                "source_url": None,
                "source_type": "registry_seed",
                "observed_at": "2026-04-01T00:00:00+00:00",
                "value_type": "current",
                "missingness_reason": "populated",
            },
        },
    )
    expense_ratio = next(item for item in rows if item["field_name"] == "expense_ratio")
    domicile = next(item for item in rows if item["field_name"] == "domicile")
    benchmark_key = next(item for item in rows if item["field_name"] == "benchmark_key")
    assert expense_ratio["authority_class"] == "issuer_secondary"
    assert expense_ratio["freshness_state"] == "current"
    assert expense_ratio["document_support_state"] == "backed"
    assert expense_ratio["document_support_refs"][0]["doc_type"] == "factsheet"
    assert benchmark_key["authority_class"] == "verified_current_truth"
    assert benchmark_key["document_support_state"] == "derived_mapping"
    assert domicile["authority_class"] == "missing"
    assert domicile["freshness_state"] == "missing"


def test_source_authority_map_treats_derived_spread_evidence_as_verified_current_truth() -> None:
    rows = build_source_authority_map(
        {"symbol": "IWDA"},
        resolved_truth={
            "bid_ask_spread_proxy": {
                "resolved_value": 2.94,
                "source_name": "etf_market_data",
                "source_type": "verified_third_party_fallback",
                "provenance_level": "verified_nonissuer",
                "observed_at": "2026-04-20T00:00:00+00:00",
                "value_type": "current",
                "missingness_reason": "populated",
            },
        },
    )
    spread = next(item for item in rows if item["field_name"] == "bid_ask_spread_proxy")
    assert spread["authority_class"] == "verified_current_truth"
    assert spread["document_support_state"] == "derived_market_evidence"


def test_source_authority_map_uses_candidate_seed_truth_when_resolved_truth_is_absent() -> None:
    rows = build_source_authority_map(
        {
            "symbol": "IWDP",
            "domicile": "IE",
            "issuer": "iShares",
            "benchmark_key": "MSCI_WORLD",
            "benchmark_name": "MSCI World",
            "primary_listing_exchange": "LSE",
            "primary_trading_currency": "USD",
            "extra": {"aum_usd": 1250000000},
        },
        resolved_truth={},
    )
    domicile = next(item for item in rows if item["field_name"] == "domicile")
    issuer = next(item for item in rows if item["field_name"] == "issuer")
    aum = next(item for item in rows if item["field_name"] == "aum")
    assert domicile["authority_class"] == "registry_seed"
    assert domicile["freshness_state"] == "current"
    assert domicile["resolved_value"] == "IE"
    assert issuer["resolved_value"] == "iShares"
    assert aum["resolved_value"] == 1250000000


def test_source_authority_map_falls_back_from_null_resolved_rows_to_mapping_and_seed_truth() -> None:
    rows = build_source_authority_map(
        {
            "symbol": "IWDP",
            "benchmark_key": "GLOBAL_REITS",
            "extra": {"tracking_difference_1y": -0.0003},
        },
        resolved_truth={
            "benchmark_name": {
                "resolved_value": None,
                "source_name": "candidate_registry",
                "source_type": "internal_fallback",
                "missingness_reason": "blocked_by_parser_gap",
            },
            "tracking_difference_1y": {
                "resolved_value": None,
                "source_name": "candidate_registry",
                "source_type": "internal_fallback",
                "missingness_reason": "blocked_by_parser_gap",
            },
        },
        benchmark_assignment={
            "benchmark_key": "GLOBAL_REITS",
            "benchmark_label": "Global REIT proxy",
        },
    )
    benchmark_name = next(item for item in rows if item["field_name"] == "benchmark_name")
    tracking_difference = next(item for item in rows if item["field_name"] == "tracking_difference_1y")

    assert benchmark_name["resolved_value"] == "FTSE EPRA Nareit Developed Dividend+ Index"
    assert benchmark_name["authority_class"] == "verified_current_truth"
    assert benchmark_name["freshness_state"] == "current"
    assert tracking_difference["resolved_value"] == -0.0003
    assert tracking_difference["authority_class"] == "verified_current_truth"
    assert tracking_difference["freshness_state"] == "current"


def test_implementation_profile_normalizes_percent_unit_tracking_difference() -> None:
    profile = build_implementation_profile(
        None,
        {
            "symbol": "HMCH",
            "extra": {"tracking_difference_1y": -0.37},
            "expense_ratio": 0.0065,
            "benchmark_name": "China equity proxy",
            "primary_listing_exchange": "LSE",
            "primary_trading_currency": "USD",
            "aum_usd": 1_281_263_968.0,
            "domicile": "IE",
            "issuer": "HSBC",
            "liquidity_proxy": "mid",
        },
        resolved_truth={},
    )

    assert profile["tracking_difference"] == "-0.37%"


def test_implementation_profile_normalizes_small_percent_unit_tracking_difference() -> None:
    profile = build_implementation_profile(
        None,
        {
            "symbol": "IWDA",
            "extra": {"tracking_difference_1y": -0.05},
            "expense_ratio": 0.002,
            "benchmark_name": "MSCI World",
            "primary_listing_exchange": "LSE",
            "primary_trading_currency": "USD",
            "aum_usd": 45_200_000_000.0,
            "domicile": "IE",
            "issuer": "iShares",
            "liquidity_proxy": "mid",
        },
        resolved_truth={},
    )

    assert profile["tracking_difference"] == "-0.05%"


def test_recommendation_gate_does_not_keep_present_authority_fields_marked_missing() -> None:
    gate = build_recommendation_gate(
        completeness={
            "critical_required_fields_missing": ["domicile", "benchmark_key", "expense_ratio"],
            "stale_required_count": 0,
            "proxy_only_count": 0,
            "readiness_level": "recommendation_ready",
        },
        implementation_profile={"execution_suitability": "execution_mixed"},
        reconciliation={"status": "verified", "summary": "ok"},
        source_authority_map=[
            {
                "field_name": "domicile",
                "freshness_state": "current",
                "authority_class": "registry_seed",
                "recommendation_critical": True,
            },
            {
                "field_name": "benchmark_key",
                "freshness_state": "current",
                "authority_class": "registry_seed",
                "recommendation_critical": True,
            },
        ],
        reconciliation_report=[],
    )
    assert gate["gate_state"] == "blocked"
    assert gate["critical_missing_fields"] == ["expense_ratio"]


def test_recommendation_gate_drops_false_missing_fields_when_fallback_truth_is_current() -> None:
    gate = build_recommendation_gate(
        completeness={
            "critical_required_fields_missing": ["benchmark_name", "tracking_difference_1y", "expense_ratio"],
            "stale_required_count": 0,
            "proxy_only_count": 0,
            "readiness_level": "recommendation_ready",
        },
        implementation_profile={"execution_suitability": "execution_mixed"},
        reconciliation={"status": "verified", "summary": "ok"},
        source_authority_map=[
            {
                "field_name": "benchmark_name",
                "freshness_state": "current",
                "authority_class": "verified_current_truth",
                "recommendation_critical": True,
                "document_support_state": "backed",
            },
            {
                "field_name": "tracking_difference_1y",
                "freshness_state": "current",
                "authority_class": "registry_seed",
                "recommendation_critical": True,
                "document_support_state": "backed",
            },
        ],
        reconciliation_report=[],
    )
    assert gate["gate_state"] == "blocked"
    assert gate["critical_missing_fields"] == ["expense_ratio"]


def test_recommendation_gate_ignores_non_applicable_review_items() -> None:
    gate = build_recommendation_gate(
        completeness={
            "critical_required_fields_missing": ["expense_ratio"],
            "stale_required_count": 0,
            "proxy_only_count": 0,
            "readiness_level": "recommendation_ready",
        },
        implementation_profile={"execution_suitability": "execution_mixed"},
        reconciliation={"status": "verified", "summary": "ok"},
        source_authority_map=[
            {
                "field_name": "benchmark_name",
                "freshness_state": "missing",
                "authority_class": "missing",
                "recommendation_critical": False,
                "document_support_state": "missing",
            }
        ],
        reconciliation_report=[
            {
                "field_name": "benchmark_name",
                "status": "critical_missing",
                "recommendation_critical": False,
            }
        ],
    )
    assert gate["gate_state"] == "blocked"
    assert gate["critical_missing_fields"] == ["expense_ratio"]


def test_surface_snapshot_store_round_trips_latest_snapshot() -> None:
    snapshot_id = record_surface_snapshot(
        surface_id="candidate_report",
        object_id="candidate_instrument_vwra",
        snapshot_kind="recommendation_state",
        state_label="review",
        data_confidence="mixed",
        decision_confidence="medium",
        generated_at="2026-04-04T00:00:00+00:00",
        contract={"surface_id": "candidate_report", "name": "VWRA"},
        input_summary={"source_count": 3},
        decision_inputs={"reference_clocks": ["Period 2026-03"]},
    )
    latest = latest_surface_snapshot(
        surface_id="candidate_report",
        object_id="candidate_instrument_vwra",
    )
    assert latest is not None
    assert latest["snapshot_id"] == snapshot_id
    assert latest["data_confidence"] == "mixed"
    assert latest["contract"]["name"] == "VWRA"
    assert latest["decision_inputs"]["reference_clocks"] == ["Period 2026-03"]
