from __future__ import annotations

from pathlib import Path

import pytest


def test_primary_document_manifest_accepts_materialized_raw_and_registry_docs(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.truth.candidate_quality as candidate_quality

    monkeypatch.setattr(
        candidate_quality,
        "load_doc_registry",
        lambda: {
            "candidates": [
                {
                    "ticker": "TEST",
                    "name": "Test ETF",
                    "issuer": "Test Issuer",
                    "docs": {
                        "factsheet_pdf_url": "https://issuer.example/test-factsheet.pdf",
                        "kid_pdf_url": "https://issuer.example/test-kid.pdf",
                    },
                }
            ]
        },
    )

    manifest = candidate_quality._primary_document_manifest(
        {
            "symbol": "TEST",
            "primary_documents": [
                {
                    "doc_type": "factsheet",
                    "doc_url": "https://upstream.example/factsheet.pdf",
                    "status": "success",
                    "title": "Upstream Factsheet",
                    "retrieved_at": "2026-04-01T00:00:00+00:00",
                }
            ],
            "primary_document_manifest": [
                {
                    "doc_type": "prospectus",
                    "source_url": "https://upstream.example/prospectus.pdf",
                    "status": "verified",
                    "document_id": "doc_existing_prospectus",
                }
            ],
        }
    )

    doc_types = {item["doc_type"] for item in manifest}
    assert {"factsheet", "prospectus", "kid"}.issubset(doc_types)
    assert any(item["document_id"] == "doc_existing_prospectus" for item in manifest)
    assert all(item["status"] in {"success", "verified"} for item in manifest)


def test_failed_document_refs_do_not_count_as_backed(monkeypatch: pytest.MonkeyPatch) -> None:
    import app.v2.truth.candidate_quality as candidate_quality

    monkeypatch.setattr(candidate_quality, "load_doc_registry", lambda: {"candidates": []})

    rows = candidate_quality.build_source_authority_map(
        {
            "symbol": "TEST",
            "primary_documents": [
                {
                    "doc_type": "factsheet",
                    "doc_url": "https://issuer.example/broken-factsheet.pdf",
                    "status": "failed",
                }
            ],
        },
        resolved_truth={
            "expense_ratio": {
                "resolved_value": 0.12,
                "source_name": "issuer_doc_parser",
                "source_type": "issuer_factsheet",
                "source_url": "https://issuer.example/broken-factsheet.pdf",
                "observed_at": "2026-04-01T00:00:00+00:00",
            }
        },
    )

    expense_ratio = next(item for item in rows if item["field_name"] == "expense_ratio")
    assert expense_ratio["document_support_refs"]
    assert expense_ratio["document_support_state"] == "partial"
    assert expense_ratio["document_support_state"] != "backed"


def test_deep_report_support_state_is_separate_and_section_complete() -> None:
    from app.v2.surfaces.blueprint.report_contract_builder import _build_deep_report_support_state

    state = _build_deep_report_support_state(
        primary_document_manifest=[
            {
                "doc_type": "factsheet",
                "doc_url": "https://issuer.example/factsheet.pdf",
                "status": "verified",
            }
        ],
        quick_brief={
            "performance_tracking_pack": {"return_1y": 4.2, "tracking_difference_1y": -0.1},
            "composition_pack": {"number_of_stocks": 500, "top_10_weight": 22.5},
            "listing_profile": {
                "exchange": "LSE",
                "trading_currency": "USD",
                "ticker": "TEST",
                "spread_proxy": "1.2 bps",
            },
        },
        implementation_profile={"execution_confidence": "usable", "missing_fields": []},
        source_authority_fields=[],
        evidence_sources=[{"directness": "direct", "label": "Issuer factsheet"}],
        market_path_support=None,
        coverage_summary={"coverage_status": "direct_ready"},
        forecast_support=None,
        scenario_blocks=[{"scenario_type": "base_case"}],
    )

    allowed_states = {
        "direct",
        "derived",
        "proxy",
        "partial",
        "stale",
        "verified_not_applicable",
        "unavailable_with_verified_reason",
    }
    assert state["source_completion_is_report_support"] is False
    assert state["unresolved_sections"] == []
    assert set(state["section_states"]) == {
        "documents",
        "performance",
        "composition",
        "listing",
        "implementation",
        "market_path",
        "evidence_sources",
    }
    assert {item["state"] for item in state["section_states"].values()}.issubset(allowed_states)
    assert state["coverage_status"] == "direct_ready"
    assert state["market_path_support_state"] == "partial"
    assert state["forecast_artifact_state"] == "unavailable_with_verified_reason"


def test_report_binding_cache_key_and_match_include_sleeve() -> None:
    from app.v2 import router

    key_a = router._report_binding_cache_key(
        "candidate_instrument_sgln",
        sleeve_key="real_assets",
        source_snapshot_id="snap_1",
        source_contract_version="0.3.1",
    )
    key_b = router._report_binding_cache_key(
        "candidate_instrument_sgln",
        sleeve_key="alternatives",
        source_snapshot_id="snap_1",
        source_contract_version="0.3.1",
    )
    assert key_a != key_b
    assert router._report_contract_matches_binding(
        {
            "sleeve_key": "real_assets",
            "bound_source_snapshot_id": "snap_1",
            "source_contract_version": "0.3.1",
        },
        sleeve_key="real_assets",
        source_snapshot_id="snap_1",
        source_generated_at=None,
        source_contract_version="0.3.1",
    )
    assert not router._report_contract_matches_binding(
        {
            "sleeve_key": "real_assets",
            "bound_source_snapshot_id": "snap_1",
            "source_contract_version": "0.3.1",
        },
        sleeve_key="alternatives",
        source_snapshot_id="snap_1",
        source_generated_at=None,
        source_contract_version="0.3.1",
    )


def test_benchmark_truth_cached_only_does_not_call_live_fetch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("IA_DB_PATH", str(tmp_path / "benchmark_cached_only.sqlite3"))
    import app.v2.sources.benchmark_truth_adapter as adapter

    def fail_live(*args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise AssertionError("live fetch should not be called in cached-only report mode")

    monkeypatch.setattr(adapter, "fetch_market_price", fail_live)
    monkeypatch.setattr(adapter, "fetch_provider_data", fail_live)
    monkeypatch.setattr(adapter, "_official_proxy_performance_fallback", fail_live)

    payload = adapter.fetch("MSCI_WORLD", surface_name="candidate_report", allow_live_fetch=False)

    assert payload["benchmark_id"] == "MSCI_WORLD"
    assert payload["current_value"] is None
    assert payload["history_error"] in {"history_cache_missing", "history_missing"}


def test_cached_forecast_support_does_not_call_runtime_forecast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import app.v2.surfaces.blueprint.report_contract_builder as report_builder

    sample_support = {
        "provider_source": "cached_kronos_artifact",
        "usefulness_label": "usable",
        "forecast_horizon": 21,
        "scenario_summary": [],
        "series_quality_summary": {"uses_proxy_series": False, "quality_label": "good"},
        "model_metadata": {},
    }

    def fail_runtime(*args, **kwargs):  # noqa: ANN001, ANN002, ANN003
        raise AssertionError("runtime forecast should not be called in cached report mode")

    monkeypatch.setattr(report_builder, "build_candidate_market_path_support", fail_runtime)
    monkeypatch.setattr(report_builder, "latest_forecast_artifact", lambda *args, **kwargs: {"market_path_support": sample_support})

    result = report_builder._try_load_cached_forecast_support(
        "candidate_instrument_test",
        label="Test ETF",
        allow_refresh=False,
    )

    assert result is not None
    assert result["market_path_support"]["provider_source"] == "cached_kronos_artifact"
