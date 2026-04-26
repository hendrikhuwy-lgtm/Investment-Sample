import sqlite3

from app.config import get_db_path
from app.models.db import connect
from app.services import upstream_health_report as upstream_health_report_module
from app.services.blueprint_payload_assembler import _apply_claim_boundary_to_value
from app.services.ingest_etf_data import fetch_configured_market_data, get_etf_holdings_profile, sync_configured_etf_data_sources
from app.services.portfolio_blueprint import build_portfolio_blueprint_payload
from app.services.provider_refresh import _blueprint_targets
from app.services.upstream_health_report import build_registry_parity_report, build_upstream_health_report, enforce_registry_parity
from app.services.upstream_truth_contract import apply_daily_brief_claim_boundary, infer_candidate_directness, precise_source_state


def _conn() -> sqlite3.Connection:
    return connect(get_db_path())


def _find_candidate(payload: dict, symbol: str) -> dict:
    target = symbol.upper()
    for sleeve in payload.get("sleeves", []):
        for candidate in sleeve.get("candidates", []):
            if str(candidate.get("symbol") or "").upper() == target:
                return candidate
    raise AssertionError(f"Candidate {symbol} not found")


def test_priority_source_sync_persists_explicit_parser_routing() -> None:
    conn = _conn()
    try:
        sync_configured_etf_data_sources(conn)
        row = conn.execute(
            """
            SELECT fetch_method, parser_type
            FROM etf_data_sources
            WHERE etf_symbol = 'A35' AND data_type = 'market_data' AND enabled = 1
            ORDER BY created_at DESC
            LIMIT 1
            """
        ).fetchone()
        assert row is not None
        assert str(row["fetch_method"] or "") == "api_call"
        assert str(row["parser_type"] or "") == "sgx_market_stub_not_implemented"
    finally:
        conn.close()


def test_summary_backed_holdings_do_not_masquerade_as_direct_holdings() -> None:
    conn = _conn()
    try:
        profile = get_etf_holdings_profile("HMCH", conn)
        assert profile is not None
        assert profile["coverage_class"] == "factsheet_summary"
        assert profile["direct_holdings_available"] is False
        assert profile["summary_support_available"] is True
        assert profile["directness_class"] == "issuer_structured_summary_backed"
        assert profile["fallback_state"] == "summary_only"
    finally:
        conn.close()


def test_blueprint_payload_exposes_canonical_upstream_truth_fields() -> None:
    payload = build_portfolio_blueprint_payload()
    candidate = _find_candidate(payload, "VEVE")

    assert isinstance(candidate.get("upstream_truth_contract"), dict)
    assert candidate["source_directness"] in {
        "direct_holdings_backed",
        "issuer_structured_summary_backed",
        "html_summary_backed",
        "structure_first",
        "proxy_backed",
    }
    assert candidate["authority_class"] in {"truth_grade", "support_grade", "structure_first", "proxy_only"}
    assert candidate["claim_limit_class"]
    assert candidate["supporting_metadata_summary"]["evidence"]["source_directness"] == candidate["source_directness"]
    if candidate["source_directness"] in {"issuer_structured_summary_backed", "html_summary_backed", "proxy_backed"}:
        assert candidate["display_source_state"] != candidate["source_state"]


def test_blueprint_provider_targets_expand_beyond_tiny_default_subset() -> None:
    conn = _conn()
    try:
        targets = _blueprint_targets(conn)
        assert len(list(targets.get("reference_meta") or [])) > 4
        assert "VWRA" in list(targets.get("reference_meta") or [])
        assert "A35" in list(targets.get("reference_meta") or [])
    finally:
        conn.close()


def test_upstream_health_report_flags_registry_and_directness_gaps() -> None:
    conn = _conn()
    try:
        parity = build_registry_parity_report(conn)
        report = build_upstream_health_report(conn)
        assert parity["status"] in {"ok", "gap_detected"}
        assert any(str(item.get("symbol") or "") == "A35" for item in parity["rows"])
        row = next(item for item in report["rows"] if item["symbol"] == "A35")
        assert row["directness_class"] in {"issuer_structured_summary_backed", "html_summary_backed", "proxy_backed"}
        assert row["remediation_recommendation"]
    finally:
        conn.close()


def test_priority_weak_symbols_surface_precise_source_states() -> None:
    expected_directness = {
        "VEVE": {"issuer_structured_summary_backed", "html_summary_backed"},
        "VAGU": {"issuer_structured_summary_backed", "html_summary_backed"},
        "BIL": {"issuer_structured_summary_backed", "html_summary_backed"},
        "BILS": {"issuer_structured_summary_backed", "html_summary_backed"},
        "HMCH": {"issuer_structured_summary_backed"},
        "XCHA": {"issuer_structured_summary_backed", "html_summary_backed"},
        "SGLN": {"html_summary_backed", "issuer_structured_summary_backed"},
        "A35": {"issuer_structured_summary_backed", "html_summary_backed"},
    }
    for symbol, allowed_directness in expected_directness.items():
        candidate = {"symbol": symbol, "instrument_type": "etf_ucits" if symbol not in {"BIL", "BILS"} else "etf_us"}
        directness = infer_candidate_directness(candidate)
        assert directness in allowed_directness
        assert precise_source_state("source_validated", directness) != "source_validated"


def test_registry_parity_enforcement_stays_nonfatal_for_current_priority_set() -> None:
    conn = _conn()
    try:
        parity = enforce_registry_parity(conn, fail_on_fatal=False)
        fatal = [row for row in parity["rows"] if row["severity"] == "fatal"]
        assert fatal == []
        assert any(row["severity"] == "warning" for row in parity["rows"] if row["symbol"] in {"BIL", "BILS", "SGOV"})
    finally:
        conn.close()


def test_registry_parity_enforcement_raises_for_fatal_rows(monkeypatch) -> None:
    def fake_report(_conn):
        return {
            "status": "gap_detected",
            "rows": [
                {
                    "symbol": "ZZZZ",
                    "severity": "fatal",
                    "missing_registries": ["source_config_registry", "doc_registry"],
                }
            ],
        }

    monkeypatch.setattr(upstream_health_report_module, "build_registry_parity_report", fake_report)
    conn = _conn()
    try:
        try:
            enforce_registry_parity(conn, fail_on_fatal=True)
        except RuntimeError as exc:
            assert "ZZZZ" in str(exc)
            assert "source_config_registry" in str(exc)
        else:
            raise AssertionError("Expected fatal parity enforcement to raise")
    finally:
        conn.close()


def test_daily_brief_bootstrap_claims_are_mechanically_downgraded() -> None:
    bounded = apply_daily_brief_claim_boundary(
        {
            "action_authority_class": "bootstrap_review_only",
            "portfolio_relevance_basis": "target_proxy",
        },
        "This signal supports decisive sleeve action.",
    )
    assert "bootstrap review context" in bounded.lower()


def test_sgx_market_path_is_explicitly_support_only() -> None:
    conn = _conn()
    try:
        result = fetch_configured_market_data("A35", conn)
        assert result["status"] == "not_implemented"
        assert result["support_class"] == "support_only"
        assert result["structural_limitation"] == "sgx_public_microstructure_under_covered"
    finally:
        conn.close()


def test_health_report_is_calibrated_for_direct_and_structure_first_symbols() -> None:
    conn = _conn()
    try:
        report = build_upstream_health_report(conn)
        vwra = next(item for item in report["rows"] if item["symbol"] == "VWRA")
        dbmf = next(item for item in report["rows"] if item["symbol"] == "DBMF")
        assert vwra["directness_class"] == "direct_holdings_backed"
        assert vwra["status"] in {"healthy", "partial"}
        assert dbmf["directness_class"] == "structure_first"
        assert dbmf["status"] == "healthy"
    finally:
        conn.close()


def test_recursive_claim_boundary_application_binds_nested_legacy_shapes() -> None:
    bounded = _apply_claim_boundary_to_value(
        {
            "summary": "This supports a stronger exposure-specific replacement claim.",
            "bullets": [
                "This candidate clearly outperforms the alternative.",
                {"detail": "This should be treated as decisive."},
            ],
        },
        {"claim_limit_class": "summary_limited"},
    )
    assert "summary-backed" in str(bounded["summary"]).lower()
    assert "summary-backed" in str(bounded["bullets"][0]).lower()
    assert "summary-backed" in str(dict(bounded["bullets"][1])["detail"]).lower()
