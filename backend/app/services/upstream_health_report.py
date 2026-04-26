from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from app.config import get_repo_root
from app.services.blueprint_benchmark_registry import default_benchmark_profile_for_candidate
from app.services.blueprint_candidate_registry import export_live_candidate_registry
from app.services.etf_doc_parser import load_doc_registry
from app.services.ingest_etf_data import (
    get_etf_holdings_profile,
    get_etf_source_config,
    get_latest_etf_factsheet_metrics,
)
from app.services.upstream_truth_contract import (
    STRUCTURE_FIRST,
    classify_authority,
    classify_claim_limit,
    infer_candidate_directness,
)


_OPTIONAL_SLEEVES = {"alternatives", "convex"}
_NON_FATAL_OBJECT_TYPES = {"policy_placeholder", "strategy_placeholder"}
_PRIORITY_SYMBOLS = {"VEVE", "VAGU", "BIL", "BILS", "HMCH", "XCHA", "SGLN", "A35"}


def _doc_registry_map() -> dict[str, dict[str, Any]]:
    registry = load_doc_registry()
    mapping: dict[str, dict[str, Any]] = {}
    for item in list(registry.get("candidates") or []):
        ticker = str(item.get("ticker") or "").strip().upper()
        if ticker:
            mapping[ticker] = dict(item)
    return mapping


def _family_status(
    *,
    directness_class: str,
    authority_class: str,
    freshness_class: str,
    coverage_class: str,
    completeness_class: str,
    structural_limitations: list[str],
) -> str:
    if directness_class == "unknown" and coverage_class == "missing":
        return "weak"
    if structural_limitations and directness_class in {"proxy_backed", "support_only"}:
        return "structurally_under_covered"
    if authority_class == "truth_grade" and directness_class == "direct_holdings_backed" and freshness_class == "fresh":
        return "healthy" if completeness_class != "missing" else "partial"
    if authority_class == STRUCTURE_FIRST:
        return "healthy"
    if authority_class == "truth_grade" and directness_class in {"issuer_structured_summary_backed", "html_summary_backed"}:
        return "partial"
    if authority_class == "proxy_only":
        return "structurally_under_covered"
    if freshness_class in {"stale", "quarantined"}:
        return "partial"
    return "partial" if coverage_class in {"mixed", "summary_majority", "direct_majority"} else "weak"


def _root_cause_category(item: dict[str, Any]) -> str:
    if not item.get("source_registered"):
        return "registry_incompleteness"
    if not item.get("doc_registered"):
        return "registry_incompleteness"
    if str(item.get("fallback_state") or "") not in {"", "none"}:
        return "fallback_masking"
    if str(item.get("directness_class") or "") in {"issuer_structured_summary_backed", "html_summary_backed"}:
        return "weak_extraction"
    if str(item.get("directness_class") or "") == "proxy_backed":
        return "proxy_overuse"
    if str(item.get("freshness_class") or "") in {"stale", "quarantined"}:
        return "stale_freshness"
    return "missing_source_coverage"


def _parity_severity(row: dict[str, Any]) -> str:
    missing = list(row.get("missing_registries") or [])
    if not missing:
        if bool(row.get("proxy_benchmark_without_explicit_limit")):
            return "warning"
        return "ok"
    if str(row.get("object_type") or "") in _NON_FATAL_OBJECT_TYPES:
        return "info"
    if str(row.get("sleeve") or "") in _OPTIONAL_SLEEVES:
        return "warning"
    if bool(row.get("decision_relevant")) or str(row.get("symbol") or "") in _PRIORITY_SYMBOLS:
        return "fatal"
    return "warning"


def _best_available_source_class(
    *,
    symbol: str,
    holdings_profile: dict[str, Any],
    factsheet: dict[str, Any],
    source_config: dict[str, Any],
    structural_limitations: list[str],
) -> str:
    if holdings_profile:
        return str(holdings_profile.get("best_available_source_class") or holdings_profile.get("coverage_class") or "missing")
    data_sources = dict(source_config.get("data_sources") or {})
    holdings = dict(data_sources.get("holdings") or {})
    factsheet_source = dict(data_sources.get("factsheet") or {})
    holdings_parser = str(holdings.get("parser_type") or "").lower()
    holdings_method = str(holdings.get("method") or "").lower()
    if holdings_method == "csv_download":
        return "direct_holdings"
    if holdings_parser.endswith("_holdings_csv"):
        return "direct_holdings"
    if holdings_parser in {
        "vanguard_holdings_html_summary",
        "ssga_holdings_html_summary",
        "xtrackers_holdings_html_summary",
        "hsbc_holdings_html_summary",
        "amova_holdings_html_summary",
        "issuer_holdings_pdf_summary",
        "hsbc_holdings_pdf_summary",
    }:
        return "factsheet_summary"
    if holdings_parser == "issuer_holdings_html_summary":
        return "html_summary"
    factsheet_parser = str(factsheet_source.get("parser_type") or "").lower()
    if factsheet:
        if factsheet_parser == "issuer_factsheet_html":
            return "html_summary"
        return "factsheet_summary"
    if structural_limitations:
        return "proxy_only"
    return "missing"


def _freshness_class(factsheet: dict[str, Any], holdings_profile: dict[str, Any]) -> str:
    if factsheet:
        quality = str(factsheet.get("quality_state") or "")
        if quality == "verified":
            return "fresh"
        if quality == "partial":
            return "aging"
    if holdings_profile:
        quality = str(holdings_profile.get("quality_state") or "")
        if quality == "verified":
            return "fresh"
        if quality == "partial":
            return "aging"
    return "unknown"


def build_registry_parity_report(conn: sqlite3.Connection) -> dict[str, Any]:
    registry_symbols = export_live_candidate_registry(conn)
    source_symbols = {str(item.get("etf_symbol") or "").strip().upper() for item in list(filter(None, [get_etf_source_config(str(row.get("symbol") or "")) for row in registry_symbols]))}
    doc_map = _doc_registry_map()
    rows = []
    for row in registry_symbols:
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        source_config = get_etf_source_config(symbol) or {}
        docs = dict(doc_map.get(symbol) or {})
        benchmark_profile = default_benchmark_profile_for_candidate(row, str(row.get("sleeve_key") or ""))
        benchmark_key = str(row.get("benchmark_key") or benchmark_profile.get("benchmark_key") or "")
        benchmark_source_type = str(benchmark_profile.get("benchmark_source_type") or "")
        benchmark_confidence = str(benchmark_profile.get("benchmark_confidence") or "")
        missing = []
        if not source_config:
            missing.append("source_config_registry")
        if not docs:
            missing.append("doc_registry")
        if not benchmark_key:
            missing.append("benchmark_registry")
        row_data = {
            "symbol": symbol,
            "sleeve": str(row.get("sleeve_key") or ""),
            "object_type": str(row.get("object_type") or row.get("instrument_type") or ""),
            "source_registered": bool(source_config),
            "doc_registered": bool(docs),
            "benchmark_registered": bool(benchmark_key),
            "benchmark_source_type": benchmark_source_type,
            "benchmark_confidence": benchmark_confidence,
            "proxy_benchmark_without_explicit_limit": benchmark_source_type == "proxy_etf" and benchmark_confidence not in {"low", "medium"},
            "missing_registries": missing,
        }
        row_data["decision_relevant"] = row_data["object_type"] not in _NON_FATAL_OBJECT_TYPES and row_data["sleeve"] not in _OPTIONAL_SLEEVES
        row_data["severity"] = _parity_severity(row_data)
        rows.append(row_data)
    return {
        "status": "ok" if all(item["severity"] == "ok" for item in rows) else "gap_detected",
        "rows": rows,
        "symbols_with_gaps": [item["symbol"] for item in rows if item["severity"] in {"fatal", "warning"}],
        "source_registry_symbols": sorted(source_symbols),
    }


def enforce_registry_parity(conn: sqlite3.Connection, *, fail_on_fatal: bool = True) -> dict[str, Any]:
    report = build_registry_parity_report(conn)
    fatal_rows = [row for row in report["rows"] if str(row.get("severity") or "") == "fatal"]
    report["fatal_gap_detected"] = bool(fatal_rows)
    if fail_on_fatal and fatal_rows:
        details = "; ".join(
            f"{row['symbol']} missing {', '.join(row['missing_registries'])}"
            for row in fatal_rows
        )
        raise RuntimeError(f"Fatal upstream registry parity failure for decision-relevant symbols: {details}")
    return report


def build_upstream_health_report(conn: sqlite3.Connection) -> dict[str, Any]:
    parity = build_registry_parity_report(conn)
    report_rows: list[dict[str, Any]] = []
    doc_map = _doc_registry_map()
    for item in export_live_candidate_registry(conn):
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        sleeve = str(item.get("sleeve_key") or "")
        source_config = get_etf_source_config(symbol) or {}
        holdings_profile = get_etf_holdings_profile(symbol, conn) or {}
        factsheet = get_latest_etf_factsheet_metrics(symbol, conn) or {}
        benchmark_profile = default_benchmark_profile_for_candidate(item, sleeve)
        benchmark_key = str(item.get("benchmark_key") or benchmark_profile.get("benchmark_key") or "")
        benchmark_support_class = str(
            benchmark_profile.get("benchmark_kind")
            or benchmark_profile.get("benchmark_source_type")
            or benchmark_profile.get("benchmark_confidence")
            or "unassigned"
        )
        candidate = dict(item)
        candidate["holdings_profile"] = holdings_profile
        directness_class = infer_candidate_directness(candidate)
        freshness_class = _freshness_class(factsheet, holdings_profile)
        fallback_state = str(holdings_profile.get("fallback_state") or "none")
        structural_limitations = list(holdings_profile.get("structural_limitations") or [])
        authority_class = classify_authority(
            directness_class,
            freshness_state=freshness_class,
            fallback_state=fallback_state,
            structure_first=directness_class == "structure_first",
        )
        coverage_class = "missing"
        best_available_source_class = _best_available_source_class(
            symbol=symbol,
            holdings_profile=holdings_profile,
            factsheet=factsheet,
            source_config=source_config,
            structural_limitations=structural_limitations,
        )
        if best_available_source_class == "direct_holdings":
            coverage_class = "direct_majority"
        elif best_available_source_class in {"factsheet_summary", "html_summary"}:
            coverage_class = "summary_majority"
        elif best_available_source_class == "proxy_only":
            coverage_class = "proxy_only"
        claim_limit_class = classify_claim_limit(
            directness_class,
            authority_class,
            freshness_state=freshness_class,
            benchmark_support_class=benchmark_support_class,
        )
        completeness_class = str(holdings_profile.get("quality_state") or factsheet.get("quality_state") or "missing")
        row = {
            "symbol": symbol,
            "sleeve": sleeve,
            "truth_family": "holdings_exposure",
            "best_available_source_class": best_available_source_class,
            "directness_class": directness_class,
            "freshness_class": freshness_class,
            "completeness_class": completeness_class,
            "fallback_state": fallback_state,
            "structural_limitation_flag": bool(structural_limitations),
            "structural_limitations": structural_limitations,
            "investor_safe_claim_status": claim_limit_class,
            "authority_class": authority_class,
            "source_registered": bool(source_config),
            "doc_registered": bool(doc_map.get(symbol)),
            "benchmark_registered": bool(benchmark_key),
            "benchmark_support_class": benchmark_support_class,
            "quality_issues": list(holdings_profile.get("quality_issues") or []),
        }
        row["status"] = _family_status(
            directness_class=directness_class,
            authority_class=authority_class,
            freshness_class=freshness_class,
            coverage_class=coverage_class,
            completeness_class=completeness_class,
            structural_limitations=structural_limitations,
        )
        row["root_cause_category"] = _root_cause_category(row)
        row["classification_rationale"] = (
            "Direct holdings truth is present and fresh enough for primary authority."
            if row["status"] == "healthy" and directness_class == "direct_holdings_backed"
            else "Direct holdings truth is present, but freshness or completeness still limits full authority."
            if directness_class == "direct_holdings_backed"
            else "Structure-first product is being judged through strategy design rather than fake holdings completeness."
            if directness_class == "structure_first"
            else "Issuer summary support is usable but still weaker than direct holdings truth."
            if directness_class in {"issuer_structured_summary_backed", "html_summary_backed"}
            else "Coverage remains proxy-backed or structurally constrained, so investor-safe claims must stay limited."
            if directness_class in {"proxy_backed", "support_only"}
            else "Truth support remains incomplete and needs engineering repair or explicit downgrade."
        )
        if row["status"] == "healthy":
            row["remediation_recommendation"] = "preserve"
        elif row["root_cause_category"] in {"registry_incompleteness", "weak_extraction"}:
            row["remediation_recommendation"] = "repair_by_engineering"
        elif row["root_cause_category"] in {"proxy_overuse", "missing_source_coverage"}:
            row["remediation_recommendation"] = "downgrade_claims_or_escalate_later"
        else:
            row["remediation_recommendation"] = "monitor"
        report_rows.append(row)
    summary = {
        "healthy": sum(1 for item in report_rows if item["status"] == "healthy"),
        "partial": sum(1 for item in report_rows if item["status"] == "partial"),
        "weak": sum(1 for item in report_rows if item["status"] == "weak"),
        "structurally_under_covered": sum(1 for item in report_rows if item["status"] == "structurally_under_covered"),
    }
    return {
        "parity": parity,
        "summary": summary,
        "rows": report_rows,
    }


def write_upstream_health_report(repo_root: Path, conn: sqlite3.Connection) -> dict[str, str]:
    report = build_upstream_health_report(conn)
    docs_dir = repo_root / "docs" / "audits"
    docs_dir.mkdir(parents=True, exist_ok=True)
    md_path = docs_dir / "UPSTREAM_HEALTH_REPORT.md"
    json_path = docs_dir / "UPSTREAM_HEALTH_REPORT.json"
    lines = [
        "# Upstream Health Report",
        "",
        f"Summary: {json.dumps(report.get('summary') or {}, sort_keys=True)}",
        "",
        "## Registry Parity",
        "",
        f"Status: {report['parity']['status']}",
        "",
    ]
    for item in report["parity"]["rows"]:
        if item["missing_registries"] or item.get("severity") not in {"ok"}:
            detail = f"missing {', '.join(item['missing_registries'])}" if item["missing_registries"] else "proxy benchmark requires stronger explicit limitation"
            lines.append(f"- {item['symbol']} ({item['sleeve']}): {item.get('severity','ok')} | {detail}")
    lines.append("")
    lines.append("## Symbol Coverage")
    lines.append("")
    for row in report["rows"]:
        lines.append(
            f"- {row['symbol']} [{row['sleeve']}]: {row['status']} | {row['best_available_source_class']} | "
            f"{row['directness_class']} | claim {row['investor_safe_claim_status']} | root cause {row['root_cause_category']} | "
            f"action {row['remediation_recommendation']} | why {row['classification_rationale']}"
        )
    md_path.write_text("\n".join(lines), encoding="utf-8")
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return {"markdown": str(md_path), "json": str(json_path)}
