from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from app.services.blueprint_benchmark_registry import (
    build_benchmark_registry_summary,
    list_benchmark_registry,
    resolve_benchmark_assignment,
)
from app.services.blueprint_candidate_registry import export_live_candidate_registry
from app.services.blueprint_candidate_truth import (
    compute_candidate_completeness,
    list_required_fields,
    resolve_candidate_field_truth,
)
from app.services.etf_doc_parser import fetch_candidate_docs
from app.services.ingest_etf_data import (
    get_etf_factsheet_history_summary,
    get_etf_holdings_profile,
    get_etf_source_config,
    get_preferred_latest_market_data,
    get_preferred_market_history_summary,
)


@dataclass(slots=True)
class SQLiteBlueprintDonor:
    """Read-only Layer 1 donor over legacy blueprint services."""

    conn: sqlite3.Connection

    def list_candidates(self) -> list[dict[str, Any]]:
        return export_live_candidate_registry(self.conn)

    def list_required_fields(self, sleeve_key: str) -> list[dict[str, Any]]:
        return list_required_fields(self.conn, sleeve_key)

    def resolve_field_truth(self, *, candidate_symbol: str, sleeve_key: str) -> dict[str, dict[str, Any]]:
        return resolve_candidate_field_truth(
            self.conn,
            candidate_symbol=candidate_symbol,
            sleeve_key=sleeve_key,
        )

    def compute_candidate_completeness(self, *, candidate: dict[str, Any]) -> dict[str, Any]:
        return compute_candidate_completeness(self.conn, candidate=candidate)

    def resolve_benchmark_assignment(self, *, candidate: dict[str, Any], sleeve_key: str) -> dict[str, Any]:
        return resolve_benchmark_assignment(self.conn, candidate=candidate, sleeve_key=sleeve_key)

    def list_benchmark_registry(self) -> list[dict[str, Any]]:
        return list_benchmark_registry(self.conn)

    def build_benchmark_registry_summary(self, *, sleeves: list[dict[str, Any]]) -> dict[str, Any]:
        return build_benchmark_registry_summary(self.conn, sleeves=sleeves)


@dataclass(slots=True)
class SQLiteEtfDonor:
    """ETF-oriented donor wrapper for source config, docs, and persisted market state."""

    conn: sqlite3.Connection

    def get_source_config(self, symbol: str) -> dict[str, Any] | None:
        payload = get_etf_source_config(symbol)
        return dict(payload) if payload is not None else None

    def get_document_record(self, symbol: str, *, use_fixtures: bool = False) -> dict[str, Any]:
        return fetch_candidate_docs(symbol, use_fixtures=use_fixtures)

    def get_holdings_profile(self, symbol: str) -> dict[str, Any] | None:
        payload = get_etf_holdings_profile(symbol, self.conn)
        return dict(payload) if payload is not None else None

    def get_factsheet_summary(self, symbol: str) -> dict[str, Any] | None:
        payload = get_etf_factsheet_history_summary(symbol, self.conn)
        return dict(payload) if payload is not None else None

    def get_market_summary(self, symbol: str) -> dict[str, Any] | None:
        payload = get_preferred_market_history_summary(symbol, self.conn)
        return dict(payload) if payload is not None else None

    def get_latest_market_data(self, symbol: str) -> dict[str, Any] | None:
        payload = get_preferred_latest_market_data(symbol, self.conn)
        return dict(payload) if payload is not None else None
