from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

from app.config import get_db_path, get_repo_root
from app.services.etf_doc_parser import fetch_candidate_docs, load_doc_registry
from app.services.ingest_etf_data import get_etf_factsheet_history_summary
from app.v2.donors.source_freshness import FreshnessClass, FreshnessState
from app.v2.sources.freshness import coerce_datetime
from app.v2.sources.freshness_registry import register_source


source_tier: str = "1A"
_SOURCE_ID = "issuer_factsheet"
_FIXTURE_DIR = get_repo_root() / "backend" / "tests" / "fixtures"
_CANDIDATE_REGISTRY_PATH = get_repo_root() / "backend" / "app" / "config" / "blueprint_candidate_registry_seed.json"
ISSUER_DOC_TARGET_FIELDS: tuple[str, ...] = (
    "domicile",
    "isin",
    "launch_date",
    "primary_trading_currency",
    "expense_ratio",
    "aum",
    "benchmark_name",
    "issuer",
)


def _now() -> datetime:
    return datetime.now(UTC)


def _read_only_connection() -> sqlite3.Connection | None:
    path = get_db_path()
    if not path.exists():
        return None
    try:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    except sqlite3.Error:
        return None
    conn.row_factory = sqlite3.Row
    return conn


def _normalize_symbol(text: str) -> str:
    return str(text or "").strip().upper()


def _normalize_asset_class(sleeve_key: str | None, name: str | None) -> str | None:
    sleeve = str(sleeve_key or "").strip().lower()
    if sleeve in {
        "global_equity_core",
        "developed_ex_us_optional",
        "emerging_markets",
        "china_satellite",
    }:
        return "equity"
    if sleeve in {"ig_bonds", "cash_bills"}:
        return "fixed_income"
    if sleeve == "real_assets":
        return "real_assets"
    if sleeve == "alternatives":
        return "alternatives"
    if sleeve == "convex":
        return "hedge"

    lowered = str(name or "").lower()
    if "bond" in lowered or "treasury" in lowered:
        return "fixed_income"
    if "gold" in lowered or "commodity" in lowered or "property" in lowered or "reit" in lowered:
        return "real_assets"
    if "world" in lowered or "equity" in lowered or "msci" in lowered or "s&p" in lowered:
        return "equity"
    return None


@lru_cache(maxsize=1)
def _candidate_registry_map() -> dict[str, dict[str, Any]]:
    if not _CANDIDATE_REGISTRY_PATH.exists():
        return {}
    try:
        rows = json.loads(_CANDIDATE_REGISTRY_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return {
        _normalize_symbol(item.get("symbol")): item
        for item in rows
        if isinstance(item, dict) and _normalize_symbol(item.get("symbol"))
    }


@lru_cache(maxsize=1)
def _doc_registry() -> list[dict[str, Any]]:
    payload = load_doc_registry()
    rows = payload.get("candidates") if isinstance(payload, dict) else []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _registry_row(ticker: str) -> dict[str, Any] | None:
    normalized = _normalize_symbol(ticker)
    for item in _doc_registry():
        if _normalize_symbol(item.get("ticker")) == normalized:
            return item
    return None


def _fixture_exists(ticker: str) -> bool:
    fixture_name = f"sample_{_normalize_symbol(ticker).lower()}_factsheet.txt"
    return (_FIXTURE_DIR / fixture_name).exists()


def _safe_docs_payload(ticker: str) -> dict[str, Any]:
    if not _fixture_exists(ticker):
        return {}
    try:
        payload = fetch_candidate_docs(_normalize_symbol(ticker), use_fixtures=True)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_factsheet_summary(ticker: str) -> dict[str, Any]:
    conn = _read_only_connection()
    if conn is None:
        return {}
    try:
        payload = get_etf_factsheet_history_summary(_normalize_symbol(ticker), conn)
    except sqlite3.Error:
        payload = None
    finally:
        conn.close()
    return dict(payload) if isinstance(payload, dict) else {}


def _clean_name(parsed_name: Any, registry_name: Any) -> str | None:
    candidate = str(parsed_name or "").strip()
    if candidate and not candidate.upper().startswith("ISIN:"):
        return candidate
    fallback = str(registry_name or "").strip()
    return fallback or None


def _last_updated_utc(raw: dict[str, Any]) -> str | None:
    for key in ("factsheet_date", "factsheet_retrieved_at", "latest_asof_date"):
        value = raw.get(key)
        dt = coerce_datetime(value)
        if dt is not None:
            return dt.astimezone(UTC).isoformat()
    return None


def _freshness_from_iso(source_id: str, last_updated_utc: str | None) -> FreshnessState:
    observed_dt = coerce_datetime(last_updated_utc)
    if observed_dt is None:
        return FreshnessState(
            source_id=source_id,
            freshness_class=FreshnessClass.EXECUTION_FAILED_OR_INCOMPLETE,
            last_updated_utc=None,
            staleness_seconds=None,
        )

    age_seconds = max(0, int((_now() - observed_dt).total_seconds()))
    if age_seconds <= 30 * 24 * 60 * 60:
        freshness_class = FreshnessClass.FRESH_FULL_REBUILD
    elif age_seconds <= 120 * 24 * 60 * 60:
        freshness_class = FreshnessClass.FRESH_PARTIAL_REBUILD
    elif age_seconds <= 180 * 24 * 60 * 60:
        freshness_class = FreshnessClass.STORED_VALID_CONTEXT
    else:
        freshness_class = FreshnessClass.DEGRADED_MONITORING_MODE
    return FreshnessState(
        source_id=source_id,
        freshness_class=freshness_class,
        last_updated_utc=observed_dt.astimezone(UTC).isoformat(),
        staleness_seconds=age_seconds,
    )


def fallback() -> dict[str, Any]:
    return {"error": "factsheet_unavailable"}


def get_missing_issuer_doc_fields(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
    sleeve_key: str,
    target_fields: tuple[str, ...] | None = None,
) -> list[str]:
    fields = tuple(target_fields or ISSUER_DOC_TARGET_FIELDS)
    if not fields:
        return []
    placeholders = ",".join("?" for _ in fields)
    try:
        rows = conn.execute(
            f"""
            SELECT field_name, source_name, missingness_reason
            FROM candidate_field_current
            WHERE candidate_symbol = ? AND sleeve_key = ? AND field_name IN ({placeholders})
            """,
            (str(candidate_symbol or "").strip().upper(), str(sleeve_key or "").strip(), *fields),
        ).fetchall()
    except sqlite3.Error:
        return list(fields)
    covered: set[str] = set()
    authoritative_sources = {
        "domicile": {"issuer_doc_parser"},
        "isin": {"issuer_doc_parser"},
        "launch_date": {"issuer_doc_parser"},
        "primary_trading_currency": {"issuer_doc_parser"},
        "expense_ratio": {"issuer_doc_parser"},
        "aum": {"issuer_doc_parser", "etf_factsheet_metrics"},
        "benchmark_name": {"issuer_doc_parser"},
        "issuer": {"issuer_doc_parser"},
    }
    for row in rows:
        field_name = str(row["field_name"] or "").strip()
        source_name = str(row["source_name"] or "").strip().lower()
        missingness_reason = str(row["missingness_reason"] or "").strip()
        if missingness_reason != "populated":
            continue
        if source_name in authoritative_sources.get(field_name, {"issuer_doc_parser"}):
            covered.add(field_name)
    return [field_name for field_name in fields if field_name not in covered]


def fetch(ticker: str) -> dict[str, Any]:
    normalized = _normalize_symbol(ticker)
    registry_row = _registry_row(normalized)
    if registry_row is None:
        return {"ticker": normalized, **fallback()}

    docs_payload = _safe_docs_payload(normalized)
    extracted = dict(docs_payload.get("extracted") or {})
    summary = _safe_factsheet_summary(normalized)
    candidate_registry = _candidate_registry_map().get(normalized, {})

    name = _clean_name(extracted.get("fund_name"), registry_row.get("name"))
    factsheet_date = extracted.get("factsheet_date") or summary.get("latest_asof_date")
    factsheet_retrieved_at = (
        dict(docs_payload.get("factsheet") or {}).get("retrieved_at")
        or dict(summary.get("citation") or {}).get("retrieved_at")
    )
    primary_documents = []
    for doc_key in ("factsheet", "kid", "prospectus"):
        doc_payload = dict(docs_payload.get(doc_key) or {})
        registered_url = str(dict(registry_row.get("docs") or {}).get(f"{doc_key}_pdf_url") or "").strip() or None
        doc_url = str(doc_payload.get("doc_url") or registered_url or "").strip() or None
        if not doc_url:
            continue
        cache_file = str(doc_payload.get("cache_file") or "").strip() or None
        status = str(doc_payload.get("status") or ("registered_unverified" if registered_url and not doc_payload else "unknown"))
        fingerprint_seed = json.dumps(
            {
                "ticker": normalized,
                "doc_type": doc_key,
                "doc_url": doc_url,
                "cache_file": cache_file,
                "status": status,
                "retrieved_at": doc_payload.get("retrieved_at"),
            },
            sort_keys=True,
            ensure_ascii=True,
        )
        primary_documents.append(
            {
                "doc_type": doc_key,
                "doc_url": doc_url,
                "status": status,
                "retrieved_at": doc_payload.get("retrieved_at"),
                "authority_class": "issuer_primary" if doc_key == "prospectus" else "issuer_secondary",
                "cache_file": cache_file,
                "document_fingerprint": hashlib.sha1(fingerprint_seed.encode("utf-8")).hexdigest()[:16],
            }
        )

    raw = {
        "ticker": normalized,
        "name": name,
        "asset_class": _normalize_asset_class(candidate_registry.get("sleeve_key"), name),
        "issuer": extracted.get("issuer") or registry_row.get("issuer"),
        "benchmark_id": candidate_registry.get("benchmark_key"),
        "benchmark_name": extracted.get("benchmark_name"),
        "ter": extracted.get("ter"),
        "aum_usd": extracted.get("aum_usd") if extracted.get("aum_usd") is not None else summary.get("latest_aum_usd"),
        "launch_date": extracted.get("launch_date") or extracted.get("inception_date"),
        "inception_date": extracted.get("inception_date"),
        "vehicle_type": extracted.get("wrapper_or_vehicle_type"),
        "domicile": extracted.get("domicile") or candidate_registry.get("domicile"),
        "isin": extracted.get("isin") or registry_row.get("expected_isin"),
        "primary_listing_exchange": extracted.get("primary_listing_exchange"),
        "base_currency": extracted.get("primary_trading_currency"),
        "expense_ratio": extracted.get("ter"),
        "factsheet_date": factsheet_date,
        "factsheet_retrieved_at": factsheet_retrieved_at,
        "expected_isin": registry_row.get("expected_isin"),
        "docs": dict(registry_row.get("docs") or {}),
        "primary_documents": primary_documents,
        "verification_missing": list(docs_payload.get("verification_missing") or []),
        "registry_name": registry_row.get("name"),
        "summary": summary,
        "source_tier": source_tier,
    }
    return raw


def fetch_all() -> list[dict[str, Any]]:
    return [fetch(str(item.get("ticker") or "")) for item in _doc_registry()]


def freshness_state() -> FreshnessState:
    latest_dt: datetime | None = None
    for item in fetch_all():
        observed = coerce_datetime(_last_updated_utc(item))
        if observed is None:
            continue
        latest_dt = observed if latest_dt is None or observed > latest_dt else latest_dt
    last_updated_utc = latest_dt.astimezone(UTC).isoformat() if latest_dt is not None else None
    return _freshness_from_iso(_SOURCE_ID, last_updated_utc)


class IssuerFactsheetAdapter:
    source_id = _SOURCE_ID
    tier = source_tier

    def fetch(self, ticker: str) -> dict[str, Any]:
        return fetch(ticker)

    def fetch_all(self) -> list[dict[str, Any]]:
        return fetch_all()

    def freshness_state(self) -> FreshnessState:
        return freshness_state()


register_source(_SOURCE_ID, adapter=__import__(__name__, fromlist=["fetch"]))
