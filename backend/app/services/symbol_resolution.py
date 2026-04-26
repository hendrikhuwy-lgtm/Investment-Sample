from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any

from app.services.blueprint_candidate_registry import export_live_candidate_registry


DEFAULT_PROVIDER_SUFFIXES: dict[str, list[str]] = {
    "eodhd": [".US", ".LSE", ".SW", ".PA", ".AS", ".SG"],
    "polygon": ["", ".US"],
    "tiingo": [".LSE", ".SW", ".PA", ".AS", ".SG", "", ".US"],
    "twelve_data": [".LSE", ".SW", ".PA", ".AS", ".SG", "", ".US"],
    "yahoo_finance": [".LSE", ".SW", ".PA", ".AS", ".SG", "", ".US"],
    "fmp": ["", ".US"],
    "finnhub": ["", ".US"],
}

KNOWN_SYMBOL_ALIASES: dict[str, list[str]] = {
    "A35": ["A35.SG"],
    "IWDP": ["IWDP.LSE"],
    "IWDA": ["IWDA.LSE"],
    "VWRA": ["VWRA.LSE"],
    "VWRL": ["VWRL.LSE"],
    "EIMI": ["EIMI.LSE"],
    "IB01": ["IB01.LSE"],
    "SGLN": ["SGLN.LSE"],
    "CMOD": ["CMOD.LSE"],
}

KNOWN_PROVIDER_ROUTE_HINTS: dict[tuple[str, str], dict[str, Any]] = {
    ("A35", "twelve_data"): {"provider_alias": "A35.SG", "resolution_reason": "sgx_exchange_qualified_preferred"},
    ("A35", "yahoo_finance"): {"provider_alias": "A35.SG", "resolution_reason": "sgx_exchange_qualified_preferred"},
    ("A35", "tiingo"): {"provider_alias": "A35.SG", "resolution_reason": "sgx_exchange_qualified_preferred"},
    ("A35", "eodhd"): {"provider_alias": "A35.SG", "resolution_reason": "sgx_exchange_qualified_preferred"},
    ("IWDP", "twelve_data"): {"provider_alias": "IWDP.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("IWDP", "yahoo_finance"): {"provider_alias": "IWDP.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("IWDP", "tiingo"): {"provider_alias": "IWDP.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("IWDP", "eodhd"): {"provider_alias": "IWDP.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("IWDA", "twelve_data"): {"provider_alias": "IWDA.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("VWRA", "twelve_data"): {"provider_alias": "VWRA.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("VWRL", "twelve_data"): {"provider_alias": "VWRL.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("SGLN", "twelve_data"): {"provider_alias": "SGLN.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("CMOD", "twelve_data"): {"provider_alias": "CMOD.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("IWDA", "yahoo_finance"): {"provider_alias": "IWDA.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("VWRA", "yahoo_finance"): {"provider_alias": "VWRA.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("VWRL", "yahoo_finance"): {"provider_alias": "VWRL.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("SGLN", "yahoo_finance"): {"provider_alias": "SGLN.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("CMOD", "yahoo_finance"): {"provider_alias": "CMOD.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("IWDA", "tiingo"): {"provider_alias": "IWDA.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("VWRA", "tiingo"): {"provider_alias": "VWRA.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("VWRL", "tiingo"): {"provider_alias": "VWRL.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("SGLN", "tiingo"): {"provider_alias": "SGLN.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
    ("CMOD", "tiingo"): {"provider_alias": "CMOD.LSE", "resolution_reason": "ucits_exchange_qualified_preferred"},
}

_EXCHANGE_SUFFIX_REGION: dict[str, str] = {
    "US": "us",
    "LSE": "non_us",
    "SW": "non_us",
    "PA": "non_us",
    "AS": "non_us",
    "SG": "non_us",
}

_FIAT_QUOTES = {"USD", "EUR", "GBP", "JPY", "SGD", "AUD", "CAD", "CHF", "HKD", "CNH", "CNY"}


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _symbol_profile(canonical_symbol: str, provider_symbol: str) -> dict[str, Any]:
    canonical = str(canonical_symbol or "").strip().upper()
    provider_value = str(provider_symbol or canonical).strip().upper()
    aliases = [str(item).strip().upper() for item in list(KNOWN_SYMBOL_ALIASES.get(canonical) or []) if str(item).strip()]
    venue_code = provider_value.rsplit(".", 1)[1] if "." in provider_value else next((alias.rsplit(".", 1)[1] for alias in aliases if "." in alias), None)
    region = _EXCHANGE_SUFFIX_REGION.get(str(venue_code or "").upper(), "us")
    if "/" in canonical:
        symbol_family = "fx_pair"
        identifier_kind = "currency_pair"
        cadence_expectation = "intraday_quote"
    elif canonical.endswith("=F"):
        symbol_family = "futures"
        identifier_kind = "futures_symbol"
        cadence_expectation = "daily_close"
    elif canonical.startswith("^"):
        symbol_family = "index"
        identifier_kind = "index_symbol"
        cadence_expectation = "daily_close"
    elif canonical == "DXY":
        symbol_family = "dollar_index"
        identifier_kind = "macro_index"
        cadence_expectation = "daily_reference"
    elif "-" in canonical and canonical.rsplit("-", 1)[1] in _FIAT_QUOTES:
        symbol_family = "crypto"
        identifier_kind = "crypto_pair"
        cadence_expectation = "intraday_quote"
    else:
        symbol_family = "security"
        identifier_kind = "exchange_qualified_symbol" if "." in provider_value else "symbol"
        cadence_expectation = "reference_snapshot"
    return {
        "symbol_family": symbol_family,
        "identifier_kind": identifier_kind,
        "listing_region": region,
        "venue_code": str(venue_code or "").upper() or None,
        "is_proxy": canonical in {"DXY"} or canonical.startswith("^990"),
        "is_synthetic": canonical in {"DXY"},
        "cadence_expectation": cadence_expectation,
    }


def ensure_symbol_resolution_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_resolution_registry (
          canonical_symbol TEXT NOT NULL,
          provider_name TEXT NOT NULL,
          endpoint_family TEXT NOT NULL,
          provider_symbol TEXT NOT NULL,
          exchange_suffix TEXT,
          asset_type TEXT,
          region TEXT,
          primary_listing TEXT,
          fallback_aliases_json TEXT NOT NULL DEFAULT '[]',
          direct_symbol TEXT,
          exchange_qualified_symbol TEXT,
          provider_alias TEXT,
          manual_override TEXT,
          verification_source TEXT,
          resolution_confidence REAL NOT NULL DEFAULT 0.5,
          resolution_reason TEXT,
          last_verified_at TEXT NOT NULL,
          PRIMARY KEY (canonical_symbol, provider_name, endpoint_family)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS symbol_resolution_failures (
          canonical_symbol TEXT NOT NULL,
          provider_name TEXT NOT NULL,
          endpoint_family TEXT NOT NULL,
          provider_symbol TEXT NOT NULL,
          error_class TEXT NOT NULL,
          failure_count INTEGER NOT NULL DEFAULT 1,
          first_failed_at TEXT NOT NULL,
          last_failed_at TEXT NOT NULL,
          last_success_at TEXT,
          disabled_until TEXT,
          PRIMARY KEY (canonical_symbol, provider_name, endpoint_family, provider_symbol, error_class)
        )
        """
    )
    columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(symbol_resolution_registry)")}
    if "direct_symbol" not in columns:
        conn.execute("ALTER TABLE symbol_resolution_registry ADD COLUMN direct_symbol TEXT")
    if "exchange_qualified_symbol" not in columns:
        conn.execute("ALTER TABLE symbol_resolution_registry ADD COLUMN exchange_qualified_symbol TEXT")
    if "provider_alias" not in columns:
        conn.execute("ALTER TABLE symbol_resolution_registry ADD COLUMN provider_alias TEXT")
    if "manual_override" not in columns:
        conn.execute("ALTER TABLE symbol_resolution_registry ADD COLUMN manual_override TEXT")
    if "verification_source" not in columns:
        conn.execute("ALTER TABLE symbol_resolution_registry ADD COLUMN verification_source TEXT")
    conn.commit()


def seed_symbol_resolution_registry(conn: sqlite3.Connection) -> None:
    ensure_symbol_resolution_tables(conn)
    for item in export_live_candidate_registry(conn):
        symbol = str(item.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        aliases = list(KNOWN_SYMBOL_ALIASES.get(symbol) or [])
        exchange_qualified_symbol = next((alias for alias in aliases if "." in str(alias)), None)
        primary_listing = str(item.get("primary_listing_exchange") or item.get("exchange") or "").strip().upper() or None
        for provider_name in DEFAULT_PROVIDER_SUFFIXES.keys():
            row = conn.execute(
                """
                SELECT 1 FROM symbol_resolution_registry
                WHERE canonical_symbol = ? AND provider_name = ? AND endpoint_family = 'reference_meta'
                LIMIT 1
                """,
                (symbol, provider_name),
            ).fetchone()
            if row is not None:
                continue
            provider_symbol = symbol
            if provider_name in {"eodhd", "tiingo", "twelve_data"} and "." not in symbol and aliases:
                provider_symbol = aliases[0]
            conn.execute(
                """
                INSERT OR IGNORE INTO symbol_resolution_registry (
                  canonical_symbol, provider_name, endpoint_family, provider_symbol,
                  exchange_suffix, asset_type, region, primary_listing, fallback_aliases_json,
                  direct_symbol, exchange_qualified_symbol, provider_alias, manual_override,
                  verification_source, resolution_confidence, resolution_reason, last_verified_at
                ) VALUES (?, ?, 'reference_meta', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    symbol,
                    provider_name,
                    provider_symbol,
                    provider_symbol.split(".", 1)[1] if "." in provider_symbol else None,
                    item.get("asset_class"),
                    item.get("domicile"),
                    primary_listing,
                    json.dumps(aliases, ensure_ascii=True, sort_keys=True),
                    symbol,
                    exchange_qualified_symbol,
                    provider_symbol if provider_symbol != symbol else None,
                    None,
                    "blueprint_candidate_registry_seed",
                    0.85 if aliases else 0.6,
                    "seeded_from_candidate_registry",
                    _now_iso(),
                ),
            )
    conn.commit()


def resolve_provider_identifiers(
    conn: sqlite3.Connection,
    *,
    provider_name: str,
    endpoint_family: str,
    identifier: str,
    asset_type: str | None = None,
    region: str | None = None,
) -> dict[str, Any]:
    ensure_symbol_resolution_tables(conn)
    seed_symbol_resolution_registry(conn)
    canonical = str(identifier or "").strip().upper()

    # Yahoo supports caret-index symbols directly. Do not allow ETF or exchange-suffixed
    # fallbacks to override the canonical index symbol for these families.
    if provider_name == "yahoo_finance" and canonical.startswith("^") and endpoint_family in {
        "market_close",
        "benchmark_proxy",
        "quote_latest",
    }:
        symbol_profile_summary = _symbol_profile(canonical, canonical)
        return {
            "canonical_symbol": canonical,
            "provider_name": provider_name,
            "endpoint_family": endpoint_family,
            "provider_symbol": canonical,
            "direct_symbol": canonical,
            "exchange_qualified_symbol": None,
            "provider_alias": None,
            "manual_override": None,
            "verification_source": "direct_symbol_pinned",
            "fallback_aliases": [],
            "resolution_confidence": 0.95,
            "resolution_reason": "direct_index_symbol_pinned",
            "asset_type": asset_type,
            "region": region,
            "provider_identifier_strategy": "direct_symbol",
            "symbol_profile_summary": symbol_profile_summary,
        }

    aliases: list[str] = []
    row = conn.execute(
        """
        SELECT * FROM symbol_resolution_registry
        WHERE canonical_symbol = ? AND provider_name = ? AND endpoint_family IN (?, 'reference_meta')
        ORDER BY CASE WHEN endpoint_family = ? THEN 0 ELSE 1 END
        LIMIT 1
        """,
        (canonical, provider_name, endpoint_family, endpoint_family),
    ).fetchone()
    provider_symbol = canonical
    confidence = 0.5
    reason = "direct_symbol"
    direct_symbol = canonical
    exchange_qualified_symbol = None
    provider_alias = None
    manual_override = None
    verification_source = "direct_symbol"
    if row is not None:
        item = dict(row)
        manual_override = str(item.get("manual_override") or "").strip() or None
        provider_alias = str(item.get("provider_alias") or "").strip() or None
        direct_symbol = str(item.get("direct_symbol") or canonical)
        exchange_qualified_symbol = str(item.get("exchange_qualified_symbol") or "").strip() or None
        verification_source = str(item.get("verification_source") or "symbol_resolution_registry").strip() or "symbol_resolution_registry"
        provider_symbol = str(manual_override or provider_alias or item.get("provider_symbol") or canonical)
        aliases.extend(json.loads(str(item.get("fallback_aliases_json") or "[]")))
        confidence = float(item.get("resolution_confidence") or 0.5)
        reason = str(item.get("resolution_reason") or "registry_match")
    route_hint = dict(KNOWN_PROVIDER_ROUTE_HINTS.get((canonical, provider_name), {}) or {})
    preferred_route_symbol = None
    if route_hint:
        provider_alias = str(route_hint.get("provider_alias") or provider_alias or "").strip() or provider_alias
        if provider_alias:
            provider_symbol = provider_alias
            preferred_route_symbol = provider_alias.upper()
        reason = str(route_hint.get("resolution_reason") or reason or "route_hint")
        confidence = max(confidence, 0.88)
    known_aliases = list(KNOWN_SYMBOL_ALIASES.get(canonical) or [])
    aliases.extend(known_aliases)
    if "." not in canonical:
        aliases.extend(f"{canonical}{suffix}" for suffix in DEFAULT_PROVIDER_SUFFIXES.get(provider_name, []) if suffix)
    seen: set[str] = set()
    ordered = []
    preferred_candidates: list[str] = []
    if exchange_qualified_symbol and provider_name in {"twelve_data", "tiingo", "eodhd", "yahoo_finance"}:
        preferred_candidates.append(exchange_qualified_symbol)
    if provider_name in {"tiingo", "eodhd"} and endpoint_family in {"reference_meta", "ohlcv_history", "benchmark_proxy"}:
        preferred_candidates.extend(known_aliases)
    preferred_candidates.extend([provider_symbol, canonical, *aliases])
    for candidate in preferred_candidates:
        value = str(candidate or "").strip().upper()
        if not value or value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    failure_rows = conn.execute(
        """
        SELECT provider_symbol, error_class, failure_count, disabled_until
        FROM symbol_resolution_failures
        WHERE canonical_symbol = ? AND provider_name = ? AND endpoint_family = ?
        """,
        (canonical, provider_name, endpoint_family),
    ).fetchall()
    failure_map = {str(row["provider_symbol"] or "").upper(): dict(row) for row in failure_rows}

    symbol_profile_summary = _symbol_profile(canonical, provider_symbol)

    def _candidate_rank(value: str) -> tuple[int, int, int]:
        route_hint_penalty = 0 if preferred_route_symbol and value == preferred_route_symbol else 1
        row = failure_map.get(value) or {}
        disabled_until = str(row.get("disabled_until") or "")
        is_disabled = 0
        if disabled_until:
            try:
                is_disabled = 1 if datetime.fromisoformat(disabled_until.replace("Z", "+00:00")) > datetime.now(UTC) else 0
            except Exception:
                is_disabled = 0
        bare_non_us_penalty = 0
        if (
            provider_name in {"finnhub", "fmp"}
            and str(symbol_profile_summary.get("listing_region") or "") == "non_us"
            and exchange_qualified_symbol
            and value == canonical
        ):
            bare_non_us_penalty = 1
        return (route_hint_penalty, is_disabled, bare_non_us_penalty, int(row.get("failure_count") or 0))

    ordered.sort(key=_candidate_rank)
    selected_provider_symbol = ordered[0] if ordered else provider_symbol
    if selected_provider_symbol and selected_provider_symbol != provider_symbol:
        provider_symbol = selected_provider_symbol
        if provider_symbol == exchange_qualified_symbol and not manual_override and not provider_alias:
            reason = "exchange_qualified_alias"
            confidence = max(confidence, 0.9)
        elif not manual_override:
            reason = "route_candidate_promoted"
            confidence = max(confidence, 0.8)
    provider_identifier_strategy = (
        "manual_override"
        if manual_override
        else "provider_alias"
        if provider_alias
        else "exchange_qualified_alias"
        if "." in str(provider_symbol or "")
        else "direct_symbol"
    )
    return {
        "canonical_symbol": canonical,
        "provider_name": provider_name,
        "endpoint_family": endpoint_family,
        "provider_symbol": provider_symbol,
        "direct_symbol": direct_symbol,
        "exchange_qualified_symbol": exchange_qualified_symbol,
        "provider_alias": provider_alias,
        "manual_override": manual_override,
        "verification_source": verification_source,
        "fallback_aliases": ordered[1:],
        "resolution_confidence": confidence,
        "resolution_reason": reason,
        "asset_type": asset_type,
        "region": region,
        "provider_identifier_strategy": provider_identifier_strategy,
        "symbol_profile_summary": symbol_profile_summary,
    }


def record_resolution_success(
    conn: sqlite3.Connection,
    *,
    canonical_symbol: str,
    provider_name: str,
    endpoint_family: str,
    provider_symbol: str,
    fallback_aliases: list[str] | None = None,
    resolution_confidence: float = 0.75,
    resolution_reason: str = "verified_runtime",
) -> None:
    ensure_symbol_resolution_tables(conn)
    conn.execute(
        """
        INSERT OR REPLACE INTO symbol_resolution_registry (
          canonical_symbol, provider_name, endpoint_family, provider_symbol,
          exchange_suffix, asset_type, region, primary_listing, fallback_aliases_json,
          direct_symbol, exchange_qualified_symbol, provider_alias, manual_override,
          verification_source, resolution_confidence, resolution_reason, last_verified_at
        ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, ?, ?, ?, ?, NULL, ?, ?, ?, ?)
        """,
        (
            canonical_symbol.upper(),
            provider_name,
            endpoint_family,
            provider_symbol.upper(),
            provider_symbol.split(".", 1)[1] if "." in provider_symbol else None,
            json.dumps(list(fallback_aliases or []), ensure_ascii=True, sort_keys=True),
            canonical_symbol.upper(),
            provider_symbol.upper() if "." in provider_symbol else None,
            provider_symbol.upper() if provider_symbol.upper() != canonical_symbol.upper() else None,
            "runtime_verified",
            float(resolution_confidence),
            resolution_reason,
            _now_iso(),
        ),
    )
    conn.execute(
        """
        UPDATE symbol_resolution_failures
        SET last_success_at = ?, disabled_until = NULL
        WHERE canonical_symbol = ? AND provider_name = ? AND endpoint_family = ? AND provider_symbol = ?
        """,
        (
            _now_iso(),
            canonical_symbol.upper(),
            provider_name,
            endpoint_family,
            provider_symbol.upper(),
        ),
    )
    conn.commit()


def record_resolution_failure(
    conn: sqlite3.Connection,
    *,
    canonical_symbol: str,
    provider_name: str,
    endpoint_family: str,
    provider_symbol: str,
    error_class: str,
) -> None:
    ensure_symbol_resolution_tables(conn)
    existing = conn.execute(
        """
        SELECT failure_count
        FROM symbol_resolution_failures
        WHERE canonical_symbol = ? AND provider_name = ? AND endpoint_family = ? AND provider_symbol = ? AND error_class = ?
        """,
        (canonical_symbol.upper(), provider_name, endpoint_family, provider_symbol.upper(), error_class),
    ).fetchone()
    failure_count = int(existing["failure_count"] or 0) + 1 if existing is not None else 1
    disabled_until = None
    if error_class in {"symbol_gap", "missing_source_gap", "not_found", "empty_response"} and failure_count >= 3:
        disabled_until = (datetime.now(UTC) + timedelta(hours=24)).replace(microsecond=0).isoformat()
    conn.execute(
        """
        INSERT INTO symbol_resolution_failures (
          canonical_symbol, provider_name, endpoint_family, provider_symbol, error_class,
          failure_count, first_failed_at, last_failed_at, last_success_at, disabled_until
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
        ON CONFLICT(canonical_symbol, provider_name, endpoint_family, provider_symbol, error_class)
        DO UPDATE SET
          failure_count = symbol_resolution_failures.failure_count + 1,
          last_failed_at = excluded.last_failed_at,
          disabled_until = excluded.disabled_until
        """,
        (
            canonical_symbol.upper(),
            provider_name,
            endpoint_family,
            provider_symbol.upper(),
            error_class,
            failure_count,
            _now_iso(),
            _now_iso(),
            disabled_until,
        ),
    )
    conn.commit()
