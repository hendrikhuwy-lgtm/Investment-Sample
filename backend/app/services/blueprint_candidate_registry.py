from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.config import get_repo_root, get_settings
from app.services.blueprint_candidate_truth import (
    _observation_reconciled_out,
    compute_candidate_completeness,
    ensure_candidate_truth_tables,
    reconcile_field_observations,
    resolve_candidate_field_truth,
    seed_required_field_matrix,
    upsert_field_observation,
)
from app.services.blueprint_benchmark_registry import canonical_benchmark_full_name, resolve_benchmark_assignment
from app.services.etf_doc_parser import fetch_and_parse_etf_doc, fetch_candidate_docs, load_doc_registry
from app.services.ingest_etf_data import (
    get_etf_factsheet_history_summary,
    get_etf_holdings_profile,
    get_latest_successful_etf_ingest_at,
    get_latest_etf_fetch_status,
    get_preferred_market_exchange,
    get_preferred_market_history_summary,
    get_preferred_latest_market_data,
    get_etf_source_config,
)
from app.services.tax_engine import build_sg_tax_truth


SEED_PATH = get_repo_root() / "backend" / "app" / "config" / "blueprint_candidate_registry_seed.json"
ACTIVE_UNIVERSE_PATH = get_repo_root() / "backend" / "app" / "config" / "blueprint_active_candidate_universe.json"

LIVE_OBJECT_TYPE = "live_fund_candidate"
POLICY_PLACEHOLDER_TYPE = "policy_placeholder"
STRATEGY_PLACEHOLDER_TYPE = "strategy_placeholder"
MANUAL_SEED_STATE = "manual_seed"
SOURCE_VALIDATED_STATE = "source_validated"
AGING_STATE = "aging"
STALE_LIVE_STATE = "stale_live"
BROKEN_SOURCE_STATE = "broken_source"

POLICY_PLACEHOLDER_SYMBOLS = {
    "SGD_CASH_RESERVE",
    "SGD_MMF_POLICY",
    "SG_TBILL_POLICY",
    "UCITS_MMF_PLACEHOLDER",
}
STRATEGY_PLACEHOLDER_SYMBOLS = {"SPX_LONG_PUT"}
_ISIN_PREFIX_TO_COUNTRY = {
    "IE": "IRELAND",
    "LU": "LUXEMBOURG",
    "SG": "SINGAPORE",
    "US": "UNITED STATES",
    "GB": "UNITED KINGDOM",
}

DEFAULT_ACTIVE_UNIVERSE_CONFIG: dict[str, Any] = {
    "selection_mode": "all_live_candidates_except_excluded",
    "include_symbols": [],
    "exclude_symbols": {
        "UCITS_MMF_PLACEHOLDER": "policy_placeholder",
        "SG_TBILL_POLICY": "policy_placeholder",
        "SGD_MMF_POLICY": "policy_placeholder",
        "SGD_CASH_RESERVE": "policy_placeholder",
        "SPX_LONG_PUT": "strategy_placeholder",
    },
}

_TARGETED_SOURCE_COMPLETION_SOURCE = "supplemental_candidate_metrics"
_TARGETED_SOURCE_COMPLETION_OVERRIDES: dict[tuple[str, str], tuple[dict[str, Any], ...]] = {
    ("global_equity_core", "CSPX"): (
        {
            "field_name": "benchmark_name",
            "value": "S&P 500 Index",
            "observed_at": "2026-04-21",
            "source_url": "https://www.spglobal.com/spdji/en/indices/equity/sp-500/",
            "reason": "Reviewed benchmark lineage override for CSPX.",
        },
    ),
    ("global_equity_core", "IWDA"): (
        {
            "field_name": "primary_trading_currency",
            "value": "USD",
            "observed_at": "2026-04-21",
            "source_url": "https://finance.yahoo.com/quote/IWDA.L",
            "reason": "Reviewed exchange-qualified LSE trading currency for IWDA.",
        },
    ),
    ("developed_ex_us_optional", "IWDA"): (
        {
            "field_name": "primary_trading_currency",
            "value": "USD",
            "observed_at": "2026-04-21",
            "source_url": "https://finance.yahoo.com/quote/IWDA.L",
            "reason": "Reviewed exchange-qualified LSE trading currency for IWDA.",
        },
    ),
    ("global_equity_core", "VWRL"): (
        {
            "field_name": "primary_trading_currency",
            "value": "USD",
            "observed_at": "2026-04-21",
            "source_url": "https://finance.yahoo.com/quote/VWRL.L",
            "reason": "Reviewed exchange-qualified LSE trading currency for VWRL.",
        },
    ),
    ("ig_bonds", "A35"): (
        {
            "field_name": "bid_ask_spread_proxy",
            "value": 24.0,
            "observed_at": "2026-04-21",
            "source_url": "https://sg.amova-am.com/general/funds/detail/abf-singapore-bond-index-fund",
            "reason": "Reviewed bounded spread proxy for A35 pending direct quote-family completion.",
        },
    ),
    ("real_assets", "CMOD"): (
        {
            "field_name": "benchmark_name",
            "value": "Bloomberg Commodity Index",
            "observed_at": "2026-04-21",
            "source_url": "https://www.invesco.com/content/dam/invesco/uk/en/product-documents/etf/share-class/factsheet/IE00BD6FTQ80_factsheet_en-uk.pdf",
            "reason": "Reviewed benchmark lineage override for CMOD.",
        },
        {
            "field_name": "aum",
            "value": 4690120000.0,
            "observed_at": "2026-03-31",
            "source_url": "https://www.invesco.com/content/dam/invesco/uk/en/product-documents/etf/share-class/factsheet/IE00BD6FTQ80_factsheet_en-uk.pdf",
            "reason": "Reviewed fund size override for CMOD from official factsheet.",
        },
    ),
    ("alternatives", "CMOD"): (
        {
            "field_name": "benchmark_name",
            "value": "Bloomberg Commodity Index",
            "observed_at": "2026-04-21",
            "source_url": "https://www.invesco.com/content/dam/invesco/uk/en/product-documents/etf/share-class/factsheet/IE00BD6FTQ80_factsheet_en-uk.pdf",
            "reason": "Reviewed benchmark lineage override for CMOD.",
        },
        {
            "field_name": "aum",
            "value": 4690120000.0,
            "observed_at": "2026-03-31",
            "source_url": "https://www.invesco.com/content/dam/invesco/uk/en/product-documents/etf/share-class/factsheet/IE00BD6FTQ80_factsheet_en-uk.pdf",
            "reason": "Reviewed fund size override for CMOD from official factsheet.",
        },
    ),
    ("real_assets", "SGLN"): (
        {
            "field_name": "primary_trading_currency",
            "value": "GBP",
            "observed_at": "2026-04-21",
            "source_url": "https://finance.yahoo.com/quote/SGLN.L",
            "reason": "Reviewed exchange-qualified LSE trading currency for SGLN.",
        },
    ),
    ("china_satellite", "HMCH"): (
        {
            "field_name": "primary_trading_currency",
            "value": "GBP",
            "observed_at": "2026-04-21",
            "source_url": "https://finance.yahoo.com/quote/HMCH.L",
            "reason": "Reviewed exchange-qualified LSE trading currency for HMCH.",
        },
        {
            "field_name": "replication_method",
            "value": "Physical-Full",
            "observed_at": "2026-04-21",
            "source_url": "https://www.ishares.com/uk/individual/en/products/251859/ishares-msci-china-ucits-etf",
            "reason": "Reviewed replication-method override for HMCH from official product materials.",
        },
    ),
    ("convex", "CAOS"): (
        {
            "field_name": "benchmark_key",
            "value": "SP500",
            "observed_at": "2026-04-21",
            "source_url": "https://funds.alphaarchitect.com/caos/",
            "reason": "Reviewed benchmark-key proxy mapping for CAOS tail-risk mandate.",
        },
    ),
    ("convex", "DBMF"): (
        {
            "field_name": "benchmark_key",
            "value": "SG_CTA",
            "observed_at": "2026-04-21",
            "source_url": "https://imgpfunds.com/im-dbi-managed-futures-strategy-etf",
            "reason": "Reviewed benchmark-key proxy mapping for DBMF managed-futures sleeve role.",
        },
        {
            "field_name": "benchmark_name",
            "value": "SG CTA Index",
            "observed_at": "2026-04-21",
            "source_url": "https://imgpfunds.com/im-dbi-managed-futures-strategy-etf",
            "reason": "Reviewed benchmark-name proxy mapping for DBMF managed-futures sleeve role.",
        },
    ),
    ("convex", "KMLM"): (
        {
            "field_name": "benchmark_key",
            "value": "KFA_MLM",
            "observed_at": "2026-04-21",
            "source_url": "https://kfafunds.com/resources/factsheet/2023_04_30_kmlm_factsheet.pdf",
            "reason": "Reviewed benchmark-key mapping for KMLM from official factsheet.",
        },
        {
            "field_name": "benchmark_name",
            "value": "KFA MLM Index",
            "observed_at": "2026-04-21",
            "source_url": "https://kfafunds.com/resources/factsheet/2023_04_30_kmlm_factsheet.pdf",
            "reason": "Reviewed benchmark-name mapping for KMLM from official factsheet.",
        },
    ),
    ("convex", "TAIL"): (
        {
            "field_name": "benchmark_key",
            "value": "SHORT_TREASURY",
            "observed_at": "2026-04-21",
            "source_url": "https://www.cambriafunds.com/assets/docs/Cambria_Annual_Final.pdf",
            "reason": "Reviewed benchmark-key proxy mapping for TAIL based on stated suitable benchmark.",
        },
        {
            "field_name": "benchmark_name",
            "value": "Bloomberg Barclays Short Treasury Index",
            "observed_at": "2026-04-21",
            "source_url": "https://www.cambriafunds.com/assets/docs/Cambria_Annual_Final.pdf",
            "reason": "Reviewed benchmark-name proxy mapping for TAIL based on stated suitable benchmark.",
        },
    ),
}


def apply_targeted_source_completion_overrides(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str | None = None,
    sleeve_key: str | None = None,
    recovery_run_id: str | None = None,
) -> dict[str, Any]:
    matched = 0
    updated = 0
    for (target_sleeve, target_symbol), overrides in _TARGETED_SOURCE_COMPLETION_OVERRIDES.items():
        if sleeve_key and target_sleeve != sleeve_key:
            continue
        if candidate_symbol and target_symbol != str(candidate_symbol or "").strip().upper():
            continue
        matched += 1
        trusted_source_names = {_TARGETED_SOURCE_COMPLETION_SOURCE}
        reviewed_at = datetime.now(UTC).isoformat()
        for item in overrides:
            field_name = str(item.get("field_name") or "").strip()
            value = item.get("value")
            source_url = str(item.get("source_url") or "").strip() or None
            observed_at = str(item.get("observed_at") or datetime.now(UTC).date().isoformat())
            reason = str(item.get("reason") or "Reviewed targeted source completion override.").strip()
            existing = conn.execute(
                """
                SELECT 1
                FROM candidate_field_observations
                WHERE candidate_symbol = ? AND sleeve_key = ? AND field_name = ?
                  AND source_name = ? AND provenance_level = 'manual_reviewed_override'
                  AND missingness_reason = 'populated' AND value_json = ?
                LIMIT 1
                """,
                (
                    target_symbol,
                    target_sleeve,
                    field_name,
                    _TARGETED_SOURCE_COMPLETION_SOURCE,
                    json.dumps(value, sort_keys=True, ensure_ascii=True),
                ),
            ).fetchone()
            if existing is None:
                upsert_field_observation(
                    conn,
                    candidate_symbol=target_symbol,
                    sleeve_key=target_sleeve,
                    field_name=field_name,
                    value=value,
                    source_name=_TARGETED_SOURCE_COMPLETION_SOURCE,
                    source_url=source_url,
                    observed_at=observed_at,
                    provenance_level="manual_reviewed_override",
                    confidence_label="high",
                    parser_method="apply_targeted_source_completion_overrides",
                    override_annotation={
                        "actor": "repo_curated_metrics",
                        "reason": reason,
                        "timestamp": reviewed_at,
                        "review_note": reason,
                        "source_completion_targeted": True,
                        "recovery_run_id": recovery_run_id,
                    },
                )
            updated += int(
                reconcile_field_observations(
                    conn,
                    candidate_symbol=target_symbol,
                    sleeve_key=target_sleeve,
                    field_name=field_name,
                    trusted_value=value,
                    trusted_source_names=trusted_source_names,
                    reason="targeted_source_completion_override",
                    recovery_run_id=recovery_run_id,
                ).get("updated")
                or 0
            )
    return {"matched": matched, "updated": updated}


def _normalize_source_state(value: Any, *, object_type: str) -> str:
    state = str(value or "").strip().lower()
    if object_type == STRATEGY_PLACEHOLDER_TYPE:
        return STRATEGY_PLACEHOLDER_TYPE
    if object_type == POLICY_PLACEHOLDER_TYPE:
        return MANUAL_SEED_STATE
    mapping = {
        "manual_static": MANUAL_SEED_STATE,
        "manual_seed": MANUAL_SEED_STATE,
        "source_validated": SOURCE_VALIDATED_STATE,
        "source_linked_not_validated": MANUAL_SEED_STATE,
        "aging": AGING_STATE,
        "stale": STALE_LIVE_STATE,
        "stale_live": STALE_LIVE_STATE,
        "quarantined": STALE_LIVE_STATE,
        "broken_source": BROKEN_SOURCE_STATE,
        "unknown": MANUAL_SEED_STATE,
        "": MANUAL_SEED_STATE,
    }
    return mapping.get(state, MANUAL_SEED_STATE)


def _provider_cached_aum(conn: sqlite3.Connection, *, symbol: str) -> dict[str, Any] | None:
    try:
        from app.services.provider_refresh import _extract_provider_candidate_fields
        from app.services.provider_cache import list_provider_snapshots
    except Exception:
        return None

    best: dict[str, Any] | None = None
    endpoint_rank = {"fundamentals": 0, "reference_meta": 1, "etf_profile": 2}
    freshness_rank = {"current": 0, "expected_lag": 1, "aging": 1, "fresh": 0, "stale": 2, "unavailable": 3}
    for snapshot in list_provider_snapshots(conn, surface_name="blueprint", limit=1200):
        endpoint_family = str(snapshot.get("endpoint_family") or "").strip()
        if endpoint_family not in endpoint_rank:
            continue
        payload = dict(snapshot.get("payload") or {})
        resolution = dict(payload.get("resolution") or {})
        cache_key = str(snapshot.get("cache_key") or "").strip().upper()
        identifier = str(payload.get("identifier") or "").strip().upper()
        canonical_symbol = str(
            resolution.get("canonical_symbol")
            or payload.get("canonical_symbol")
            or ""
        ).strip().upper()
        if symbol.upper() not in {cache_key, identifier, canonical_symbol}:
            continue
        extracted = _extract_provider_candidate_fields(endpoint_family, payload)
        aum = extracted.get("aum")
        if aum in {None, ""}:
            continue
        freshness_state = str(snapshot.get("freshness_state") or payload.get("freshness_state") or "").strip().lower() or "unknown"
        if freshness_state in {"stale", "unavailable", "unknown"}:
            continue
        candidate = {
            "value": aum,
            "source_name": str(snapshot.get("provider_name") or "provider_cache"),
            "observed_at": payload.get("observed_at") or snapshot.get("fetched_at"),
            "freshness_state": freshness_state,
        }
        candidate_key = (
            freshness_rank.get(freshness_state, 4),
            endpoint_rank.get(endpoint_family, 9),
            0 if candidate["observed_at"] else 1,
            str(candidate["observed_at"] or ""),
        )
        if best is None or candidate_key < best["_rank"]:
            best = {**candidate, "_rank": candidate_key}
    if best is None:
        return None
    best.pop("_rank", None)
    return best


def _yfinance_symbol_candidates(symbol: str, *, primary_listing_exchange: str | None = None) -> list[str]:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return []
    candidates: list[str] = []
    if "." not in normalized:
        exchange = str(primary_listing_exchange or "").strip().upper()
        suffixes: list[str] = []
        if any(token in exchange for token in ("LSE", "LONDON")):
            suffixes.append(".L")
        if any(token in exchange for token in ("SGX", "SES", "SINGAPORE")):
            suffixes.append(".SI")
        for suffix in suffixes:
            candidates.append(f"{normalized}{suffix}")
    candidates.append(normalized)
    seen: set[str] = set()
    ordered: list[str] = []
    for candidate in candidates:
        if not candidate or candidate in seen:
            continue
        seen.add(candidate)
        ordered.append(candidate)
    return ordered


def _normalize_yfinance_launch_date(value: Any) -> str | None:
    if value in {None, ""}:
        return None
    try:
        if hasattr(value, "date"):
            return value.date().isoformat()
    except Exception:
        pass
    try:
        return datetime.fromtimestamp(float(value), tz=UTC).date().isoformat()
    except Exception:
        return None


def _normalize_yfinance_issuer(info: dict[str, Any]) -> str | None:
    for key in ("fundFamily", "family", "fund_family"):
        value = str(info.get(key) or "").strip()
        if value:
            return value
    return None


def _yfinance_quote_snapshot(
    symbol: str,
    *,
    primary_listing_exchange: str | None = None,
) -> dict[str, Any] | None:
    try:
        import yfinance as yf
    except Exception:
        return None

    for candidate in _yfinance_symbol_candidates(symbol, primary_listing_exchange=primary_listing_exchange):
        try:
            ticker = yf.Ticker(candidate)
            info = dict(ticker.info or {})
        except Exception:
            continue
        try:
            fast_info = dict(getattr(ticker, "fast_info", {}) or {})
        except Exception:
            fast_info = {}
        assets = (
            info.get("totalAssets")
            or info.get("netAssets")
            or (
                dict(info.get("fundProfile") or {}).get("totalNetAssets")
                if isinstance(info.get("fundProfile"), dict)
                else None
            )
        )
        raw_currency = info.get("currency") or fast_info.get("currency")
        currency_token = "".join(ch for ch in str(raw_currency or "") if ch.isalpha()).upper()
        currency = currency_token[:3] if len(currency_token) >= 3 else None
        launch_date = _normalize_yfinance_launch_date(
            info.get("fundInceptionDate")
            or info.get("inceptionDate")
            or info.get("fundLaunchDate")
        )
        issuer = _normalize_yfinance_issuer(info)
        if assets in {None, ""} and currency in {None, ""} and launch_date in {None, ""} and issuer in {None, ""}:
            continue
        return {
            "symbol_used": candidate,
            "aum": assets,
            "currency": currency,
            "launch_date": launch_date,
            "issuer": issuer,
            "short_name": info.get("shortName"),
            "fetched_at": datetime.now(UTC).isoformat(),
        }
    return None


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True)


def _parse_json(value: Any, default: Any) -> Any:
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _meaningful(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return True


def _coalesce_meaningful(*values: Any) -> Any:
    for value in values:
        if _meaningful(value):
            return value
    return None


def _structural_primary_trading_currency(
    *,
    doc_extracted: dict[str, Any],
    official_citation_extracted: dict[str, Any],
    extra: dict[str, Any],
    yfinance_quote: dict[str, Any] | None,
) -> Any:
    return _coalesce_meaningful(
        doc_extracted.get("primary_trading_currency"),
        official_citation_extracted.get("primary_trading_currency"),
        extra.get("primary_trading_currency"),
        extra.get("trading_currency"),
        dict(yfinance_quote or {}).get("currency"),
    )


def _reconcile_preferred_observations(
    conn: sqlite3.Connection,
    *,
    candidate_symbol: str,
    sleeve_key: str,
    doc_extracted: dict[str, Any],
    doc_registry_expected_isin: str,
    doc_registry_domicile: str,
    doc_registry_issuer: str | None,
    candidate_registry_currency: Any | None,
    benchmark_assignment: dict[str, Any],
    canonical_benchmark_key: str | None,
    cached_provider_aum: dict[str, Any] | None,
    factsheet_summary: dict[str, Any] | None,
    recovery_run_id: str | None,
) -> dict[str, Any]:
    updated = 0
    def _normalize_alpha_words(value: Any) -> str:
        raw = str(value or "").strip()
        cleaned = "".join(ch if (ch.isalpha() or ch.isspace()) else " " for ch in raw)
        return " ".join(cleaned.upper().split())

    def _canonical_domicile(value: Any) -> str | None:
        token = _normalize_alpha_words(value)
        if not token:
            return None
        if any(
            fragment in token
            for fragment in (
                "ALLOW YOU TO ACCESS",
                "IN WHICH IT IS BEING ACCESSED",
                "WHERE SUCH AN OFFER",
                "WHEN SUCH AN OFFER",
                "AGAINST THE LAW",
                "OFFER OR SOLICITATION",
                "ORSOLICITATION",
            )
        ):
            return None
        return {
            "IRELAND": "Ireland",
            "IRISH": "Ireland",
            "IE": "Ireland",
            "LUXEMBOURG": "Luxembourg",
            "LU": "Luxembourg",
            "SINGAPORE": "Singapore",
            "SG": "Singapore",
            "UNITED KINGDOM": "United Kingdom",
            "GREAT BRITAIN": "United Kingdom",
            "UK": "United Kingdom",
            "GB": "United Kingdom",
            "UNITED STATES": "United States",
            "USA": "United States",
            "US": "United States",
        }.get(token)

    def _canonical_currency(value: Any) -> str | None:
        token = "".join(ch for ch in str(value or "").upper() if ch.isalpha())
        if len(token) < 3:
            return None
        return token[:3]

    def _benchmark_tokens(value: Any) -> set[str]:
        raw = str(value or "").strip().upper()
        if not raw:
            return set()
        cleaned = "".join(ch if (ch.isalnum() or ch.isspace()) else " " for ch in raw)
        tokens = {part for part in cleaned.split() if len(part) > 2}
        return tokens - {
            "INDEX",
            "NET",
            "TOTAL",
            "RETURN",
            "USD",
            "EUR",
            "GBP",
            "THE",
            "AND",
            "DIST",
            "ACC",
        }

    def _benchmark_name_matches_assignment(extracted: Any, canonical_name: Any) -> bool:
        extracted_tokens = _benchmark_tokens(extracted)
        canonical_tokens = _benchmark_tokens(canonical_name)
        if not extracted_tokens or not canonical_tokens:
            return False
        return bool(extracted_tokens & canonical_tokens)

    def _preferred_aum_choice() -> tuple[Any, set[str]] | None:
        factsheet_value = (factsheet_summary or {}).get("latest_aum_usd")
        if _meaningful(factsheet_value):
            return factsheet_value, {"etf_factsheet_metrics"}
        cached_freshness = str((cached_provider_aum or {}).get("freshness_state") or "").strip().lower()
        cached_value = (cached_provider_aum or {}).get("value")
        if _meaningful(cached_value) and cached_freshness not in {"stale", "aging", "unavailable"}:
            return cached_value, {str((cached_provider_aum or {}).get("source_name") or "provider_cache")}
        doc_aum = doc_extracted.get("aum_usd")
        if _meaningful(doc_aum):
            return doc_aum, {"issuer_doc_parser"}
        return None

    benchmark_key = str(
        benchmark_assignment.get("benchmark_key")
        or canonical_benchmark_key
        or ""
    ).strip()
    canonical_benchmark_name = (
        canonical_benchmark_full_name(
            benchmark_key,
            benchmark_assignment.get("benchmark_label"),
        )
        if benchmark_key
        else None
    )
    extracted_benchmark_name = doc_extracted.get("benchmark_name")
    if (
        _meaningful(extracted_benchmark_name)
        and _meaningful(canonical_benchmark_name)
        and _benchmark_name_matches_assignment(extracted_benchmark_name, canonical_benchmark_name)
    ):
        benchmark_name = extracted_benchmark_name
        benchmark_name_sources = {"issuer_doc_parser"}
    else:
        benchmark_name = _coalesce_meaningful(
            canonical_benchmark_name,
            benchmark_assignment.get("benchmark_label"),
            extracted_benchmark_name,
        )
        benchmark_name_sources = {"benchmark_registry"} if _meaningful(benchmark_name) else set()
    preferred_rows: list[tuple[str, Any, set[str], str]] = []
    if _meaningful(doc_registry_issuer):
        preferred_rows.append(("issuer", doc_registry_issuer, {"issuer_doc_registry"}, "issuer_registry_preferred"))
    elif _meaningful(doc_extracted.get("issuer")):
        preferred_rows.append(("issuer", doc_extracted.get("issuer"), {"issuer_doc_parser"}, "issuer_doc_preferred"))
    if _meaningful(doc_extracted.get("isin")):
        preferred_rows.append(("isin", doc_extracted.get("isin"), {"issuer_doc_parser"}, "issuer_doc_preferred"))
    elif _meaningful(doc_registry_expected_isin):
        preferred_rows.append(("isin", doc_registry_expected_isin, {"issuer_doc_registry"}, "issuer_registry_preferred"))
    trusted_domicile = _canonical_domicile(doc_extracted.get("domicile"))
    if trusted_domicile:
        preferred_rows.append(("domicile", trusted_domicile, {"issuer_doc_parser"}, "issuer_doc_preferred"))
    else:
        registry_domicile = _canonical_domicile(doc_registry_domicile)
        if registry_domicile:
            preferred_rows.append(("domicile", registry_domicile, {"issuer_doc_registry"}, "issuer_registry_preferred"))
    registry_currency = _canonical_currency(candidate_registry_currency)
    doc_currency = _canonical_currency(doc_extracted.get("primary_trading_currency"))
    if registry_currency and doc_currency and registry_currency != doc_currency:
        preferred_rows.append(
            (
                "primary_trading_currency",
                registry_currency,
                {"candidate_registry"},
                "candidate_registry_currency_preferred",
            )
        )
    elif doc_currency:
        preferred_rows.append(
            ("primary_trading_currency", doc_currency, {"issuer_doc_parser"}, "issuer_doc_currency_preferred")
        )
    elif registry_currency:
        preferred_rows.append(
            (
                "primary_trading_currency",
                registry_currency,
                {"candidate_registry"},
                "candidate_registry_currency_preferred",
            )
        )
    if _meaningful(benchmark_key):
        preferred_rows.append(("benchmark_key", benchmark_key, {"benchmark_registry"}, "validated_benchmark_lineage"))
    if _meaningful(benchmark_name):
        preferred_rows.append(
            (
                "benchmark_name",
                benchmark_name,
                benchmark_name_sources or {"benchmark_registry"},
                "validated_benchmark_lineage",
            )
        )
    preferred_aum_choice = _preferred_aum_choice()
    if preferred_aum_choice:
        preferred_aum, aum_sources = preferred_aum_choice
        preferred_rows.append(("aum", preferred_aum, aum_sources, "aum_authority_reconciliation"))

    def _trusted_observation_exists(field_name: str, trusted_value: Any, trusted_sources: set[str]) -> bool:
        trusted_value_json = _json(trusted_value) if trusted_value is not None else None
        if trusted_value_json is None:
            return False
        rows = conn.execute(
            """
            SELECT value_json, source_name, missingness_reason, override_annotation_json
            FROM candidate_field_observations
            WHERE candidate_symbol = ? AND sleeve_key = ? AND field_name = ?
            """,
            (candidate_symbol.upper(), sleeve_key, field_name),
        ).fetchall()
        for raw_row in rows:
            row = dict(raw_row)
            if str(row.get("missingness_reason") or "") != "populated":
                continue
            if _observation_reconciled_out(row):
                continue
            if str(row.get("source_name") or "").strip() not in trusted_sources:
                continue
            if str(row.get("value_json") or "").strip() == trusted_value_json:
                return True
        return False

    for field_name, trusted_value, trusted_sources, reason in preferred_rows:
        if not _trusted_observation_exists(field_name, trusted_value, trusted_sources):
            trusted_source_name = next(iter(sorted(trusted_sources)), "candidate_registry")
            provenance_level = (
                "verified_official"
                if trusted_source_name in {"issuer_doc_parser", "etf_factsheet_metrics"}
                else "verified_mapping"
                if trusted_source_name == "benchmark_registry"
                else "verified_nonissuer"
            )
            upsert_field_observation(
                conn,
                candidate_symbol=candidate_symbol,
                sleeve_key=sleeve_key,
                field_name=field_name,
                value=trusted_value,
                source_name=trusted_source_name,
                observed_at=datetime.now(UTC).date().isoformat(),
                provenance_level=provenance_level,
                confidence_label="high",
            )
        updated += int(
            reconcile_field_observations(
                conn,
                candidate_symbol=candidate_symbol,
                sleeve_key=sleeve_key,
                field_name=field_name,
                trusted_value=trusted_value,
                trusted_source_names=trusted_sources,
                reason=reason,
                recovery_run_id=recovery_run_id,
            ).get("updated")
            or 0
        )
    return {"updated": updated}


def _normalize_tracking_difference(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if abs(number) >= 0.01:
        return number / 100.0
    return number


def _doc_result_needs_live_refresh(doc_result: dict[str, Any] | None) -> bool:
    if not doc_result:
        return True
    if str(doc_result.get("status") or "") == "failed":
        return True
    extracted = dict(doc_result.get("extracted") or {})
    if not extracted:
        return True
    launch_present = _meaningful(extracted.get("launch_date")) or _meaningful(extracted.get("inception_date"))
    exchange_present = _meaningful(extracted.get("primary_listing_exchange"))
    currency_present = _meaningful(extracted.get("primary_trading_currency"))
    aum_present = _meaningful(extracted.get("aum_usd"))
    issuer_present = _meaningful(extracted.get("issuer"))
    benchmark_present = _meaningful(extracted.get("benchmark_name"))
    domicile_present = _meaningful(extracted.get("domicile"))
    return not (launch_present and exchange_present and currency_present and aum_present and issuer_present and benchmark_present and domicile_present)


def _doc_registry_row(symbol: str) -> dict[str, Any]:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return {}
    try:
        rows = list(dict(item) for item in list(load_doc_registry().get("candidates") or []) if isinstance(item, dict))
    except Exception:
        return {}
    for item in rows:
        if str(item.get("ticker") or "").strip().upper() == normalized:
            return item
    return {}


def _merge_doc_results(
    fixture_result: dict[str, Any] | None,
    live_result: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not fixture_result and not live_result:
        return None
    if not fixture_result:
        return live_result
    if not live_result:
        return fixture_result

    merged = dict(fixture_result)
    fixture_extracted = dict(fixture_result.get("extracted") or {})
    live_extracted = dict(live_result.get("extracted") or {})
    merged_extracted = dict(fixture_extracted)
    for key, value in live_extracted.items():
        if _meaningful(value):
            merged_extracted[key] = value
    merged["extracted"] = merged_extracted

    fixture_factsheet = dict(fixture_result.get("factsheet") or {})
    live_factsheet = dict(live_result.get("factsheet") or {})
    if live_factsheet:
        merged["factsheet"] = {**fixture_factsheet, **live_factsheet}
    elif fixture_factsheet:
        merged["factsheet"] = fixture_factsheet

    fixture_meta = dict(fixture_result.get("meta") or {})
    live_meta = dict(live_result.get("meta") or {})
    if live_meta:
        merged["meta"] = {**fixture_meta, **live_meta}
    elif fixture_meta:
        merged["meta"] = fixture_meta

    if _meaningful(live_result.get("verified")):
        merged["verified"] = live_result.get("verified")
    elif _meaningful(fixture_result.get("verified")):
        merged["verified"] = fixture_result.get("verified")

    merged["status"] = live_result.get("status") or fixture_result.get("status")
    return merged


def _active_universe_config() -> dict[str, Any]:
    payload = dict(DEFAULT_ACTIVE_UNIVERSE_CONFIG)
    if ACTIVE_UNIVERSE_PATH.exists():
        try:
            loaded = json.loads(ACTIVE_UNIVERSE_PATH.read_text(encoding="utf-8"))
        except Exception:
            loaded = {}
        if isinstance(loaded, dict):
            payload.update({key: value for key, value in loaded.items() if key != "exclude_symbols"})
            merged_exclusions = dict(DEFAULT_ACTIVE_UNIVERSE_CONFIG.get("exclude_symbols") or {})
            extra_exclusions = loaded.get("exclude_symbols")
            if isinstance(extra_exclusions, dict):
                merged_exclusions.update(
                    {
                        str(symbol).strip().upper(): str(reason).strip() or "manual_exclusion"
                        for symbol, reason in extra_exclusions.items()
                        if str(symbol).strip()
                    }
                )
            payload["exclude_symbols"] = merged_exclusions
    payload["include_symbols"] = [
        str(symbol).strip().upper()
        for symbol in list(payload.get("include_symbols") or [])
        if str(symbol).strip()
    ]
    payload["exclude_symbols"] = {
        str(symbol).strip().upper(): str(reason).strip() or "manual_exclusion"
        for symbol, reason in dict(payload.get("exclude_symbols") or {}).items()
        if str(symbol).strip()
    }
    return payload


def _active_universe_include_set() -> set[str]:
    return set(_active_universe_config().get("include_symbols") or [])


def _active_universe_exclusions() -> dict[str, str]:
    return dict(_active_universe_config().get("exclude_symbols") or {})


def _row_is_live_scope(item: dict[str, Any]) -> bool:
    symbol = str(item.get("symbol") or "").strip().upper()
    object_type = str(item.get("object_type") or "")
    include_symbols = _active_universe_include_set()
    exclude_symbols = _active_universe_exclusions()
    if not symbol or object_type != LIVE_OBJECT_TYPE:
        return False
    if symbol in exclude_symbols:
        return False
    if include_symbols and symbol not in include_symbols:
        return False
    return True


def ensure_candidate_registry_tables(conn: sqlite3.Connection) -> None:
    # Legacy table retained as migration source and compatibility layer.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_candidate_registry (
          registry_id TEXT PRIMARY KEY,
          symbol TEXT NOT NULL,
          name TEXT NOT NULL,
          sleeve_key TEXT NOT NULL,
          issuer TEXT,
          asset_class TEXT,
          instrument_type TEXT NOT NULL,
          domicile TEXT NOT NULL,
          share_class TEXT,
          benchmark_key TEXT,
          replication_method TEXT,
          expense_ratio REAL,
          expected_withholding_drag_estimate REAL,
          estate_risk_flag INTEGER NOT NULL DEFAULT 0,
          liquidity_proxy TEXT,
          liquidity_score REAL,
          rationale TEXT,
          citation_keys_json TEXT NOT NULL DEFAULT '[]',
          source_links_json TEXT NOT NULL DEFAULT '[]',
          source_state TEXT NOT NULL DEFAULT 'manual_static',
          factsheet_asof TEXT,
          market_data_asof TEXT,
          verification_metadata_json TEXT NOT NULL DEFAULT '{}',
          manual_provenance_note TEXT,
          extra_json TEXT NOT NULL DEFAULT '{}',
          effective_at TEXT NOT NULL,
          retired_at TEXT,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_blueprint_candidate_registry_symbol_active
        ON blueprint_candidate_registry (symbol, sleeve_key, effective_at)
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_canonical_instruments (
          instrument_id TEXT PRIMARY KEY,
          symbol TEXT NOT NULL UNIQUE,
          name TEXT NOT NULL,
          issuer TEXT,
          asset_class TEXT,
          instrument_type TEXT NOT NULL,
          object_type TEXT NOT NULL DEFAULT 'live_fund_candidate',
          domicile TEXT NOT NULL,
          share_class TEXT,
          benchmark_key TEXT,
          replication_method TEXT,
          expense_ratio REAL,
          expected_withholding_drag_estimate REAL,
          estate_risk_flag INTEGER NOT NULL DEFAULT 0,
          liquidity_proxy TEXT,
          liquidity_score REAL,
          source_links_json TEXT NOT NULL DEFAULT '[]',
          source_state TEXT NOT NULL DEFAULT 'manual_seed',
          factsheet_asof TEXT,
          market_data_asof TEXT,
          latest_fetch_status TEXT NOT NULL DEFAULT 'unknown',
          verification_metadata_json TEXT NOT NULL DEFAULT '{}',
          manual_provenance_note TEXT,
          extra_json TEXT NOT NULL DEFAULT '{}',
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS blueprint_sleeve_candidate_memberships (
          membership_id TEXT PRIMARY KEY,
          symbol TEXT NOT NULL,
          sleeve_key TEXT NOT NULL,
          role_in_sleeve TEXT,
          sleeve_specific_notes TEXT,
          rationale TEXT,
          citation_keys_json TEXT NOT NULL DEFAULT '[]',
          extra_json TEXT NOT NULL DEFAULT '{}',
          effective_at TEXT NOT NULL,
          retired_at TEXT,
          updated_at TEXT NOT NULL,
          UNIQUE(symbol, sleeve_key, effective_at)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_blueprint_memberships_active
        ON blueprint_sleeve_candidate_memberships (sleeve_key, symbol, effective_at DESC)
        """
    )
    ensure_candidate_truth_tables(conn)
    conn.commit()


def _canonical_row_from_legacy(conn: sqlite3.Connection, symbol: str) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT *
        FROM blueprint_candidate_registry
        WHERE symbol = ? AND retired_at IS NULL
        ORDER BY updated_at DESC, effective_at DESC
        """,
        (symbol,),
    ).fetchall()
    if not rows:
        return {}
    items = [dict(row) for row in rows]
    best = dict(items[0])
    merged_extra: dict[str, Any] = {}
    source_links: list[str] = []
    verification_metadata = {}
    for item in items:
        merged_extra.update(_parse_json(item.get("extra_json"), {}))
        source_links.extend([str(link).strip() for link in _parse_json(item.get("source_links_json"), []) if str(link).strip()])
        meta = _parse_json(item.get("verification_metadata_json"), {})
        if isinstance(meta, dict) and len(meta) >= len(verification_metadata):
            verification_metadata = meta
        if item.get("factsheet_asof") and not best.get("factsheet_asof"):
            best["factsheet_asof"] = item.get("factsheet_asof")
        if item.get("market_data_asof") and not best.get("market_data_asof"):
            best["market_data_asof"] = item.get("market_data_asof")
        if item.get("source_state") == SOURCE_VALIDATED_STATE:
            best["source_state"] = SOURCE_VALIDATED_STATE
    best["extra"] = merged_extra
    best["source_links"] = sorted(dict.fromkeys(source_links))
    best["verification_metadata"] = verification_metadata
    return best


def _config_source_links(symbol: str) -> list[str]:
    config = get_etf_source_config(symbol) or {}
    out: list[str] = []
    for details in dict(config.get("data_sources") or {}).values():
        if not isinstance(details, dict):
            continue
        url = str(details.get("url") or details.get("url_template") or "").strip()
        if url:
            out.append(url)
    return sorted(dict.fromkeys(out))


def _infer_object_type(item: dict[str, Any]) -> str:
    symbol = str(item.get("symbol") or "").strip().upper()
    instrument_type = str(item.get("instrument_type") or "").strip().lower()
    extra = dict(item.get("extra") or {})
    if symbol in STRATEGY_PLACEHOLDER_SYMBOLS or instrument_type == "long_put_overlay_strategy":
        return STRATEGY_PLACEHOLDER_TYPE
    if symbol in POLICY_PLACEHOLDER_SYMBOLS or bool(extra.get("policy_placeholder")):
        return POLICY_PLACEHOLDER_TYPE
    return LIVE_OBJECT_TYPE


def _upsert_canonical_instrument(conn: sqlite3.Connection, item: dict[str, Any], *, now: str) -> None:
    symbol = str(item.get("symbol") or "").strip().upper()
    if not symbol:
        return
    object_type = _infer_object_type(item)
    legacy = _canonical_row_from_legacy(conn, symbol)
    existing = conn.execute(
        "SELECT * FROM blueprint_canonical_instruments WHERE symbol = ? LIMIT 1",
        (symbol,),
    ).fetchone()
    existing_item = dict(existing) if existing else {}
    seed_extra = dict(item.get("extra") or {})
    merged_extra = dict(_parse_json(existing_item.get("extra_json"), {}))
    merged_extra.update(dict(legacy.get("extra") or {}))
    merged_extra.update(seed_extra)
    source_links = list(_parse_json(existing_item.get("source_links_json"), []))
    source_links.extend(list(legacy.get("source_links") or []))
    source_links.extend(list(item.get("source_links") or []))
    source_links.extend(_config_source_links(symbol))
    source_links = sorted(dict.fromkeys([str(link).strip() for link in source_links if str(link).strip()]))
    verification_metadata = dict(_parse_json(existing_item.get("verification_metadata_json"), {}))
    verification_metadata.update(dict(legacy.get("verification_metadata") or {}))
    verification_metadata.setdefault("seed_source", str(SEED_PATH.relative_to(get_repo_root())))
    verification_metadata["seed_loaded_at"] = now
    source_state = _normalize_source_state(
        existing_item.get("source_state") or legacy.get("source_state") or MANUAL_SEED_STATE,
        object_type=object_type,
    )
    conn.execute(
        """
        INSERT INTO blueprint_canonical_instruments (
          instrument_id, symbol, name, issuer, asset_class, instrument_type, object_type,
          domicile, share_class, benchmark_key, replication_method, expense_ratio,
          expected_withholding_drag_estimate, estate_risk_flag, liquidity_proxy, liquidity_score,
          source_links_json, source_state, factsheet_asof, market_data_asof, latest_fetch_status,
          verification_metadata_json, manual_provenance_note, extra_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
          name = excluded.name,
          issuer = COALESCE(excluded.issuer, blueprint_canonical_instruments.issuer),
          asset_class = COALESCE(excluded.asset_class, blueprint_canonical_instruments.asset_class),
          instrument_type = excluded.instrument_type,
          object_type = excluded.object_type,
          domicile = excluded.domicile,
          share_class = COALESCE(excluded.share_class, blueprint_canonical_instruments.share_class),
          benchmark_key = COALESCE(excluded.benchmark_key, blueprint_canonical_instruments.benchmark_key),
          replication_method = COALESCE(excluded.replication_method, blueprint_canonical_instruments.replication_method),
          expense_ratio = COALESCE(excluded.expense_ratio, blueprint_canonical_instruments.expense_ratio),
          expected_withholding_drag_estimate = COALESCE(excluded.expected_withholding_drag_estimate, blueprint_canonical_instruments.expected_withholding_drag_estimate),
          estate_risk_flag = excluded.estate_risk_flag,
          liquidity_proxy = COALESCE(excluded.liquidity_proxy, blueprint_canonical_instruments.liquidity_proxy),
          liquidity_score = COALESCE(excluded.liquidity_score, blueprint_canonical_instruments.liquidity_score),
          source_links_json = excluded.source_links_json,
          source_state = excluded.source_state,
          factsheet_asof = COALESCE(blueprint_canonical_instruments.factsheet_asof, excluded.factsheet_asof),
          market_data_asof = COALESCE(blueprint_canonical_instruments.market_data_asof, excluded.market_data_asof),
          latest_fetch_status = COALESCE(NULLIF(blueprint_canonical_instruments.latest_fetch_status, 'unknown'), excluded.latest_fetch_status),
          verification_metadata_json = excluded.verification_metadata_json,
          manual_provenance_note = COALESCE(blueprint_canonical_instruments.manual_provenance_note, excluded.manual_provenance_note),
          extra_json = excluded.extra_json,
          updated_at = excluded.updated_at
        """,
        (
            f"instrument_{symbol.lower()}",
            symbol,
            str(item.get("name") or legacy.get("name") or symbol),
            str(existing_item.get("issuer") or legacy.get("issuer") or _infer_issuer(item)),
            str(existing_item.get("asset_class") or legacy.get("asset_class") or _infer_asset_class(item)),
            str(item.get("instrument_type") or existing_item.get("instrument_type") or legacy.get("instrument_type") or "etf_ucits"),
            object_type,
            str(item.get("domicile") or existing_item.get("domicile") or legacy.get("domicile") or "unknown"),
            str(item.get("accumulation") or item.get("share_class") or existing_item.get("share_class") or legacy.get("share_class") or "unknown"),
            item.get("benchmark_key") or existing_item.get("benchmark_key") or legacy.get("benchmark_key"),
            str(item.get("replication_method") or existing_item.get("replication_method") or legacy.get("replication_method") or "unknown"),
            item.get("expense_ratio") if item.get("expense_ratio") is not None else existing_item.get("expense_ratio") or legacy.get("expense_ratio"),
            item.get("expected_withholding_drag_estimate") if item.get("expected_withholding_drag_estimate") is not None else existing_item.get("expected_withholding_drag_estimate") or legacy.get("expected_withholding_drag_estimate"),
            1 if bool(item.get("us_situs_risk_flag") or item.get("estate_risk_flag") or existing_item.get("estate_risk_flag") or legacy.get("estate_risk_flag")) else 0,
            str(item.get("liquidity_proxy") or existing_item.get("liquidity_proxy") or legacy.get("liquidity_proxy") or "unknown"),
            item.get("liquidity_score") if item.get("liquidity_score") is not None else existing_item.get("liquidity_score") or legacy.get("liquidity_score"),
            _json(source_links),
            source_state,
            existing_item.get("factsheet_asof") or legacy.get("factsheet_asof") or seed_extra.get("factsheet_asof"),
            existing_item.get("market_data_asof") or legacy.get("market_data_asof") or seed_extra.get("market_data_asof"),
            str(existing_item.get("latest_fetch_status") or legacy.get("verification_metadata", {}).get("latest_fetch_status") or "unknown"),
            _json(verification_metadata),
            existing_item.get("manual_provenance_note") or legacy.get("manual_provenance_note") or "Seeded from controlled candidate registry fixture; canonical truth awaits source refresh.",
            _json(merged_extra),
            str(existing_item.get("created_at") or now),
            now,
        ),
    )


def _upsert_membership(conn: sqlite3.Connection, item: dict[str, Any], *, now: str) -> None:
    symbol = str(item.get("symbol") or "").strip().upper()
    sleeve_key = str(item.get("sleeve_key") or "").strip()
    if not symbol or not sleeve_key:
        return
    existing = conn.execute(
        "SELECT * FROM blueprint_sleeve_candidate_memberships WHERE symbol = ? AND sleeve_key = ? AND retired_at IS NULL LIMIT 1",
        (symbol, sleeve_key),
    ).fetchone()
    existing_item = dict(existing) if existing else {}
    conn.execute(
        """
        INSERT INTO blueprint_sleeve_candidate_memberships (
          membership_id, symbol, sleeve_key, role_in_sleeve, sleeve_specific_notes, rationale,
          citation_keys_json, extra_json, effective_at, retired_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, ?)
        ON CONFLICT(symbol, sleeve_key, effective_at) DO UPDATE SET
          role_in_sleeve = COALESCE(excluded.role_in_sleeve, blueprint_sleeve_candidate_memberships.role_in_sleeve),
          sleeve_specific_notes = COALESCE(excluded.sleeve_specific_notes, blueprint_sleeve_candidate_memberships.sleeve_specific_notes),
          rationale = COALESCE(excluded.rationale, blueprint_sleeve_candidate_memberships.rationale),
          citation_keys_json = excluded.citation_keys_json,
          extra_json = excluded.extra_json,
          updated_at = excluded.updated_at
        """,
        (
            f"membership_{symbol.lower()}_{sleeve_key}",
            symbol,
            sleeve_key,
            str((item.get("extra") or {}).get("role_in_sleeve") or "").strip() or None,
            str((item.get("extra") or {}).get("sleeve_specific_notes") or "").strip() or None,
            str(item.get("rationale") or ""),
            _json(list(item.get("citation_keys") or [])),
            _json(dict(item.get("extra") or {})),
            str(existing_item.get("effective_at") or now),
            now,
        ),
    )


def seed_default_candidate_registry(conn: sqlite3.Connection) -> int:
    ensure_candidate_registry_tables(conn)
    seed_required_field_matrix(conn, overwrite_existing=True)
    if not SEED_PATH.exists():
        return 0
    rows = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    now = datetime.now(UTC).isoformat()
    for item in rows:
        _upsert_canonical_instrument(conn, item, now=now)
        _upsert_membership(conn, item, now=now)
    conn.commit()
    current_count = conn.execute(
        "SELECT COUNT(*) AS count FROM blueprint_sleeve_candidate_memberships WHERE retired_at IS NULL"
    ).fetchone()
    return int(current_count["count"] or 0) if current_count else 0


def _joined_active_rows(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    ensure_candidate_registry_tables(conn)
    rows = conn.execute(
        """
        SELECT
          m.membership_id AS registry_id,
          m.symbol,
          c.name,
          m.sleeve_key,
          c.issuer,
          c.asset_class,
          c.instrument_type,
          c.object_type,
          c.domicile,
          c.share_class,
          c.benchmark_key,
          c.replication_method,
          c.expense_ratio,
          c.expected_withholding_drag_estimate,
          c.estate_risk_flag,
          c.liquidity_proxy,
          c.liquidity_score,
          m.rationale,
          m.citation_keys_json,
          c.source_links_json,
          c.source_state,
          c.factsheet_asof,
          c.market_data_asof,
          c.latest_fetch_status,
          c.verification_metadata_json,
          c.manual_provenance_note,
          c.extra_json AS canonical_extra_json,
          m.extra_json AS membership_extra_json,
          m.effective_at,
          m.retired_at,
          m.updated_at,
          c.updated_at AS canonical_updated_at
        FROM blueprint_sleeve_candidate_memberships m
        JOIN blueprint_canonical_instruments c ON c.symbol = m.symbol
        WHERE m.retired_at IS NULL
        ORDER BY m.sleeve_key ASC, m.symbol ASC, m.effective_at DESC
        """
    ).fetchall()
    items: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        citation_keys = _parse_json(item.pop("citation_keys_json"), [])
        source_links = _parse_json(item.pop("source_links_json"), [])
        verification_metadata = _parse_json(item.pop("verification_metadata_json"), {})
        canonical_extra = _parse_json(item.pop("canonical_extra_json"), {})
        membership_extra = _parse_json(item.pop("membership_extra_json"), {})
        merged_extra = dict(canonical_extra)
        merged_extra.update(dict(membership_extra))
        item["citation_keys"] = citation_keys
        item["source_links"] = source_links
        item["verification_metadata"] = verification_metadata
        item["extra"] = merged_extra
        item["estate_risk_flag"] = bool(item.get("estate_risk_flag"))
        item["source_links_count"] = len(source_links)
        items.append(item)
    return items


def list_active_candidate_registry(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return _joined_active_rows(conn)


def export_candidate_registry(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return _joined_active_rows(conn)


def export_live_candidate_registry(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    return [item for item in _joined_active_rows(conn) if _row_is_live_scope(item)]


def list_live_candidate_symbols(conn: sqlite3.Connection) -> list[str]:
    symbols: list[str] = []
    for item in export_live_candidate_registry(conn):
        symbol = str(item.get("symbol") or "").strip().upper()
        if symbol and symbol not in symbols:
            symbols.append(symbol)
    return symbols


def active_candidate_universe_summary(conn: sqlite3.Connection) -> dict[str, Any]:
    rows = export_live_candidate_registry(conn)
    raw_rows = export_candidate_registry(conn)
    config = _active_universe_config()
    return {
        "selection_mode": str(config.get("selection_mode") or "all_live_candidates_except_excluded"),
        "source_path": str(ACTIVE_UNIVERSE_PATH.relative_to(get_repo_root())),
        "raw_membership_count": len(raw_rows),
        "active_membership_count": len(rows),
        "active_symbol_count": len(list_live_candidate_symbols(conn)),
        "excluded_symbols": dict(config.get("exclude_symbols") or {}),
        "included_symbols": list(config.get("include_symbols") or []),
    }


def refresh_registry_candidate_truth(
    conn: sqlite3.Connection,
    *,
    symbol: str,
    recovery_run_id: str | None = None,
    activate_market_series: bool = False,
) -> dict[str, Any]:
    ensure_candidate_registry_tables(conn)
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return {"symbol": symbol, "updated": False, "reason": "missing_symbol"}

    canonical_row = conn.execute(
        "SELECT * FROM blueprint_canonical_instruments WHERE symbol = ? LIMIT 1",
        (normalized,),
    ).fetchone()
    if canonical_row is None:
        return {"symbol": normalized, "updated": False, "reason": "missing_canonical_instrument"}
    canonical = dict(canonical_row)
    memberships = conn.execute(
        "SELECT * FROM blueprint_sleeve_candidate_memberships WHERE symbol = ? AND retired_at IS NULL ORDER BY sleeve_key",
        (normalized,),
    ).fetchall()
    membership_items = [dict(row) for row in memberships]
    instrument_type = str(canonical.get("instrument_type") or "")
    object_type = str(canonical.get("object_type") or LIVE_OBJECT_TYPE)
    source_config = get_etf_source_config(normalized) or {}
    doc_registry_row = _doc_registry_row(normalized)
    doc_registry_expected_isin = str(doc_registry_row.get("expected_isin") or "").strip().upper()
    doc_registry_domicile = _ISIN_PREFIX_TO_COUNTRY.get(doc_registry_expected_isin[:2], "") if len(doc_registry_expected_isin) >= 2 else ""
    doc_registry_issuer = str(doc_registry_row.get("issuer") or "").strip() or None

    if object_type == STRATEGY_PLACEHOLDER_TYPE:
        source_state = STRATEGY_PLACEHOLDER_TYPE
        latest_status = "not_applicable"
        factsheet_asof = canonical.get("factsheet_asof")
        market_asof = canonical.get("market_data_asof")
        fetch_entries = []
        doc_extracted = {}
        doc_available = False
        holdings_profile = None
        latest_market_data = None
        factsheet_summary = None
        market_summary = None
    elif object_type == POLICY_PLACEHOLDER_TYPE:
        source_state = MANUAL_SEED_STATE
        latest_status = "not_applicable"
        factsheet_asof = canonical.get("factsheet_asof")
        market_asof = canonical.get("market_data_asof")
        fetch_entries = []
        doc_extracted = {}
        doc_available = False
        holdings_profile = None
        latest_market_data = None
        factsheet_summary = None
        market_summary = None
    else:
        factsheet_summary = get_etf_factsheet_history_summary(normalized, conn)
        market_summary = get_preferred_market_history_summary(normalized, conn)
        latest_market_data = get_preferred_latest_market_data(normalized, conn)
        fetch_status = get_latest_etf_fetch_status(normalized, conn)
        factsheet_asof = (factsheet_summary or {}).get("latest_asof_date") or canonical.get("factsheet_asof")
        market_asof = (market_summary or {}).get("latest_asof_date") or canonical.get("market_data_asof")
        fetch_entries = list(fetch_status.get("entries") or [])
        latest_status = str(fetch_status.get("status") or "unknown")
        doc_result = fetch_candidate_docs(normalized, use_fixtures=True) if instrument_type in {"etf_ucits", "etf_us"} else None
        if instrument_type in {"etf_ucits", "etf_us"} and _doc_result_needs_live_refresh(doc_result):
            try:
                live_doc_result = fetch_candidate_docs(normalized, use_fixtures=False)
            except Exception:
                live_doc_result = None
            if live_doc_result and str(live_doc_result.get("status") or "") != "failed":
                doc_result = _merge_doc_results(doc_result, live_doc_result)
        doc_extracted = dict((doc_result or {}).get("extracted") or {})
        doc_available = bool(doc_result) and (
            str((doc_result or {}).get("status") or "") == "success"
            or str(dict((doc_result or {}).get("factsheet") or {}).get("status") or "") == "success"
            or bool((doc_result or {}).get("verified"))
        )
        if doc_available:
            factsheet_asof = doc_extracted.get("factsheet_date") or factsheet_asof or canonical.get("factsheet_asof")
            if latest_status == "unknown":
                latest_status = "success"
            if not any(str(entry.get("status") or "") == "success" for entry in fetch_entries):
                fetch_entries.append(
                    {
                        "data_type": "factsheet",
                        "source_id": f"{normalized.lower()}_factsheet_fixture",
                        "started_at": datetime.now(UTC).isoformat(),
                        "finished_at": datetime.now(UTC).isoformat(),
                        "status": "success",
                        "records_fetched": 1,
                        "error_message": None,
                        "source_url": str(dict((doc_result or {}).get("factsheet") or {}).get("doc_url") or dict((doc_result or {}).get("meta") or {}).get("cache_file") or ""),
                    }
                )
        holdings_profile = get_etf_holdings_profile(normalized, conn) if instrument_type in {"etf_ucits", "etf_us"} else None
        linked = bool(_config_source_links(normalized))
        if latest_status == "failed" and linked:
            source_state = BROKEN_SOURCE_STATE
        elif linked and (doc_available or latest_status in {"success", "partial_success"} or factsheet_asof or market_asof):
            freshest = None
            for candidate_date in (factsheet_asof, market_asof):
                try:
                    parsed = datetime.fromisoformat(str(candidate_date)[:10])
                except Exception:
                    continue
                if freshest is None or parsed > freshest:
                    freshest = parsed
            if freshest is None:
                source_state = "source_validated"
            else:
                settings = get_settings()
                age_days = (datetime.now(UTC).date() - freshest.date()).days
                if age_days <= 7:
                    source_state = "source_validated"
                elif age_days <= max(14, settings.blueprint_liquidity_proxy_freshness_days):
                    source_state = AGING_STATE
                else:
                    source_state = STALE_LIVE_STATE
        else:
            source_state = MANUAL_SEED_STATE

    source_links: list[str] = []
    source_links.extend(list(_parse_json(canonical.get("source_links_json"), [])))
    source_links.extend(_config_source_links(normalized))
    for summary in (factsheet_summary, market_summary):
        citation = dict((summary or {}).get("citation") or {})
        url = str(citation.get("source_url") or "").strip()
        if url:
            source_links.append(url)
    for entry in fetch_entries[:5]:
        url = str(entry.get("source_url") or "").strip()
        if url:
            source_links.append(url)
    source_links = sorted(dict.fromkeys([link for link in source_links if link]))

    verification_metadata = dict(_parse_json(canonical.get("verification_metadata_json"), {}))
    verification_metadata.update(
        {
            "latest_fetch_status": latest_status,
            "latest_run_at": None if object_type != LIVE_OBJECT_TYPE else (None if latest_status == "not_applicable" else datetime.now(UTC).isoformat()),
            "latest_success_at": datetime.now(UTC).isoformat() if latest_status in {"success", "partial_success"} else verification_metadata.get("latest_success_at"),
            "fetch_entries": fetch_entries[:5],
            "factsheet_summary": factsheet_summary or {},
            "market_summary": market_summary or {},
            "source_config": {
                "name": source_config.get("name"),
                "listings": list(source_config.get("listings") or []),
            },
            "registry_refreshed_at": datetime.now(UTC).isoformat(),
        }
    )

    now = datetime.now(UTC).isoformat()
    conn.execute(
        """
        UPDATE blueprint_canonical_instruments
        SET source_state = ?,
            factsheet_asof = COALESCE(?, factsheet_asof),
            market_data_asof = COALESCE(?, market_data_asof),
            latest_fetch_status = ?,
            source_links_json = ?,
            verification_metadata_json = ?,
            updated_at = ?
        WHERE symbol = ?
        """,
        (
            source_state,
            factsheet_asof,
            market_asof,
            latest_status,
            _json(source_links),
            _json(verification_metadata),
            now,
            normalized,
        ),
    )

    canonical_extra = dict(_parse_json(canonical.get("extra_json"), {}))
    market_series_activated = False
    for membership in membership_items:
        extra = dict(canonical_extra)
        extra.update(_parse_json(membership.get("extra_json"), {}))
        candidate_for_assignment = {
            "symbol": normalized,
            "sleeve_key": str(membership.get("sleeve_key") or ""),
            "benchmark_key": canonical.get("benchmark_key") or extra.get("benchmark_key"),
            "benchmark_name": extra.get("benchmark_name"),
            "extra": extra,
        }
        benchmark_assignment = resolve_benchmark_assignment(
            conn,
            candidate=candidate_for_assignment,
            sleeve_key=str(membership.get("sleeve_key") or ""),
        )
        from app.services.blueprint_investment_quality import get_latest_performance_metrics

        performance_metrics = get_latest_performance_metrics(normalized, conn, candidate={**candidate_for_assignment, **canonical, **membership})
        tax_truth = build_sg_tax_truth(
            domicile=str(canonical.get("domicile") or ""),
            expected_withholding_rate=canonical.get("expected_withholding_drag_estimate"),
            us_situs_risk_flag=bool(canonical.get("estate_risk_flag")),
            accumulation_or_distribution=str(canonical.get("share_class") or ""),
            instrument_type=instrument_type,
        )
        observed_at = factsheet_asof or market_asof or now
        configured_sources = set(dict(source_config.get("data_sources") or {}).keys())
        holdings_backed_fields = {
            "us_weight",
            "em_weight",
            "top_10_concentration",
            "sector_concentration_proxy",
            "holdings_count",
            "developed_market_exposure_summary",
            "emerging_market_exposure_summary",
        }
        market_backed_fields = {"bid_ask_spread_proxy", "volume_30d_avg", "primary_listing_exchange"}
        factsheet_metric_fields = {"tracking_difference_1y", "tracking_difference_3y", "tracking_difference_5y", "tracking_error_1y", "aum"}
        factsheet_backed_fields = {
            "isin",
            "fund_name",
            "share_class",
            "share_class_proven",
            "wrapper_or_vehicle_type",
            "distribution_type",
            "replication_method",
            "expense_ratio",
            "benchmark_name",
            "factsheet_as_of",
            "primary_trading_currency",
            "ucits_status",
            "effective_duration",
            "average_maturity",
            "yield_proxy",
            "credit_quality_mix",
            "government_vs_corporate_split",
            "interest_rate_sensitivity_proxy",
            "issuer_concentration_proxy",
            "weighted_average_maturity",
            "portfolio_quality_summary",
            "redemption_settlement_notes",
        }

        def _missingness_for_field(field_name: str) -> str:
            if object_type != LIVE_OBJECT_TYPE:
                return "not_applicable"
            if field_name in holdings_backed_fields:
                if holdings_profile:
                    return "blocked_by_parser_gap"
                if "holdings" in configured_sources or "factsheet" in configured_sources:
                    return "fetchable_from_current_sources"
                return "blocked_by_source_gap"
            if field_name in market_backed_fields:
                if latest_market_data or market_summary:
                    return "blocked_by_parser_gap"
                if "market_data" in configured_sources:
                    return "fetchable_from_current_sources"
                return "blocked_by_source_gap"
            if field_name in factsheet_metric_fields:
                if factsheet_summary:
                    return "blocked_by_parser_gap"
                if doc_available or "factsheet" in configured_sources:
                    return "fetchable_from_current_sources"
                return "blocked_by_source_gap"
            if field_name in factsheet_backed_fields:
                if doc_available:
                    return "blocked_by_parser_gap"
                if "factsheet" in configured_sources:
                    return "fetchable_from_current_sources"
                return "blocked_by_source_gap"
            return "blocked_by_source_gap"

        cached_provider_aum = _provider_cached_aum(conn, symbol=normalized)
        preferred_listing_exchange = _coalesce_meaningful(
            doc_extracted.get("primary_listing_exchange"),
            extra.get("primary_listing_exchange"),
            get_preferred_market_exchange(normalized, conn),
        )
        yfinance_quote = (
            _yfinance_quote_snapshot(
                normalized,
                primary_listing_exchange=str(preferred_listing_exchange or "") or None,
            )
            if object_type == LIVE_OBJECT_TYPE
            else None
        )
        official_citation_source_url = str(dict((factsheet_summary or {}).get("citation") or {}).get("source_url") or "") or None
        official_citation_extracted: dict[str, Any] = {}
        needs_citation_followup = any(
            not _meaningful(doc_extracted.get(field_name))
            for field_name in (
                "issuer",
                "benchmark_name",
                "primary_trading_currency",
                "launch_date",
                "aum_usd",
                "tracking_difference_1y",
                "tracking_difference_3y",
                "tracking_difference_5y",
            )
        )
        if object_type == LIVE_OBJECT_TYPE and official_citation_source_url and needs_citation_followup:
            citation_result = fetch_and_parse_etf_doc(
                normalized,
                official_citation_source_url,
                "factsheet",
                Path("outbox/etf_docs_cache") / normalized,
            )
            if str(citation_result.get("status") or "") == "success":
                official_citation_extracted = dict(citation_result.get("extracted") or {})

        selected_trading_currency = _structural_primary_trading_currency(
            doc_extracted=doc_extracted,
            official_citation_extracted=official_citation_extracted,
            extra=extra,
            yfinance_quote=yfinance_quote,
        )

        base_fields = {
            "candidate_id": membership.get("membership_id"),
            "symbol": normalized,
            "fund_name": _coalesce_meaningful(
                doc_extracted.get("fund_name"),
                None
                if str(canonical.get("name") or "").strip().upper().startswith(("ISIN:", "CUSIP:", "SEDOL:"))
                else canonical.get("name"),
            ),
            "issuer": _coalesce_meaningful(
                doc_registry_issuer,
                doc_extracted.get("issuer"),
                official_citation_extracted.get("issuer"),
                canonical.get("issuer"),
                dict(yfinance_quote or {}).get("issuer"),
            ),
            "domicile": _coalesce_meaningful(doc_extracted.get("domicile"), doc_registry_domicile, canonical.get("domicile"), extra.get("domicile")),
            "isin": _coalesce_meaningful(doc_extracted.get("isin"), doc_registry_expected_isin, extra.get("isin")),
            "share_class": doc_extracted.get("accumulating_status") or canonical.get("share_class"),
            "share_class_proven": bool(doc_extracted.get("accumulating_status")) if doc_available else extra.get("share_class_proven"),
            "wrapper_or_vehicle_type": doc_extracted.get("wrapper_or_vehicle_type") or canonical.get("instrument_type"),
            "distribution_type": doc_extracted.get("accumulating_status") or canonical.get("share_class"),
            "replication_method": doc_extracted.get("replication_method") or canonical.get("replication_method"),
            "expense_ratio": doc_extracted.get("ter") if doc_extracted.get("ter") is not None else canonical.get("expense_ratio"),
            "benchmark_name": _coalesce_meaningful(
                doc_extracted.get("benchmark_name"),
                official_citation_extracted.get("benchmark_name"),
                canonical_benchmark_full_name(
                    canonical.get("benchmark_key") or extra.get("benchmark_key") or benchmark_assignment.get("benchmark_key"),
                    benchmark_assignment.get("benchmark_label") or extra.get("benchmark_name"),
                ),
                extra.get("benchmark_name"),
            ),
            "benchmark_key": canonical.get("benchmark_key") or extra.get("benchmark_key") or benchmark_assignment.get("benchmark_key"),
            "benchmark_assignment_method": benchmark_assignment.get("assignment_source") or ("candidate_registry" if canonical.get("benchmark_key") else None),
            "benchmark_assignment_proof": benchmark_assignment.get("rationale") or ("explicit candidate registry benchmark key" if canonical.get("benchmark_key") else None),
            "benchmark_confidence": benchmark_assignment.get("benchmark_confidence"),
            "liquidity_proxy": None if object_type == LIVE_OBJECT_TYPE else canonical.get("liquidity_proxy"),
            "source_state": source_state,
            "factsheet_as_of": factsheet_asof,
            "market_data_as_of": market_asof,
            "last_successful_ingest_at": get_latest_successful_etf_ingest_at(normalized, conn) if object_type == LIVE_OBJECT_TYPE else None,
            "estate_risk_posture": tax_truth.get("estate_risk_posture"),
            "withholding_tax_posture": tax_truth.get("withholding_tax_posture"),
            "primary_trading_currency": selected_trading_currency,
            "primary_listing_exchange": preferred_listing_exchange,
            "launch_date": _coalesce_meaningful(
                doc_extracted.get("launch_date"),
                doc_extracted.get("inception_date"),
                official_citation_extracted.get("launch_date"),
                official_citation_extracted.get("inception_date"),
                dict(yfinance_quote or {}).get("launch_date"),
                extra.get("launch_date"),
                extra.get("inception_date"),
            ),
            "tracking_difference_1y": _normalize_tracking_difference(
                (performance_metrics or {}).get("tracking_difference_1y")
                if performance_metrics and (performance_metrics or {}).get("tracking_difference_1y") is not None
                else (
                (factsheet_summary or {}).get("tracking_difference_1y")
                if factsheet_summary and (factsheet_summary or {}).get("tracking_difference_1y") is not None
                else _coalesce_meaningful(
                    doc_extracted.get("tracking_difference_1y"),
                    official_citation_extracted.get("tracking_difference_1y"),
                    extra.get("tracking_difference_1y"),
                ))
            ),
            "tracking_difference_3y": _normalize_tracking_difference(
                (performance_metrics or {}).get("tracking_difference_3y")
                if performance_metrics and (performance_metrics or {}).get("tracking_difference_3y") is not None
                else (
                (factsheet_summary or {}).get("tracking_difference_3y")
                if factsheet_summary and (factsheet_summary or {}).get("tracking_difference_3y") is not None
                else _coalesce_meaningful(
                    doc_extracted.get("tracking_difference_3y"),
                    official_citation_extracted.get("tracking_difference_3y"),
                    extra.get("tracking_difference_3y"),
                ))
            ),
            "tracking_difference_5y": _normalize_tracking_difference(
                (performance_metrics or {}).get("tracking_difference_5y")
                if performance_metrics and (performance_metrics or {}).get("tracking_difference_5y") is not None
                else (
                (factsheet_summary or {}).get("tracking_difference_5y")
                if factsheet_summary and (factsheet_summary or {}).get("tracking_difference_5y") is not None
                else _coalesce_meaningful(
                    doc_extracted.get("tracking_difference_5y"),
                    official_citation_extracted.get("tracking_difference_5y"),
                    extra.get("tracking_difference_5y"),
                ))
            ),
            "tracking_error_1y": (factsheet_summary or {}).get("tracking_error_1y") if factsheet_summary else extra.get("tracking_error_1y"),
            "aum": _coalesce_meaningful(
                (factsheet_summary or {}).get("latest_aum_usd") if factsheet_summary else None,
                doc_extracted.get("aum_usd"),
                official_citation_extracted.get("aum_usd"),
                dict(cached_provider_aum or {}).get("value"),
                dict(yfinance_quote or {}).get("aum"),
                extra.get("aum_usd"),
            ),
            "bid_ask_spread_proxy": (latest_market_data or {}).get("bid_ask_spread_bps") if latest_market_data and (latest_market_data or {}).get("bid_ask_spread_bps") is not None else None,
            "volume_30d_avg": (latest_market_data or {}).get("volume_30d_avg") if latest_market_data and (latest_market_data or {}).get("volume_30d_avg") is not None else extra.get("volume_30d_avg"),
            "us_weight": (holdings_profile or {}).get("us_weight") if holdings_profile else extra.get("us_weight_pct"),
            "em_weight": (holdings_profile or {}).get("em_weight") if holdings_profile else extra.get("em_weight_pct"),
            "top_10_concentration": (holdings_profile or {}).get("top_10_concentration") if holdings_profile else extra.get("top10_concentration_pct"),
            "sector_concentration_proxy": (holdings_profile or {}).get("sector_concentration_proxy") if holdings_profile else extra.get("tech_weight_pct"),
            "holdings_count": doc_extracted.get("holdings_count") if doc_extracted.get("holdings_count") is not None else (holdings_profile or {}).get("holdings_count") if holdings_profile else extra.get("holdings_count"),
            "developed_market_exposure_summary": (holdings_profile or {}).get("developed_market_exposure_summary") if holdings_profile else extra.get("developed_market_exposure_summary"),
            "emerging_market_exposure_summary": (holdings_profile or {}).get("emerging_market_exposure_summary") if holdings_profile else extra.get("emerging_market_exposure_summary"),
            "ucits_status": doc_extracted.get("ucits_status"),
            "effective_duration": doc_extracted.get("effective_duration", extra.get("effective_duration")),
            "average_maturity": doc_extracted.get("average_maturity", extra.get("average_maturity")),
            "yield_proxy": doc_extracted.get("yield_proxy", extra.get("yield_proxy")),
            "credit_quality_mix": doc_extracted.get("credit_quality_mix", extra.get("credit_quality_mix")),
            "government_vs_corporate_split": doc_extracted.get("government_vs_corporate_split", extra.get("government_vs_corporate_split")),
            "interest_rate_sensitivity_proxy": doc_extracted.get("interest_rate_sensitivity_proxy", extra.get("interest_rate_sensitivity_proxy")),
            "issuer_concentration_proxy": doc_extracted.get("issuer_concentration_proxy", extra.get("issuer_concentration_proxy")),
            "weighted_average_maturity": doc_extracted.get("weighted_average_maturity", extra.get("weighted_average_maturity")),
            "portfolio_quality_summary": doc_extracted.get("portfolio_quality_summary", extra.get("portfolio_quality_summary")),
            "redemption_settlement_notes": doc_extracted.get("redemption_settlement_notes", extra.get("redemption_settlement_notes")),
            "sg_suitability_note": extra.get("sg_suitability_note"),
            "underlying_currency_exposure": doc_extracted.get("underlying_currency_exposure") or extra.get("underlying_currency_exposure"),
            "role_in_portfolio": extra.get("role_in_portfolio") or extra.get("bucket"),
            "implementation_method": extra.get("implementation_method") or canonical.get("instrument_type"),
            "cost_model": extra.get("cost_model") or extra.get("tracking_difference_note"),
            "liquidity_and_execution_constraints": extra.get("liquidity_and_execution_constraints") or canonical.get("liquidity_proxy"),
            "scenario_role": extra.get("scenario_role") or extra.get("bucket"),
            "governance_conditions": extra.get("governance_conditions") or extra.get("fallback_routing"),
            "max_loss_known": extra.get("max_loss_known"),
            "margin_required": extra.get("margin_required"),
            "short_options": extra.get("short_options"),
            "asset_type_classification": extra.get("asset_type_classification") or canonical.get("asset_class"),
            "inflation_linkage_rationale": extra.get("inflation_linkage_rationale"),
            "underlying_exposure_profile": extra.get("underlying_exposure_profile"),
            "distribution_policy": extra.get("distribution_policy") or canonical.get("share_class"),
            "tax_posture": tax_truth.get("withholding_tax_posture"),
            "freshness_state": "unknown",
        }
        for field_name, value in base_fields.items():
            if value is not None and not (isinstance(value, str) and not value.strip()):
                source_url = None
                doc_value = {
                    "isin": doc_extracted.get("isin"),
                    "fund_name": doc_extracted.get("fund_name"),
                    "issuer": doc_extracted.get("issuer"),
                    "domicile": doc_extracted.get("domicile"),
                    "share_class": doc_extracted.get("accumulating_status"),
                    "share_class_proven": doc_extracted.get("accumulating_status"),
                    "wrapper_or_vehicle_type": doc_extracted.get("wrapper_or_vehicle_type"),
                    "distribution_type": doc_extracted.get("accumulating_status"),
                    "replication_method": doc_extracted.get("replication_method"),
                    "expense_ratio": doc_extracted.get("ter"),
                    "benchmark_name": doc_extracted.get("benchmark_name"),
                    "factsheet_as_of": factsheet_asof if doc_available else None,
                    "primary_trading_currency": doc_extracted.get("primary_trading_currency"),
                    "primary_listing_exchange": doc_extracted.get("primary_listing_exchange"),
                    "launch_date": doc_extracted.get("launch_date") or doc_extracted.get("inception_date"),
                    "aum": doc_extracted.get("aum_usd"),
                    "tracking_difference_1y": doc_extracted.get("tracking_difference_1y"),
                    "tracking_difference_3y": doc_extracted.get("tracking_difference_3y"),
                    "tracking_difference_5y": doc_extracted.get("tracking_difference_5y"),
                    "ucits_status": doc_extracted.get("ucits_status"),
                    "effective_duration": doc_extracted.get("effective_duration"),
                    "average_maturity": doc_extracted.get("average_maturity"),
                    "yield_proxy": doc_extracted.get("yield_proxy"),
                    "credit_quality_mix": doc_extracted.get("credit_quality_mix"),
                    "government_vs_corporate_split": doc_extracted.get("government_vs_corporate_split"),
                    "interest_rate_sensitivity_proxy": doc_extracted.get("interest_rate_sensitivity_proxy"),
                    "issuer_concentration_proxy": doc_extracted.get("issuer_concentration_proxy"),
                    "weighted_average_maturity": doc_extracted.get("weighted_average_maturity"),
                    "portfolio_quality_summary": doc_extracted.get("portfolio_quality_summary"),
                    "redemption_settlement_notes": doc_extracted.get("redemption_settlement_notes"),
                    "underlying_currency_exposure": doc_extracted.get("underlying_currency_exposure"),
                }.get(field_name)
                citation_doc_value = {
                    "issuer": official_citation_extracted.get("issuer"),
                    "benchmark_name": official_citation_extracted.get("benchmark_name"),
                    "primary_trading_currency": official_citation_extracted.get("primary_trading_currency"),
                    "launch_date": official_citation_extracted.get("launch_date") or official_citation_extracted.get("inception_date"),
                    "aum": official_citation_extracted.get("aum_usd"),
                    "tracking_difference_1y": official_citation_extracted.get("tracking_difference_1y"),
                    "tracking_difference_3y": official_citation_extracted.get("tracking_difference_3y"),
                    "tracking_difference_5y": official_citation_extracted.get("tracking_difference_5y"),
                }.get(field_name)
                performance_metric_value = {
                    "tracking_difference_1y": (performance_metrics or {}).get("tracking_difference_1y"),
                    "tracking_difference_3y": (performance_metrics or {}).get("tracking_difference_3y"),
                    "tracking_difference_5y": (performance_metrics or {}).get("tracking_difference_5y"),
                }.get(field_name)
                factsheet_metric_value = {
                    "tracking_difference_1y": (factsheet_summary or {}).get("tracking_difference_1y") if factsheet_summary else None,
                    "tracking_difference_3y": (factsheet_summary or {}).get("tracking_difference_3y") if factsheet_summary else None,
                    "tracking_difference_5y": (factsheet_summary or {}).get("tracking_difference_5y") if factsheet_summary else None,
                    "tracking_error_1y": (factsheet_summary or {}).get("tracking_error_1y") if factsheet_summary else None,
                    "aum": (factsheet_summary or {}).get("latest_aum_usd") if factsheet_summary else None,
                }.get(field_name)
                if doc_available and _meaningful(doc_value):
                    source_name = "issuer_doc_parser"
                    provenance = "verified_official"
                    source_url = str(dict((doc_result or {}).get("factsheet") or {}).get("doc_url") or "") or None
                elif _meaningful(citation_doc_value):
                    source_name = "issuer_doc_parser"
                    provenance = "verified_official"
                    source_url = official_citation_source_url
                elif field_name in {"tracking_difference_1y", "tracking_difference_3y", "tracking_difference_5y"} and _meaningful(performance_metric_value):
                    source_name = str((performance_metrics or {}).get("source_name") or "validated_history_derivation")
                    provenance = "derived_from_validated_history"
                    source_url = str((performance_metrics or {}).get("source_url") or "") or None
                elif field_name in {"benchmark_key", "benchmark_name", "benchmark_assignment_method", "benchmark_assignment_proof", "benchmark_confidence"} and benchmark_assignment.get("validation_status") == "assigned":
                    source_name = "benchmark_registry"
                    provenance = "verified_mapping"
                elif field_name in {"us_weight", "em_weight", "top_10_concentration", "sector_concentration_proxy", "holdings_count", "developed_market_exposure_summary", "emerging_market_exposure_summary"} and holdings_profile:
                    source_name = "etf_holdings"
                    provenance = "verified_official"
                    source_url = str(dict((holdings_profile or {}).get("citation") or {}).get("source_url") or "") or None
                elif field_name in {"tracking_difference_1y", "tracking_difference_3y", "tracking_difference_5y", "tracking_error_1y", "aum"} and _meaningful(factsheet_metric_value):
                    source_name = "etf_factsheet_metrics"
                    provenance = "verified_official"
                    source_url = str(dict((factsheet_summary or {}).get("citation") or {}).get("source_url") or "") or None
                elif field_name == "aum" and cached_provider_aum and value == cached_provider_aum.get("value"):
                    source_name = str(cached_provider_aum.get("source_name") or "provider_cache")
                    provenance = "verified_nonissuer"
                elif (
                    field_name in {"aum", "primary_trading_currency", "launch_date", "issuer"}
                    and yfinance_quote
                    and value == yfinance_quote.get(
                        "aum"
                        if field_name == "aum"
                        else "currency"
                        if field_name == "primary_trading_currency"
                        else "launch_date"
                        if field_name == "launch_date"
                        else "issuer"
                    )
                ):
                    source_name = "Yahoo Finance"
                    provenance = "verified_nonissuer"
                    source_url = f"https://finance.yahoo.com/quote/{yfinance_quote.get('symbol_used')}"
                elif field_name in {"issuer", "isin", "domicile"} and (
                    (field_name == "issuer" and _meaningful(doc_registry_issuer) and str(value).strip().upper() == str(doc_registry_issuer).strip().upper())
                    or (field_name == "isin" and _meaningful(doc_registry_expected_isin) and str(value).strip().upper() == str(doc_registry_expected_isin).strip().upper())
                    or (field_name == "domicile" and _meaningful(doc_registry_domicile) and str(value).strip().upper() == str(doc_registry_domicile).strip().upper())
                ):
                    source_name = "issuer_doc_registry"
                    provenance = "verified_nonissuer"
                elif field_name in {"bid_ask_spread_proxy", "volume_30d_avg"} and latest_market_data:
                    source_name = "etf_market_data"
                    provenance = "verified_official"
                    source_url = str(dict((market_summary or {}).get("citation") or {}).get("source_url") or "") or None
                elif field_name in {"withholding_tax_posture", "estate_risk_posture", "tax_posture", "sg_suitability_note", "distribution_policy"}:
                    source_name = "tax_engine"
                    provenance = "inferred"
                elif field_name == "source_state":
                    source_name = "canonical_instrument"
                    provenance = "verified_nonissuer"
                else:
                    source_name = "candidate_registry"
                    provenance = "seeded_fallback"
                upsert_field_observation(
                    conn,
                    candidate_symbol=normalized,
                    sleeve_key=str(membership.get("sleeve_key") or ""),
                    field_name=field_name,
                    value=value,
                    source_name=source_name,
                    source_url=source_url,
                    observed_at=str(observed_at),
                    provenance_level=provenance,
                    confidence_label="medium" if object_type == LIVE_OBJECT_TYPE else "low",
                    parser_method="refresh_registry_candidate_truth",
                    override_annotation=(
                        {
                            "freshness_state": str((cached_provider_aum or {}).get("freshness_state") or "current"),
                            "observed_at": cached_provider_aum.get("observed_at"),
                            "recovery_run_id": recovery_run_id,
                            "degraded": str((cached_provider_aum or {}).get("freshness_state") or "").lower() in {"stale", "aging", "expected_lag"},
                            "extraction_confidence": "provider_cache_aum",
                        }
                        if field_name == "aum" and cached_provider_aum and value == cached_provider_aum.get("value")
                        else (
                            {
                                "symbol_used": yfinance_quote.get("symbol_used"),
                                "observed_at": yfinance_quote.get("fetched_at"),
                                "recovery_run_id": recovery_run_id,
                                "extraction_confidence": "yfinance_quote",
                            }
                            if (
                                field_name in {"aum", "primary_trading_currency", "launch_date", "issuer"}
                                and yfinance_quote
                                and value == yfinance_quote.get(
                                    "aum"
                                    if field_name == "aum"
                                    else "currency"
                                    if field_name == "primary_trading_currency"
                                    else "launch_date"
                                    if field_name == "launch_date"
                                    else "issuer"
                                )
                            )
                            else ({"recovery_run_id": recovery_run_id} if recovery_run_id else None)
                        )
                    ),
                )
            else:
                upsert_field_observation(
                    conn,
                    candidate_symbol=normalized,
                    sleeve_key=str(membership.get("sleeve_key") or ""),
                    field_name=field_name,
                    value=None,
                    source_name="candidate_registry",
                    observed_at=str(observed_at),
                    provenance_level="seeded_fallback",
                    confidence_label="low",
                    parser_method="refresh_registry_candidate_truth",
                    missingness_reason=_missingness_for_field(field_name),
                    override_annotation={"recovery_run_id": recovery_run_id} if recovery_run_id else None,
                )
        _reconcile_preferred_observations(
            conn,
            candidate_symbol=normalized,
            sleeve_key=str(membership.get("sleeve_key") or ""),
            doc_extracted=doc_extracted,
            doc_registry_expected_isin=doc_registry_expected_isin,
            doc_registry_domicile=doc_registry_domicile,
            doc_registry_issuer=doc_registry_issuer,
            candidate_registry_currency=selected_trading_currency,
            benchmark_assignment=benchmark_assignment,
            canonical_benchmark_key=str(canonical.get("benchmark_key") or extra.get("benchmark_key") or ""),
            cached_provider_aum=cached_provider_aum,
            factsheet_summary=factsheet_summary,
            recovery_run_id=recovery_run_id,
        )
        apply_targeted_source_completion_overrides(
            conn,
            candidate_symbol=normalized,
            sleeve_key=str(membership.get("sleeve_key") or ""),
            recovery_run_id=recovery_run_id,
        )
        resolve_candidate_field_truth(conn, candidate_symbol=normalized, sleeve_key=str(membership.get("sleeve_key") or ""))
        if (
            activate_market_series
            and not market_series_activated
            and instrument_type in {"etf_ucits", "etf_us"}
            and object_type == LIVE_OBJECT_TYPE
        ):
            from app.v2.blueprint_market.market_identity import candidate_id_for_symbol
            from app.v2.blueprint_market.series_refresh_service import refresh_candidate_series

            refresh_candidate_series(
                conn,
                candidate_id=candidate_id_for_symbol(normalized),
                stale_only=True,
            )
            market_series_activated = True
        try:
            from app.v2.blueprint_market.coverage import build_candidate_coverage_summary

            coverage_summary = build_candidate_coverage_summary(
                conn,
                {**candidate_for_assignment, **candidate_for_assignment.get("extra", {}), **canonical, **membership},
                {},
                market_path_support=None,
            )
        except Exception:
            coverage_summary = {}
        current_attempt_status = str(dict(coverage_summary.get("coverage_workflow_summary") or {}).get("direct_history_attempt_status") or "").strip()
        route_stability_state = str(dict(coverage_summary.get("coverage_workflow_summary") or {}).get("route_stability_state") or "").strip()
        direct_history_depth = int(coverage_summary.get("direct_history_depth") or 0)
        proxy_history_depth = int(coverage_summary.get("proxy_history_depth") or 0)
        if (
            activate_market_series
            and instrument_type in {"etf_ucits", "etf_us"}
            and object_type == LIVE_OBJECT_TYPE
            and route_stability_state == "stable"
            and direct_history_depth == 0
            and not current_attempt_status
        ):
            from app.v2.blueprint_market.market_identity import candidate_id_for_symbol
            from app.v2.blueprint_market.series_refresh_service import refresh_candidate_series

            try:
                refresh_candidate_series(
                    conn,
                    candidate_id=candidate_id_for_symbol(normalized),
                    stale_only=False,
                )
                coverage_summary = build_candidate_coverage_summary(
                    conn,
                    {**candidate_for_assignment, **candidate_for_assignment.get("extra", {}), **canonical, **membership},
                    {},
                    market_path_support=None,
                )
                direct_history_depth = int(coverage_summary.get("direct_history_depth") or 0)
                proxy_history_depth = int(coverage_summary.get("proxy_history_depth") or 0)
            except Exception:
                pass
        runtime_liquidity_proxy = None
        if direct_history_depth >= 260:
            runtime_liquidity_proxy = "direct_history_backed"
        elif proxy_history_depth >= 260:
            runtime_liquidity_proxy = "proxy_history_backed"
        coverage_fields = {
            "route_validity_state": dict(coverage_summary.get("coverage_workflow_summary") or {}).get("status"),
            "direct_history_depth": direct_history_depth,
            "proxy_history_depth": proxy_history_depth,
            "liquidity_proxy": runtime_liquidity_proxy,
        }
        for field_name, field_value in coverage_fields.items():
            if field_value in {None, ""}:
                continue
            upsert_field_observation(
                conn,
                candidate_symbol=normalized,
                sleeve_key=str(membership.get("sleeve_key") or ""),
                field_name=field_name,
                value=field_value,
                source_name="market_route_runtime",
                observed_at=str(market_asof or now),
                provenance_level="derived_from_validated_history",
                confidence_label="medium",
                parser_method="refresh_registry_candidate_truth:coverage",
                override_annotation={"recovery_run_id": recovery_run_id} if recovery_run_id else None,
            )
        resolve_candidate_field_truth(conn, candidate_symbol=normalized, sleeve_key=str(membership.get("sleeve_key") or ""))
        compute_candidate_completeness(
            conn,
            candidate={
                "symbol": normalized,
                "sleeve_key": str(membership.get("sleeve_key") or ""),
                "instrument_type": instrument_type,
                "eligibility": {},
            },
        )
    conn.commit()
    return {
        "symbol": normalized,
        "updated": True,
        "recovery_run_id": recovery_run_id,
        "source_state": source_state,
        "factsheet_asof": factsheet_asof,
        "market_data_asof": market_asof,
        "object_type": object_type,
    }


def _infer_issuer(item: dict[str, Any]) -> str:
    symbol = str(item.get("symbol") or "").upper()
    name = str(item.get("name") or "").lower()
    if symbol.startswith("VW") or "vanguard" in name:
        return "Vanguard"
    if symbol in {"IWDA", "SSAC", "CSPX", "EIMI", "AGGU", "IB01", "SGLN", "IWDP"} or "ishares" in name:
        return "iShares"
    if symbol.startswith("X") or "xtrackers" in name:
        return "Xtrackers"
    if "hsbc" in name or symbol == "HMCH":
        return "HSBC Asset Management"
    if symbol == "A35":
        return "Amova Asset Management"
    if symbol == "CMOD":
        return "Invesco"
    if symbol == "TAIL":
        return "Cambria"
    if symbol == "CAOS":
        return "Alpha Architect"
    if symbol == "DBMF":
        return "Natixis / iMGP"
    if symbol == "KMLM":
        return "KFA Funds"
    if symbol == "BIL":
        return "State Street / SPDR"
    return "Issuer"


def _infer_asset_class(item: dict[str, Any]) -> str:
    sleeve_key = str(item.get("sleeve_key") or "")
    instrument_type = str(item.get("instrument_type") or "etf_ucits")
    if sleeve_key in {"global_equity_core", "developed_ex_us_optional", "emerging_markets", "china_satellite"}:
        return "equity"
    if sleeve_key in {"ig_bonds", "cash_bills"}:
        return "fixed_income"
    if sleeve_key in {"real_assets", "alternatives"}:
        return "real_assets"
    if sleeve_key == "convex":
        return "convex"
    if instrument_type == "cash_account_sg":
        return "cash"
    return "other"
