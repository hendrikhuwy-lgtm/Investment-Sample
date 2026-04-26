from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import sqlite3
import statistics
import subprocess
import uuid
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from pydantic import BaseModel

from app.config import Settings, get_db_path
from app.models.db import connect, init_db
from app.models.types import Citation, ConvexSleevePosition, InstrumentTaxProfile, SourceRecord, TaxResidencyProfile
from app.services.convex_engine import validate_retail_safe_convex
from app.services.brief_action_tags import tag_items
from app.services.brief_approval import create_or_refresh_approval
from app.services.brief_delivery_state import record_brief_versions
from app.services.brief_history_compare import build_history_compare, record_regime_history
from app.services.brief_policy_pack import POLICY_PACK_VERSION, build_policy_pack
from app.services.brief_visuals import build_brief_charts
from app.services.chart_cache import store_chart_artifact
from app.services.citation_health import summarize_policy_citation_health
from app.services.cma_engine import current_cma_version
from app.services.data_lag import classify_lag_cause, compute_lag_days as compute_lag_days_china
from app.services.ingest_mcp import MCPIngestionResult, ingest_mcp_omni
from app.services.instrument_mapping import build_implementation_mapping
from app.services.ingest_web import WEB_SOURCES, fetch_web_sources
from app.services.ips_generator import persist_ips_snapshot
from app.services.language_safety import assert_no_directive_language
from app.services.normalize import CitationPolicyError
from app.services.reporting import ReportBuildError, build_narrated_email_brief, write_narrated_email_files
from app.services.regime_methodology import list_regime_methodology
from app.services.provider_refresh import refresh_daily_brief_provider_snapshots
from app.services.scenario_registry import record_scenario_comparisons
from app.services.signals import extended_market_signals, signal_methodology_registry, summarize_signal_state
from app.services.tax_engine import compare_equivalent_exposures


PROJECT_ROOT = Path(__file__).resolve().parents[3]
BACKEND_ROOT = Path(__file__).resolve().parents[2]
SCHEMA_PATH = BACKEND_ROOT / "app" / "storage" / "schema.sql"
OUTBOX_DIR = PROJECT_ROOT / "outbox"
CACHE_DIR = OUTBOX_DIR / "live_cache"
SGT = ZoneInfo("Asia/Singapore")

FRED_SERIES = [
    ("DGS10", "US 10Y Treasury Yield", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DGS10"),
    (
        "T10YIE",
        "US 10Y Breakeven Inflation",
        "https://fred.stlouisfed.org/graph/fredgraph.csv?id=T10YIE",
    ),
    ("SP500", "S&P 500 Index", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=SP500"),
    ("VIXCLS", "CBOE VIX", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=VIXCLS"),
    (
        "BAMLH0A0HYM2",
        "US High Yield OAS",
        "https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2",
    ),
    ("T10Y2Y", "US 10Y-2Y Treasury Curve", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=T10Y2Y"),
    ("DCOILWTICO", "WTI Crude Oil Spot Price", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DCOILWTICO"),
    ("DTWEXBGS", "US Dollar Broad Index", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DTWEXBGS"),
    ("VXEEMCLS", "CBOE Emerging Markets ETF Volatility", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=VXEEMCLS"),
    ("DEXSIUS", "SGD per USD", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=DEXSIUS"),
    ("IRLTLT01SGM156N", "Singapore 10Y Government Yield", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=IRLTLT01SGM156N"),
    ("IRLTLT01EZM156N", "Euro Area 10Y Government Yield", "https://fred.stlouisfed.org/graph/fredgraph.csv?id=IRLTLT01EZM156N"),
]
FRED_SOURCE_BY_CODE = {code: f"fred_{code.lower()}" for code, _label, _url in FRED_SERIES}
REQUIRED_FRED_SERIES = {"DGS10", "T10YIE", "SP500", "VIXCLS", "BAMLH0A0HYM2"}


class OmniBriefError(RuntimeError):
    pass


def _jsonable(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _sparkline(values: list[float]) -> str:
    if not values:
        return ""
    blocks = "▁▂▃▄▅▆▇█"
    low = min(values)
    high = max(values)
    if high == low:
        return blocks[0] * len(values)
    return "".join(blocks[int((value - low) / (high - low) * (len(blocks) - 1))] for value in values)


def _range_bar(value: float, low: float, high: float, width: int = 24) -> str:
    if high <= low:
        pct = 0.5
    else:
        pct = max(0.0, min(1.0, (value - low) / (high - low)))
    filled = int(round(width * pct))
    return "█" * filled + "░" * (width - filled)


def _citation(
    url: str,
    source_id: str,
    retrieved_at: datetime,
    importance: str,
    cached: bool = False,
    observed_at: str | None = None,
    lag_days: int | None = None,
    lag_class: str | None = None,
    lag_cause: str | None = None,
) -> Citation:
    label = f"{importance}; retrieval={'cached' if cached else 'live'}"
    return Citation(
        url=url,
        source_id=source_id,
        retrieved_at=retrieved_at,
        importance=label,
        observed_at=observed_at,
        lag_days=lag_days,
        lag_class=lag_class,
        lag_cause=lag_cause,
    )


def _resolve_tz(timezone: str) -> ZoneInfo:
    normalized = timezone.strip()
    if normalized.lower() in {"asia singapore", "asia/singapore"}:
        return ZoneInfo("Asia/Singapore")
    try:
        return ZoneInfo(normalized)
    except Exception:
        return ZoneInfo("Asia/Singapore")


def compute_lag_days(
    observation_date: str,
    retrieved_at: datetime,
    timezone: str = "Asia Singapore",
) -> tuple[int | None, str | None]:
    normalized_tz = "Asia/Shanghai" if timezone.strip().lower() in {"asia singapore", "asia/singapore"} else timezone
    return compute_lag_days_china(observation_date, retrieved_at, timezone=normalized_tz)


def _file_timestamp(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="replace")


def _fallback_sg10_series_from_mas() -> list[tuple[str, float]]:
    try:
        import requests
        from bs4 import BeautifulSoup
    except Exception:
        return []
    try:
        html = requests.get(
            "https://eservices.mas.gov.sg/statistics/fdanet/BondOriginalMaturities.aspx?type=NX",
            timeout=30,
            headers={"User-Agent": "Mozilla/5.0"},
        ).text
        soup = BeautifulSoup(html, "html.parser")
        table = soup.find("table", id="ContentPlaceHolder1_OriginalMaturitiesTable")
        if table is None:
            return []
        rows = table.find_all("tr")
        if len(rows) < 3:
            return []
        yield_headers: list[int] = []
        header_cells = rows[1].find_all(["th", "td"])
        for idx, cell in enumerate(header_cells[1:], start=1):
            if cell.get_text(" ", strip=True).lower() == "yield":
                yield_headers.append(idx + 1)
        if not yield_headers:
            return []
        latest_yield_col = yield_headers[-1]
        series: list[tuple[str, float]] = []
        for row in rows[2:]:
            cells = row.find_all("td")
            if len(cells) <= latest_yield_col:
                continue
            date_text = cells[0].get_text(" ", strip=True)
            value_text = cells[latest_yield_col].get_text(" ", strip=True)
            if not date_text or not value_text:
                continue
            try:
                parsed_date = datetime.strptime(date_text, "%d %b %Y").date().isoformat()
                value = float(value_text)
            except Exception:
                continue
            series.append((parsed_date, value))
        return sorted(series)
    except Exception:
        return []


def _refresh_live_cache(settings: Settings) -> tuple[bool, str]:
    if not settings.refresh_live_cache_on_brief:
        return False, "refresh disabled by config"

    script = BACKEND_ROOT / "scripts" / "refresh_live_cache.sh"
    result = subprocess.run([str(script)], capture_output=True, text=True, check=False)
    if result.returncode == 0:
        return True, result.stdout.strip() or "cache refresh succeeded"
    return False, (result.stderr.strip() or result.stdout.strip() or "cache refresh failed")


def _load_series_from_cache(force_cache_only: bool) -> tuple[dict[str, dict[str, Any]], list[Citation], bool]:
    summary: dict[str, dict[str, Any]] = {}
    citations: list[Citation] = []
    cached_used = force_cache_only

    for code, label, url in FRED_SERIES:
        path = CACHE_DIR / f"{code}.csv"
        if not path.exists():
            if code == "IRLTLT01SGM156N":
                clean = _fallback_sg10_series_from_mas()
                if len(clean) >= 6:
                    latest_date = clean[-1][0]
                    retrieved_at = datetime.now(UTC)
                    lag_days, lag_class = compute_lag_days(latest_date, retrieved_at, timezone="Asia/Shanghai")
                    lag_cause = classify_lag_cause(
                        series_key=code,
                        observed_at=latest_date,
                        retrieved_at=retrieved_at,
                        lag_days=lag_days,
                        retrieval_succeeded=True,
                        cache_fallback_used=True,
                        latest_available_matches_observed=True,
                        previous_observed_at=clean[-2][0] if len(clean) >= 2 else None,
                    )
                    latest_value = clean[-1][1]
                    prior_date, prior_value = clean[-6]
                    history_20 = clean[-20:]
                    values_20 = [value for _, value in history_20]
                    history_60 = clean[-60:] if len(clean) >= 60 else clean
                    values_60 = [value for _, value in history_60]
                    one_day_change = (latest_value - clean[-2][1]) if len(clean) >= 2 else None
                    summary[code] = {
                        "label": label,
                        "latest_date": latest_date,
                        "latest_value": latest_value,
                        "observed_at": latest_date,
                        "lag_days": lag_days,
                        "lag_class": lag_class,
                        "lag_cause": lag_cause,
                        "prior_date": prior_date,
                        "prior_value": prior_value,
                        "change_5obs": latest_value - prior_value,
                        "change_1d": one_day_change,
                        "sparkline": _sparkline(values_20),
                        "range_bar": _range_bar(latest_value, min(values_20), max(values_20)),
                        "points": values_20,
                        "change_20obs": latest_value - clean[-21][1] if len(clean) >= 21 else None,
                        "sparkline_60": _sparkline(values_60),
                        "range_bar_60": _range_bar(latest_value, min(values_60), max(values_60)),
                        "points_60": values_60,
                        "history": clean,
                    }
                    citations.append(
                        _citation(
                            url="https://eservices.mas.gov.sg/statistics/fdanet/BondOriginalMaturities.aspx?type=NX",
                            source_id="mas_sg10_proxy",
                            retrieved_at=retrieved_at,
                            importance=f"MAS historical 10-year SGS proxy for {label}",
                            cached=True,
                            observed_at=latest_date,
                            lag_days=lag_days,
                            lag_class=lag_class,
                            lag_cause=lag_cause,
                        )
                    )
                    continue
            if code in REQUIRED_FRED_SERIES:
                raise OmniBriefError(f"Missing cache file for {code}: {path}")
            continue

        rows = list(csv.DictReader(_read_text(path).splitlines()))
        fallback_source = False
        clean: list[tuple[str, float]] = []
        for row in rows:
            value = row.get(code)
            if not value or value == ".":
                continue
            date_value = row.get("DATE") or row.get("observation_date")
            if not date_value:
                continue
            clean.append((date_value, float(value)))

        if len(clean) < 6:
            if code == "IRLTLT01SGM156N":
                clean = _fallback_sg10_series_from_mas()
                fallback_source = len(clean) >= 6
            if code in REQUIRED_FRED_SERIES:
                raise OmniBriefError(f"Insufficient points for {code}")
            if len(clean) < 6:
                continue

        latest_date, latest_value = clean[-1]
        prior_date, prior_value = clean[-6]
        history_20 = clean[-20:]
        values_20 = [value for _, value in history_20]
        history_60 = clean[-60:] if len(clean) >= 60 else clean
        values_60 = [value for _, value in history_60]

        retrieved_at = datetime.now(UTC) if fallback_source else _file_timestamp(path)
        lag_days, lag_class = compute_lag_days(latest_date, retrieved_at, timezone="Asia/Shanghai")
        lag_cause = classify_lag_cause(
            series_key=code,
            observed_at=latest_date,
            retrieved_at=retrieved_at,
            lag_days=lag_days,
            retrieval_succeeded=(not force_cache_only) or fallback_source,
            cache_fallback_used=force_cache_only or fallback_source,
            latest_available_matches_observed=True,
            previous_observed_at=clean[-2][0] if len(clean) >= 2 else None,
        )
        one_day_change = (latest_value - clean[-2][1]) if len(clean) >= 2 else None

        summary[code] = {
            "label": label,
            "latest_date": latest_date,
            "latest_value": latest_value,
            "observed_at": latest_date,
            "lag_days": lag_days,
            "lag_class": lag_class,
            "lag_cause": lag_cause,
            "prior_date": prior_date,
            "prior_value": prior_value,
            "change_5obs": latest_value - prior_value,
            "change_1d": one_day_change,
            "sparkline": _sparkline(values_20),
            "range_bar": _range_bar(latest_value, min(values_20), max(values_20)),
            "points": values_20,
            "change_20obs": latest_value - clean[-21][1] if len(clean) >= 21 else None,
            "sparkline_60": _sparkline(values_60),
            "range_bar_60": _range_bar(latest_value, min(values_60), max(values_60)),
            "points_60": values_60,
            "history": clean,
        }

        citations.append(
            _citation(
                url="https://eservices.mas.gov.sg/statistics/fdanet/BondOriginalMaturities.aspx?type=NX" if fallback_source and code == "IRLTLT01SGM156N" else url,
                source_id="mas_sg10_proxy" if fallback_source and code == "IRLTLT01SGM156N" else f"fred_{code.lower()}",
                retrieved_at=retrieved_at,
                importance=f"{'MAS historical 10-year SGS proxy' if fallback_source and code == 'IRLTLT01SGM156N' else 'Official FRED series'} for {label}",
                cached=force_cache_only or fallback_source,
                observed_at=latest_date,
                lag_days=lag_days,
                lag_class=lag_class,
                lag_cause=lag_cause,
            )
        )

    return summary, citations, cached_used


def _trajectory_and_pattern(values: list[float]) -> tuple[str, str]:
    if len(values) < 3:
        return "insufficient data", "pattern unresolved"

    delta = values[-1] - values[0]
    if abs(delta) <= max(1e-9, abs(values[0]) * 0.005):
        trajectory = "sideways"
    elif delta > 0:
        trajectory = "upward"
    else:
        trajectory = "downward"

    diffs = [abs(values[i] - values[i - 1]) for i in range(1, len(values))]
    avg_move = sum(diffs) / len(diffs)
    level = max(abs(values[-1]), 1e-9)
    vol_ratio = avg_move / level
    if vol_ratio > 0.02:
        pattern = "high variability"
    elif vol_ratio > 0.01:
        pattern = "moderate variability"
    else:
        pattern = "stable variability"
    return trajectory, pattern


def _build_volume_row(
    metric_label: str,
    url: str,
    source_id: str,
    series: list[tuple[str, float]],
    retrieved_at: datetime,
    force_cache_only: bool,
    publisher: str,
    source_payload: str,
) -> tuple[dict[str, Any], Citation, SourceRecord]:
    last_twenty = series[-20:]
    values = [value for _, value in last_twenty]
    latest_date, latest_value = series[-1]
    prior_date, prior_value = series[-6]
    trajectory, pattern = _trajectory_and_pattern(values)

    lag_days, lag_class = compute_lag_days(latest_date, retrieved_at, timezone="Asia/Shanghai")
    lag_cause = classify_lag_cause(
        series_key=source_id,
        observed_at=latest_date,
        retrieved_at=retrieved_at,
        lag_days=lag_days,
        retrieval_succeeded=not force_cache_only,
        cache_fallback_used=force_cache_only,
        latest_available_matches_observed=True,
        previous_observed_at=series[-2][0] if len(series) >= 2 else None,
    )

    row = {
        "metric": f"{metric_label} ({latest_date})",
        "latest": f"{latest_value:,.0f}",
        "delta_5": f"{(latest_value - prior_value):+,.0f}",
        "trajectory": trajectory,
        "pattern": pattern,
        "sparkline": _sparkline(values),
        "range_bar": _range_bar(latest_value, min(values), max(values)),
        "latest_date": latest_date,
        "observed_at": latest_date,
        "lag_days": lag_days,
        "lag_class": lag_class,
        "lag_cause": lag_cause,
        "prior_date": prior_date,
        "latest_value": latest_value,
        "change_5obs": latest_value - prior_value,
        "points": values,
    }

    citation = _citation(
        url=url,
        source_id=source_id,
        retrieved_at=retrieved_at,
        importance=f"Public volume indicator source for {metric_label}",
        cached=force_cache_only,
        observed_at=latest_date,
        lag_days=lag_days,
        lag_class=lag_class,
        lag_cause=lag_cause,
    )
    row["citation"] = citation

    record = SourceRecord(
        source_id=source_id,
        url=url,
        publisher=publisher,
        retrieved_at=retrieved_at,
        topic="volume",
        credibility_tier="secondary",
        raw_hash=hashlib.sha256(source_payload.encode("utf-8")).hexdigest(),
        source_type="web",
    )

    return row, citation, record


def _load_volume_indicators_from_cache(force_cache_only: bool) -> tuple[list[dict[str, Any]], list[Citation], list[SourceRecord]]:
    rows: list[dict[str, Any]] = []
    citations: list[Citation] = []
    records: list[SourceRecord] = []
    stooq_specs = [
        (
            "stooq_spy_volume.csv",
            "US Large-Cap Index Participation Proxy (SPY Volume)",
            "https://stooq.com/q/d/l/?s=spy.us&i=d",
            "stooq_spy_volume",
        ),
        (
            "stooq_qqq_volume.csv",
            "US ETF Volume (QQQ)",
            "https://stooq.com/q/d/l/?s=qqq.us&i=d",
            "stooq_qqq_volume",
        ),
    ]
    yahoo_specs = [
        (
            "yahoo_spx_chart.json",
            "S&P 500 Index Volume Proxy (^GSPC)",
            "https://query1.finance.yahoo.com/v8/finance/chart/%5EGSPC?range=6mo&interval=1d",
            "yahoo_gspc_volume",
        ),
        (
            "yahoo_spy_chart.json",
            "SPY ETF Volume",
            "https://query1.finance.yahoo.com/v8/finance/chart/SPY?range=6mo&interval=1d",
            "yahoo_spy_volume",
        ),
    ]

    for filename, metric_label, url, source_id in stooq_specs:
        path = CACHE_DIR / filename
        if not path.exists():
            continue

        payload_text = _read_text(path)
        series_rows = list(csv.DictReader(payload_text.splitlines()))
        series: list[tuple[str, float]] = []
        for item in series_rows:
            date_value = item.get("Date") or item.get("DATE")
            volume_value = item.get("Volume") or item.get("VOLUME")
            if not date_value or not volume_value:
                continue
            try:
                volume = float(volume_value)
            except ValueError:
                continue
            if volume <= 0:
                continue
            series.append((date_value, volume))
        if len(series) < 6:
            continue

        row, citation, record = _build_volume_row(
            metric_label=metric_label,
            url=url,
            source_id=source_id,
            series=series,
            retrieved_at=_file_timestamp(path),
            force_cache_only=force_cache_only,
            publisher="Stooq",
            source_payload=payload_text,
        )
        rows.append(row)
        citations.append(citation)
        records.append(record)

    # Fallback to cached Yahoo responses if Stooq data is unavailable.
    for filename, metric_label, url, source_id in yahoo_specs:
        if len(rows) >= 2:
            break
        path = CACHE_DIR / filename
        if not path.exists():
            continue

        payload_text = _read_text(path)
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            continue

        result = ((payload.get("chart") or {}).get("result") or [{}])[0]
        indicators = result.get("indicators", {})
        quote = (indicators.get("quote") or [{}])[0]
        volumes = quote.get("volume") or []
        timestamps = result.get("timestamp") or []

        series: list[tuple[str, float]] = []
        for ts, vol in zip(timestamps, volumes):
            if vol is None or float(vol) <= 0:
                continue
            series.append((datetime.fromtimestamp(int(ts), tz=UTC).date().isoformat(), float(vol)))
        if len(series) < 6:
            continue

        row, citation, record = _build_volume_row(
            metric_label=metric_label,
            url=url,
            source_id=source_id,
            series=series,
            retrieved_at=_file_timestamp(path),
            force_cache_only=force_cache_only,
            publisher="Yahoo Finance",
            source_payload=payload_text,
        )
        rows.append(row)
        citations.append(citation)
        records.append(record)

    return rows, citations, records


def _load_sti_proxy_from_cache(force_cache_only: bool) -> tuple[dict[str, Any] | None, Citation | None, SourceRecord | None]:
    path = CACHE_DIR / "yahoo_sti_chart.json"
    series: list[tuple[str, float]] = []
    payload_text = ""
    source_url = "https://query1.finance.yahoo.com/v8/finance/chart/%5ESTI?range=6mo&interval=1d"
    source_id = "yahoo_sti_proxy"
    publisher = "Yahoo Finance"
    if path.exists():
        payload_text = _read_text(path)
        try:
            payload = json.loads(payload_text)
            result = ((payload.get("chart") or {}).get("result") or [{}])[0]
            timestamps = result.get("timestamp") or []
            quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
            closes = quote.get("close") or []
            for ts, close in zip(timestamps, closes):
                if close is None:
                    continue
                series.append((datetime.fromtimestamp(int(ts), tz=UTC).date().isoformat(), float(close)))
        except json.JSONDecodeError:
            series = []
    if len(series) < 6:
        fallback = CACHE_DIR / "stooq_sti_proxy.csv"
        if not fallback.exists():
            return None, None, None
        payload_text = _read_text(fallback)
        lines = [line.strip() for line in payload_text.splitlines() if line.strip()]
        if len(lines) < 7 or not lines[0].lower().startswith("observation_date,sti_proxy"):
            return None, None, None
        series = []
        for line in lines[1:]:
            date_text, value_text = (line.split(",", 1) + [""])[:2]
            try:
                series.append((date_text.strip(), float(value_text.strip())))
            except ValueError:
                continue
        if len(series) < 6:
            return None, None, None
        path = fallback
        source_url = "https://stooq.com/q/d/l/?s=%5Esti&i=d"
        source_id = "stooq_sti_proxy"
        publisher = "Stooq"
    latest_date, latest_value = series[-1]
    prior_value = series[-6][1]
    retrieved_at = _file_timestamp(path)
    lag_days, lag_class = compute_lag_days(latest_date, retrieved_at, timezone="Asia/Singapore")
    lag_cause = classify_lag_cause(
        series_key="yahoo_sti_close",
        observed_at=latest_date,
        retrieved_at=retrieved_at,
        lag_days=lag_days,
        retrieval_succeeded=not force_cache_only,
        cache_fallback_used=force_cache_only,
        latest_available_matches_observed=True,
        previous_observed_at=series[-2][0] if len(series) >= 2 else None,
    )
    row = {
        "metric": f"STI proxy ({latest_date})",
        "latest_value": latest_value,
        "change_5obs": latest_value - prior_value,
        "latest_date": latest_date,
        "observed_at": latest_date,
        "lag_days": lag_days,
        "lag_class": lag_class,
        "lag_cause": lag_cause,
    }
    citation = _citation(
        url=source_url,
        source_id=source_id,
        retrieved_at=retrieved_at,
        importance="Public STI local equity proxy",
        cached=force_cache_only,
        observed_at=latest_date,
        lag_days=lag_days,
        lag_class=lag_class,
        lag_cause=lag_cause,
    )
    record = SourceRecord(
        source_id=source_id,
        url=source_url,
        publisher=publisher,
        retrieved_at=retrieved_at,
        topic="singapore_equity",
        credibility_tier="secondary",
        raw_hash=hashlib.sha256(payload_text.encode("utf-8")).hexdigest(),
        source_type="web",
    )
    return row, citation, record


def _load_vea_proxy_from_cache(force_cache_only: bool) -> tuple[dict[str, Any] | None, Citation | None, SourceRecord | None]:
    path = CACHE_DIR / "yahoo_vea_chart.json"
    if not path.exists():
        return None, None, None
    payload_text = _read_text(path)
    series: list[tuple[str, float]] = []
    try:
        payload = json.loads(payload_text)
        result = ((payload.get("chart") or {}).get("result") or [{}])[0]
        timestamps = result.get("timestamp") or []
        quote = ((result.get("indicators") or {}).get("quote") or [{}])[0]
        closes = quote.get("close") or []
        for ts, close in zip(timestamps, closes):
            if close is None:
                continue
            series.append((datetime.fromtimestamp(int(ts), tz=UTC).date().isoformat(), float(close)))
    except json.JSONDecodeError:
        return None, None, None
    if len(series) < 6:
        return None, None, None
    latest_date, latest_value = series[-1]
    prior_value = series[-6][1]
    retrieved_at = _file_timestamp(path)
    lag_days, lag_class = compute_lag_days(latest_date, retrieved_at, timezone="Asia/Singapore")
    lag_cause = classify_lag_cause(
        series_key="yahoo_vea_close",
        observed_at=latest_date,
        retrieved_at=retrieved_at,
        lag_days=lag_days,
        retrieval_succeeded=not force_cache_only,
        cache_fallback_used=force_cache_only,
        latest_available_matches_observed=True,
        previous_observed_at=series[-2][0] if len(series) >= 2 else None,
    )
    row = {
        "metric": f"VEA proxy ({latest_date})",
        "latest_value": latest_value,
        "change_5obs": latest_value - prior_value,
        "latest_date": latest_date,
        "observed_at": latest_date,
        "lag_days": lag_days,
        "lag_class": lag_class,
        "lag_cause": lag_cause,
    }
    citation = _citation(
        url="https://query1.finance.yahoo.com/v8/finance/chart/VEA?range=6mo&interval=1d",
        source_id="yahoo_vea_proxy",
        retrieved_at=retrieved_at,
        importance="Public developed ex-US equity proxy",
        cached=force_cache_only,
        observed_at=latest_date,
        lag_days=lag_days,
        lag_class=lag_class,
        lag_cause=lag_cause,
    )
    record = SourceRecord(
        source_id="yahoo_vea_proxy",
        url="https://query1.finance.yahoo.com/v8/finance/chart/VEA?range=6mo&interval=1d",
        publisher="Yahoo Finance",
        retrieved_at=retrieved_at,
        topic="developed_ex_us_equity",
        credibility_tier="secondary",
        raw_hash=hashlib.sha256(payload_text.encode("utf-8")).hexdigest(),
        source_type="web",
    )
    return row, citation, record


def _parse_iso_date(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S"):
        try:
            parsed = datetime.strptime(text, fmt)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=UTC)
            return parsed.astimezone(UTC)
        except ValueError:
            continue
    return None


def _parse_date_only(value: str) -> datetime.date | None:
    parsed = _parse_iso_date(value)
    if parsed is None:
        return None
    return parsed.date()


def _history_with_dates(history: list[tuple[str, float]]) -> list[tuple[datetime.date, float]]:
    rows: list[tuple[datetime.date, float]] = []
    for date_text, value in history:
        date_value = _parse_date_only(date_text)
        if date_value is None:
            continue
        rows.append((date_value, float(value)))
    return rows


def _window_rows(
    history: list[tuple[datetime.date, float]],
    latest_date: datetime.date,
    days: int,
) -> list[tuple[datetime.date, float]]:
    cutoff = latest_date - timedelta(days=days)
    window = [item for item in history if item[0] >= cutoff]
    return window if window else history


def _nearest_value_at_or_before(
    history: list[tuple[datetime.date, float]],
    target: datetime.date,
) -> float | None:
    candidates = [value for date_value, value in history if date_value <= target]
    if not candidates:
        return history[0][1] if history else None
    return candidates[-1]


def _sample_weekly(history: list[tuple[datetime.date, float]]) -> list[tuple[datetime.date, float]]:
    by_week: dict[tuple[int, int], tuple[datetime.date, float]] = {}
    for date_value, value in history:
        key = date_value.isocalendar()[:2]
        by_week[key] = (date_value, value)
    return sorted(by_week.values(), key=lambda item: item[0])


def _compress_values(values: list[float], max_points: int = 60) -> list[float]:
    if len(values) <= max_points:
        return values
    step = len(values) / max_points
    out: list[float] = []
    for idx in range(max_points):
        source_idx = min(len(values) - 1, int(round(idx * step)))
        out.append(values[source_idx])
    return out


def _percentile_rank(values: list[float], latest_value: float) -> float | None:
    if not values:
        return None
    count = sum(1 for value in values if value <= latest_value)
    return (count / len(values)) * 100.0


def _volatility_annualized(values: list[float], periods_per_year: int = 52) -> float | None:
    if len(values) < 3:
        return None
    returns: list[float] = []
    for idx in range(1, len(values)):
        prev = values[idx - 1]
        curr = values[idx]
        if abs(prev) < 1e-12:
            continue
        returns.append((curr - prev) / prev)
    if len(returns) < 2:
        return None
    return statistics.pstdev(returns) * math.sqrt(periods_per_year)


def _regime_classification(percentile: float | None, limited_history: bool) -> str:
    if percentile is None:
        return "limited history"
    if percentile <= 10:
        label = "extreme low percentile"
    elif percentile <= 20:
        label = "lower percentile regime"
    elif percentile < 40:
        label = "below median regime"
    elif percentile < 60:
        label = "middle regime"
    elif percentile < 80:
        label = "upper percentile regime"
    elif percentile < 90:
        label = "high percentile regime"
    else:
        label = "extreme high percentile"
    if limited_history:
        return f"{label} (limited history)"
    return label


def _direction_tag(momentum: float | None, latest_value: float) -> str:
    if momentum is None:
        return "limited history"
    threshold = max(abs(latest_value) * 0.002, 1e-9)
    if momentum > threshold:
        return "upward"
    if momentum < -threshold:
        return "downward"
    return "sideways"


def _zscore_for_20obs(history_values: list[float]) -> float | None:
    if len(history_values) < 80:
        return None
    deltas: list[float] = []
    for idx in range(20, len(history_values)):
        deltas.append(history_values[idx] - history_values[idx - 20])
    if len(deltas) < 10:
        return None
    baseline = deltas[:-1]
    if len(baseline) < 5:
        return None
    sigma = statistics.pstdev(baseline)
    if sigma <= 1e-12:
        return None
    return (deltas[-1] - statistics.mean(baseline)) / sigma


def _build_dual_horizon_graph_rows(
    series: dict[str, dict[str, Any]],
    fred_citations: list[Citation],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    citation_by_code: dict[str, Citation] = {}
    for citation in fred_citations:
        source_id = str(citation.source_id or "")
        if source_id.startswith("fred_"):
            citation_by_code[source_id.replace("fred_", "").upper()] = citation
        elif source_id == "mas_sg10_proxy":
            citation_by_code["IRLTLT01SGM156N"] = citation

    for code, label, _url in FRED_SERIES:
        row = series.get(code)
        if not row:
            continue
        history_raw = row.get("history", [])
        history = _history_with_dates(history_raw)
        if not history:
            continue

        latest_date, latest_value = history[-1]
        earliest_date = history[0][0]
        coverage_years = (latest_date - earliest_date).days / 365.25
        limited_history_5y = coverage_years < 5.0
        limited_history_10y = coverage_years < 10.0

        one_year_ref = _nearest_value_at_or_before(history, latest_date - timedelta(days=365))
        change_1y = (latest_value - one_year_ref) if one_year_ref is not None else None

        three_year_weekly = _sample_weekly(_window_rows(history, latest_date, days=365 * 3))
        three_year_vals = _compress_values([value for _, value in three_year_weekly], max_points=48)
        spark_3y = _sparkline(three_year_vals)

        five_year_window = _window_rows(history, latest_date, days=365 * 5)
        ten_year_window = _window_rows(history, latest_date, days=365 * 10)
        five_year_vals = [value for _, value in five_year_window]
        ten_year_vals = [value for _, value in ten_year_window]

        pct_5y = _percentile_rank(five_year_vals, latest_value)
        pct_10y = _percentile_rank(ten_year_vals, latest_value) if not limited_history_10y else None
        percentile_anchor = pct_10y if pct_10y is not None else pct_5y

        range_base = ten_year_vals if pct_10y is not None else five_year_vals
        range_bar_10y = _range_bar(latest_value, min(range_base), max(range_base)) if range_base else ""

        weekly_5y = _sample_weekly(five_year_window)
        vol_5y = _volatility_annualized([value for _, value in weekly_5y])
        regime = _regime_classification(percentile_anchor, limited_history_5y)

        short_values = [value for _, value in history[-60:]] if len(history) >= 60 else [value for _, value in history]
        short_pct_60 = _percentile_rank(short_values, latest_value)
        momentum_20 = latest_value - history[-21][1] if len(history) >= 21 else None
        change_5 = latest_value - history[-6][1] if len(history) >= 6 else None
        spark_60 = _sparkline(_compress_values(short_values, max_points=60))
        range_bar_60 = _range_bar(latest_value, min(short_values), max(short_values)) if short_values else ""

        citation = citation_by_code.get(code)
        lag_days = citation.lag_days if isinstance(citation, Citation) else None
        lag_class = citation.lag_class if isinstance(citation, Citation) else None
        lag_cause = citation.lag_cause if isinstance(citation, Citation) else None
        observed_at = citation.observed_at if isinstance(citation, Citation) else latest_date.isoformat()
        latest_1d = (latest_value - history[-2][1]) if len(history) >= 2 else None
        if lag_days is not None and lag_days > 0:
            daily_change_cue = f"1d delta: n/a, series not updated since {observed_at}"
        elif latest_1d is None:
            daily_change_cue = "1d delta: n/a, insufficient history"
        else:
            daily_change_cue = f"1d delta: {latest_1d:+.2f}"

        rows.append(
            {
                "series_code": code,
                "metric": f"{label} ({latest_date.isoformat()})",
                "latest_date": latest_date.isoformat(),
                "citation": citation,
                "as_of": observed_at or "observed_at unavailable",
                "lag_days": lag_days,
                "lag_class": lag_class,
                "lag_cause": lag_cause,
                "daily_change_cue": daily_change_cue,
                "long_horizon": {
                    "latest": latest_value,
                    "date": latest_date.isoformat(),
                    "change_1d": latest_1d,
                    "change_1y": change_1y,
                    "sparkline_3y_weekly": spark_3y,
                    "rolling_vol_5y": vol_5y,
                    "percentile_5y": pct_5y,
                    "percentile_10y": pct_10y,
                    "range_bar_10y": range_bar_10y,
                    "regime_classification": regime,
                    "history_note": "limited history" if limited_history_5y else "",
                    "ten_year_note": "limited history" if limited_history_10y else "",
                },
                "short_horizon": {
                    "change_5obs": change_5,
                    "momentum_20obs": momentum_20,
                    "sparkline_60d": spark_60,
                    "range_bar_60d": range_bar_60,
                    "percentile_60d": short_pct_60,
                    "direction_tag": _direction_tag(momentum_20, latest_value),
                    "zscore_20obs": _zscore_for_20obs([value for _, value in history]),
                },
            }
        )
    return rows


def _derive_long_state(graph_rows: list[dict[str, Any]]) -> str:
    percentiles = []
    for row in graph_rows:
        long = row.get("long_horizon", {})
        percentile = long.get("percentile_10y")
        if percentile is None:
            percentile = long.get("percentile_5y")
        if percentile is None:
            continue
        percentiles.append(float(percentile))
    if not percentiles:
        return "Normal"
    extreme = sum(1 for value in percentiles if value >= 90 or value <= 10)
    watch = sum(1 for value in percentiles if value >= 80 or value <= 20)
    if extreme >= 2:
        return "Alert"
    if extreme >= 1 or watch >= 2:
        return "Watch"
    return "Normal"


def _severity_from_components(citations: list[Citation], corroboration_count: int, multi_asset: bool) -> str:
    source_count = len({citation.source_id for citation in citations})
    if corroboration_count >= 2 and source_count >= 2 and multi_asset:
        return "critical"
    if corroboration_count >= 2:
        return "warning"
    return "info"


def _generate_alert_events(
    graph_rows: list[dict[str, Any]],
    mcp_risk_text: str,
    mcp_risk_citations: list[Citation],
    big_players: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []

    for row in graph_rows:
        metric = row.get("metric", "metric")
        code = row.get("series_code", "")
        long = row.get("long_horizon", {})
        short = row.get("short_horizon", {})
        pct_5y = long.get("percentile_5y")
        pct_60d = short.get("percentile_60d")
        momentum_20 = short.get("momentum_20obs")
        direction = short.get("direction_tag")
        zscore = short.get("zscore_20obs")
        citation = row.get("citation")
        if citation is None:
            continue

        # Regime shift watch: short-horizon extreme with elevated long-horizon percentile.
        if pct_60d is not None and pct_5y is not None:
            if float(pct_60d) >= 80 and float(pct_5y) >= 60 and direction in {"upward", "downward"}:
                corroboration = 2 if (momentum_20 is not None and abs(float(momentum_20)) > 0) else 1
                alerts.append(
                    {
                        "category": "Regime shift watch",
                        "severity": _severity_from_components([citation], corroboration, multi_asset=code in {"VIXCLS", "BAMLH0A0HYM2"}),
                        "trigger_reason": f"{metric}: short-horizon percentile {float(pct_60d):.1f} with long-horizon percentile {float(pct_5y):.1f}.",
                        "what_moved": f"20-observation momentum {float(momentum_20):+.2f} and directional tag {direction}.",
                        "why_it_matters": "This configuration may warrant closer monitoring because short-horizon acceleration is occurring in an already elevated long-horizon regime.",
                        "what_would_neutralize": "A sustained drop in short-horizon percentile and momentum reversion toward neutral historically reduces regime-pressure signals.",
                        "citations": [citation],
                    }
                )

        # Dislocation watchlist: large 20-observation z-score relative to recent distribution.
        if zscore is not None and abs(float(zscore)) >= 2.0:
            alerts.append(
                {
                    "category": "Dislocation watchlist",
                    "severity": _severity_from_components([citation], 2, multi_asset=code in {"SP500", "VIXCLS", "BAMLH0A0HYM2"}),
                    "trigger_reason": f"{metric}: 20-observation z-score is {float(zscore):+.2f}.",
                    "what_moved": f"20-observation momentum registered {float(momentum_20 or 0.0):+.2f}.",
                    "why_it_matters": "Large deviations from the recent distribution are historically associated with heightened path-dependency and monitoring value.",
                    "what_would_neutralize": "A decline in absolute z-score below the trigger threshold with steadier percentile behavior would neutralize this watch.",
                    "citations": [citation],
                }
            )

    event_keywords = ["central bank", "regulation", "credit", "systemic", "liquidity", "policy", "issuer"]
    event_citations = list(mcp_risk_citations)
    for item in big_players:
        if any(keyword in str(item.get("text", "")).lower() for keyword in event_keywords):
            event_citations.extend(item.get("citations", []))

    if event_citations and any(keyword in mcp_risk_text.lower() for keyword in event_keywords):
        citations = event_citations[:3]
        alerts.append(
            {
                "category": "Event and policy risk",
                "severity": _severity_from_components(citations, corroboration_count=2, multi_asset=True),
                "trigger_reason": "Newly ingested event text references policy or systemic risk terms across sources.",
                "what_moved": "Event-driven risk language increased in current MCP/web ingestion compared with baseline context.",
                "why_it_matters": "Cross-source policy and systemic language may imply changing risk narratives that merit near-term follow-through monitoring.",
                "what_would_neutralize": "Subsequent runs with fewer policy-risk references and stable cross-asset short-horizon metrics would neutralize this alert.",
                "citations": citations,
            }
        )

    return alerts


def _generate_opportunity_observations(
    graph_rows: list[dict[str, Any]],
    tax_citations: list[Citation],
) -> list[dict[str, Any]]:
    observations: list[dict[str, Any]] = []
    for row in graph_rows:
        metric = row.get("metric", "metric")
        long = row.get("long_horizon", {})
        short = row.get("short_horizon", {})
        citation = row.get("citation")
        if citation is None:
            continue

        pct_5y = long.get("percentile_5y")
        pct_60d = short.get("percentile_60d")
        zscore = short.get("zscore_20obs")
        direction = short.get("direction_tag")

        if pct_5y is not None and pct_60d is not None and (float(pct_5y) >= 80 or float(pct_5y) <= 20):
            observations.append(
                {
                    "condition_observed": f"{metric} sits in a long-horizon extreme percentile zone ({float(pct_5y):.1f} 5y pct).",
                    "confirmation_data": "Confirm with persistence in 60d percentile and stabilization in 20-observation momentum direction.",
                    "time_horizon": "medium",
                    "confidence": 0.68,
                    "citations": [citation],
                }
            )

        if zscore is not None and abs(float(zscore)) >= 1.75:
            observations.append(
                {
                    "condition_observed": f"{metric} shows a near-term distribution dislocation (20-observation z-score {float(zscore):+.2f}).",
                    "confirmation_data": "Confirm with repeated z-score breaches and corroboration in related credit/volatility metrics.",
                    "time_horizon": "short",
                    "confidence": 0.64,
                    "citations": [citation],
                }
            )

        if direction == "upward" and pct_60d is not None and float(pct_60d) > 70 and "Yield" in metric:
            observations.append(
                {
                    "condition_observed": f"{metric} is in an upper short-horizon percentile with upward direction.",
                    "confirmation_data": "Confirm with continued momentum and unchanged long-horizon percentile regime classification.",
                    "time_horizon": "short",
                    "confidence": 0.61,
                    "citations": [citation],
                }
            )

    if tax_citations:
        observations.append(
            {
                "condition_observed": "Singapore tax implementation spread remains material in modeled withholding and estate-risk assumptions.",
                "confirmation_data": "Confirm with updated treaty and withholding references plus vehicle-level cost/liquidity refresh.",
                "time_horizon": "medium",
                "confidence": 0.66,
                "citations": tax_citations[:2],
            }
        )

    # Rank by confidence and deduplicate by condition text.
    dedup: dict[str, dict[str, Any]] = {}
    for item in observations:
        key = str(item["condition_observed"])
        prev = dedup.get(key)
        if prev is None or float(item["confidence"]) > float(prev["confidence"]):
            dedup[key] = item
    ranked = sorted(dedup.values(), key=lambda item: float(item["confidence"]), reverse=True)
    return ranked[:6]


def _validate_cited_items(section_name: str, items: list[dict[str, Any]]) -> None:
    for item in items:
        citations = item.get("citations", [])
        if not citations:
            raise OmniBriefError(f"{section_name} item missing citations")


def _validate_implementation_mapping(mapping: dict[str, Any]) -> None:
    sleeves = mapping.get("sleeves", {})
    for sleeve_key, payload in sleeves.items():
        candidates = payload.get("candidates", [])
        for candidate in candidates:
            if not candidate.citations:
                raise OmniBriefError(f"Implementation candidate missing citations in sleeve {sleeve_key}")
            if sleeve_key == "convex":
                if candidate.margin_required is True:
                    raise OmniBriefError(f"Convex candidate {candidate.symbol} violates no-margin constraint")
                if candidate.max_loss_known is False:
                    raise OmniBriefError(
                        f"Convex candidate {candidate.symbol} violates defined-max-loss constraint"
                    )
                if candidate.option_position is not None and candidate.option_position != "long_put":
                    raise OmniBriefError(f"Convex option candidate {candidate.symbol} must be long_put only")

        for item in payload.get("sg_tax_observations", []):
            citations = item.get("citations", [])
            if not citations:
                raise OmniBriefError(f"SG tax observation missing citations in sleeve {sleeve_key}")

    for item in mapping.get("watchlist_candidates", []):
        if not item.get("citations", []):
            raise OmniBriefError("Implementation watchlist candidate missing citations")


def _short_state_from_alerts(alerts: list[dict[str, Any]]) -> str:
    severities = {item.get("severity", "info").lower() for item in alerts}
    if "critical" in severities:
        return "Alert"
    if "warning" in severities or "info" in severities:
        return "Watch"
    return "Normal"


def _extract_quote(text: str, max_words: int = 25) -> str | None:
    plain = re.sub(r"<[^>]+>", " ", text)
    plain = re.sub(r"\s+", " ", plain).strip()
    if not plain:
        return None
    words = plain.split(" ")
    if len(words) > max_words:
        words = words[:max_words]
    quote = " ".join(words).strip()
    if not quote:
        return None
    return quote


def _extract_publication_date(text: str) -> str | None:
    patterns = [
        r'property="article:published_time" content="([^"]+)"',
        r'name="article:published_time" content="([^"]+)"',
        r'"datePublished"\s*:\s*"([^"]+)"',
        r'property="og:updated_time" content="([^"]+)"',
    ]
    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).strip()
    return None


def _ensure_brief_metadata_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS series_observations (
          series_id TEXT NOT NULL,
          source_id TEXT NOT NULL,
          metric_key TEXT NOT NULL,
          observation_date TEXT NOT NULL,
          observation_value REAL,
          retrieved_at TEXT NOT NULL,
          lag_days INTEGER,
          lag_class TEXT,
          lag_cause TEXT,
          retrieval_succeeded INTEGER,
          raw_hash TEXT NOT NULL,
          PRIMARY KEY (series_id, retrieved_at)
        )
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_series_observations_metric
        ON series_observations (metric_key, retrieved_at DESC)
        """
    )
    series_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(series_observations)").fetchall()}
    if "lag_days" not in series_columns:
        conn.execute("ALTER TABLE series_observations ADD COLUMN lag_days INTEGER")
    if "lag_class" not in series_columns:
        conn.execute("ALTER TABLE series_observations ADD COLUMN lag_class TEXT")
    if "lag_cause" not in series_columns:
        conn.execute("ALTER TABLE series_observations ADD COLUMN lag_cause TEXT")
    if "retrieval_succeeded" not in series_columns:
        conn.execute("ALTER TABLE series_observations ADD COLUMN retrieval_succeeded INTEGER")

    metric_columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(metric_snapshots)").fetchall()}
    if "run_id" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN run_id TEXT")
    if "metric_key" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN metric_key TEXT")
    if "observed_at" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN observed_at TEXT")
    if "retrieved_at" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN retrieved_at TEXT")
    if "lag_days" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN lag_days INTEGER")
    if "lag_class" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN lag_class TEXT")
    if "lag_cause" not in metric_columns:
        conn.execute("ALTER TABLE metric_snapshots ADD COLUMN lag_cause TEXT")
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_metric_snapshots_run_metric
        ON metric_snapshots (run_id, metric_key)
        """
    )
    conn.commit()


def _persist_series_observations(
    conn: sqlite3.Connection,
    series: dict[str, dict[str, Any]],
    fred_citations: list[Citation],
) -> None:
    citation_by_source = {citation.source_id: citation for citation in fred_citations}
    for code, _label, _url in FRED_SERIES:
        source_id = FRED_SOURCE_BY_CODE.get(code, "")
        citation = citation_by_source.get(source_id)
        row = series.get(code, {})
        observation_date = str(row.get("latest_date") or "")
        observation_value = row.get("latest_value")
        if not observation_date or citation is None:
            continue
        raw_hash = hashlib.sha256(
            json.dumps(
                {
                    "series_id": code,
                    "source_id": source_id,
                    "observation_date": observation_date,
                    "observation_value": observation_value,
                    "retrieved_at": citation.retrieved_at.isoformat(),
                },
                sort_keys=True,
            ).encode("utf-8")
        ).hexdigest()
        conn.execute(
            """
            INSERT OR REPLACE INTO series_observations (
              series_id, source_id, metric_key, observation_date, observation_value, retrieved_at,
              lag_days, lag_class, lag_cause, retrieval_succeeded, raw_hash
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                code,
                source_id,
                code,
                observation_date,
                float(observation_value) if observation_value is not None else None,
                citation.retrieved_at.isoformat(),
                citation.lag_days,
                citation.lag_class,
                citation.lag_cause,
                0 if str(citation.importance).lower().find("retrieval=cached") >= 0 else 1,
                raw_hash,
            ),
        )
    conn.commit()


def _metric_name_from_row(row: dict[str, Any]) -> str:
    raw = str(row.get("metric", "")).strip()
    if not raw:
        return str(row.get("series_code", "metric"))
    return raw.split("(", 1)[0].strip()


def _metric_anchor_percentile(row: dict[str, Any]) -> float | None:
    long = dict(row.get("long_horizon") or {})
    for key in ("percentile_10y", "percentile_5y", "percentile_60"):
        value = long.get(key)
        if value is None:
            continue
        try:
            return float(value)
        except (TypeError, ValueError):
            continue
    return None


def _persist_metric_snapshots(
    conn: sqlite3.Connection,
    run_id: str,
    graph_rows: list[dict[str, Any]],
) -> None:
    asof_ts = datetime.now(UTC).isoformat()
    for row in graph_rows:
        metric_key = str(row.get("series_code") or "").strip()
        if not metric_key:
            continue
        long_horizon = dict(row.get("long_horizon") or {})
        short_horizon = dict(row.get("short_horizon") or {})
        value = long_horizon.get("latest")
        if value is None:
            continue
        citation = row.get("citation")
        observed_at = str(row.get("latest_date") or "")
        retrieved_at = citation.retrieved_at.isoformat() if isinstance(citation, Citation) else asof_ts
        lag_days = citation.lag_days if isinstance(citation, Citation) else row.get("lag_days")
        lag_class = citation.lag_class if isinstance(citation, Citation) else row.get("lag_class")
        lag_cause = citation.lag_cause if isinstance(citation, Citation) else row.get("lag_cause")
        snapshot_id = f"ms_{metric_key}_{uuid.uuid4().hex[:10]}"
        citations_json = []
        if isinstance(citation, Citation):
            citations_json.append(citation.model_dump(mode="json"))
        conn.execute(
            """
            INSERT INTO metric_snapshots (
              snapshot_id, asof_ts, run_id, metric_key, metric_id, metric_name, value, observed_at, retrieved_at,
              lag_days, lag_class, lag_cause,
              delta_1d, window_5_change, window_20_change, window_60_range_low, window_60_range_high,
              percentile_60, prev_percentile_60, percentile_shift, stddev_60, state_short, days_in_state_short, citations_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                snapshot_id,
                asof_ts,
                run_id,
                metric_key,
                metric_key,
                _metric_name_from_row(row),
                float(value),
                observed_at or None,
                retrieved_at,
                lag_days,
                lag_class,
                lag_cause,
                long_horizon.get("change_1d"),
                short_horizon.get("change_5obs"),
                short_horizon.get("momentum_20obs"),
                None,
                None,
                short_horizon.get("percentile_60d"),
                None,
                None,
                None,
                None,
                None,
                json.dumps(citations_json),
            ),
        )
    conn.commit()


def _load_previous_metric_snapshot_map(
    conn: sqlite3.Connection,
    run_id: str,
) -> tuple[str | None, dict[str, dict[str, Any]]]:
    prior_run = conn.execute(
        """
        SELECT run_id, MAX(COALESCE(retrieved_at, asof_ts)) AS ts
        FROM metric_snapshots
        WHERE run_id IS NOT NULL AND run_id != ?
        GROUP BY run_id
        ORDER BY ts DESC
        LIMIT 1
        """,
        (run_id,),
    ).fetchone()
    if not prior_run:
        return None, {}

    prior_run_id = str(prior_run["run_id"])
    rows = conn.execute(
        """
        SELECT metric_key, metric_name, value, observed_at, retrieved_at, percentile_60
        FROM metric_snapshots
        WHERE run_id = ?
        """,
        (prior_run_id,),
    ).fetchall()
    payload: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = str(row["metric_key"] or "")
        if not key:
            continue
        payload[key] = {
            "metric_name": str(row["metric_name"] or key),
            "value": float(row["value"]),
            "observed_at": str(row["observed_at"] or ""),
            "retrieved_at": str(row["retrieved_at"] or ""),
            "percentile_60": float(row["percentile_60"]) if row["percentile_60"] is not None else None,
        }
    return prior_run_id, payload


def _threshold_crossing_label(previous_pct: float | None, current_pct: float | None) -> str:
    if previous_pct is None or current_pct is None:
        return "threshold unchanged"
    bands = [10.0, 20.0, 80.0, 90.0]
    for threshold in bands:
        crossed_up = previous_pct < threshold <= current_pct
        crossed_down = previous_pct > threshold >= current_pct
        if crossed_up:
            return f"crossed above {threshold:.0f}th percentile"
        if crossed_down:
            return f"crossed below {threshold:.0f}th percentile"
    return "threshold unchanged"


def _build_quantified_top_movers(
    conn: sqlite3.Connection,
    run_id: str,
    graph_rows: list[dict[str, Any]],
    default_citations: list[Citation],
) -> list[dict[str, Any]]:
    prior_run_id, previous_map = _load_previous_metric_snapshot_map(conn, run_id=run_id)
    if not prior_run_id:
        return [
            {
                "text": "Top quantified changes since last report are unavailable because no prior successful metric snapshot exists.",
                "citations": default_citations[:1],
            }
        ]

    movers: list[dict[str, Any]] = []
    for row in graph_rows:
        metric_key = str(row.get("series_code") or "")
        if not metric_key or metric_key not in previous_map:
            continue
        previous = previous_map[metric_key]
        long_horizon = dict(row.get("long_horizon") or {})
        current_value = long_horizon.get("latest")
        if current_value is None:
            continue
        previous_value = float(previous["value"])
        delta_value = float(current_value) - previous_value
        current_pct = _metric_anchor_percentile(row)
        prev_pct = previous.get("percentile_60")
        threshold_label = _threshold_crossing_label(prev_pct, current_pct)
        observed_at = str(row.get("latest_date") or "n/a")
        citation = row.get("citation")
        lag_text = "observed_at unavailable"
        if isinstance(citation, Citation) and citation.lag_days is not None:
            lag_text = f"lag {citation.lag_days} day(s)"
        delta_days = "n/a"
        prev_observed = str(previous.get("observed_at") or "")
        if observed_at and prev_observed:
            try:
                delta_days = str(
                    (datetime.strptime(observed_at, "%Y-%m-%d").date() - datetime.strptime(prev_observed, "%Y-%m-%d").date()).days
                )
            except ValueError:
                delta_days = "n/a"
        metric_name = _metric_name_from_row(row)
        movers.append(
            {
                "score": abs(delta_value),
                "text": (
                    f"{metric_name} moved {delta_value:+.2f} since prior report "
                    f"(prior {previous_value:.2f} -> current {float(current_value):.2f}); "
                    f"{threshold_label}. Window: since last report ({delta_days} day(s) in observed data). "
                    f"As of {observed_at}, {lag_text}."
                ),
                "citations": [citation] if isinstance(citation, Citation) else default_citations[:1],
            }
        )
    if not movers:
        return [
            {
                "text": "Top quantified changes since last report are unavailable because comparable prior metric keys were not found.",
                "citations": default_citations[:1],
            }
        ]
    movers.sort(key=lambda item: item["score"], reverse=True)
    return [{"text": item["text"], "citations": item["citations"]} for item in movers[:5]]


def _build_data_recency_summary(graph_rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = {"fresh": 0, "lagged": 0, "stale": 0}
    lagged_metrics: list[dict[str, Any]] = []
    non_compliant_metrics: list[str] = []
    for row in graph_rows:
        citation = row.get("citation")
        metric = _metric_name_from_row(row)
        source_id = citation.source_id if isinstance(citation, Citation) else "unknown_source"
        observed_at = citation.observed_at if isinstance(citation, Citation) else None
        lag_days = citation.lag_days if isinstance(citation, Citation) else None
        lag_class = citation.lag_class if isinstance(citation, Citation) else None
        if not observed_at:
            non_compliant_metrics.append(metric)
        if lag_class in counts:
            counts[lag_class] += 1
        if lag_days is not None:
            lagged_metrics.append(
                {
                    "metric": metric,
                    "source_id": source_id,
                    "observed_at": observed_at or "observed_at unavailable",
                    "lag_days": int(lag_days),
                    "lag_class": lag_class or "unknown",
                    "lag_cause": citation.lag_cause if isinstance(citation, Citation) else None,
                }
            )
    lagged_metrics.sort(key=lambda item: item["lag_days"], reverse=True)
    return {
        "counts": counts,
        "top_lagged_metrics": lagged_metrics[:5],
        "stale_present": counts.get("stale", 0) > 0,
        "non_compliant_metrics": non_compliant_metrics[:5],
    }


_DOMINANT_SIGNAL_CODES = {
    "DGS10",
    "T10YIE",
    "SP500",
    "VIXCLS",
    "BAMLH0A0HYM2",
    "T10Y2Y",
    "DTWEXBGS",
    "DEXSIUS",
    "IRLTLT01SGM156N",
}

_CONTEXTUAL_SIGNAL_CODES = {
    "VXEEMCLS",
    "DCOILWTICO",
    "STI_PROXY",
    "VEA_PROXY",
    "IRLTLT01EZM156N",
}


def _signal_role_for_metric(metric_code: str) -> str:
    normalized = str(metric_code or "").strip().upper()
    if normalized in _DOMINANT_SIGNAL_CODES:
        return "dominant_signal"
    if normalized in _CONTEXTUAL_SIGNAL_CODES:
        return "contextual_signal"
    if normalized:
        return "supporting_signal"
    return "background_signal"


def _series_freshness_reason_code(
    row: dict[str, Any],
    *,
    refresh_attempted: bool,
    refresh_ok: bool,
    role: str,
) -> str:
    citation = row.get("citation")
    lag_class = str((citation.lag_class if isinstance(citation, Citation) else row.get("lag_class")) or "").strip().lower()
    lag_cause = str((citation.lag_cause if isinstance(citation, Citation) else row.get("lag_cause")) or "").strip().lower()
    lag_days = citation.lag_days if isinstance(citation, Citation) else row.get("lag_days")
    if not refresh_attempted:
        return "refresh_skipped_policy"
    if lag_cause == "expected_publication_lag":
        return "latest_available_source_lag"
    if lag_cause in {"unexpected_ingestion_lag", "cache_fallback"} or (refresh_attempted and not refresh_ok and lag_class in {"lagged", "stale"}):
        return "refresh_failed_used_cache"
    if role == "background_signal" and lag_class in {"stale", "lagged", "slightly_lagged"}:
        return "stale_excluded" if bool(settings.daily_brief_exclude_stale_background) else "stale_demoted"
    if role == "supporting_signal" and lag_class in {"stale", "lagged"}:
        return "stale_demoted"
    if role == "dominant_signal":
        max_lag = int(settings.daily_brief_dominant_max_lag_days or 2)
    elif role == "supporting_signal":
        max_lag = int(settings.daily_brief_support_max_lag_days or 4)
    else:
        max_lag = int(settings.daily_brief_background_max_lag_days or 14)
    try:
        lag_days_value = int(lag_days) if lag_days is not None else None
    except Exception:
        lag_days_value = None
    if lag_days_value is not None and lag_days_value > max_lag:
        if role == "background_signal" and bool(settings.daily_brief_exclude_stale_background):
            return "stale_excluded"
        if role in {"supporting_signal", "background_signal"} and bool(settings.daily_brief_demote_stale_support):
            return "stale_demoted"
    if lag_class in {"current", "fresh", "recent"}:
        return "refreshed_current"
    if lag_class in {"slightly_lagged", "lagged"}:
        return "latest_available_source_lag"
    return "refreshed_current"


_REFRESH_GROUP_FILES = {
    "fred": [f"{code}.csv" for code, _label, _url in FRED_SERIES],
    "mas": ["IRLTLT01SGM156N.csv"],
    "stooq": ["stooq_spy_volume.csv", "stooq_qqq_volume.csv", "stooq_sti_proxy.csv"],
    "yahoo": ["yahoo_spx_chart.json", "yahoo_spy_chart.json", "yahoo_sti_chart.json", "yahoo_vea_chart.json"],
    "web": [
        "oaktree_sea_change.html",
        "taleb_fat_tails.html",
        "iras_overseas_income.html",
        "irs_withholding_nra.html",
        "mcp_servers.json",
        "mcp_registry_snapshot.json",
    ],
}


def _series_group_from_source_id(source_id: str) -> str:
    normalized = str(source_id or "").lower()
    if normalized.startswith("fred_"):
        return "fred"
    if normalized.startswith("mas_"):
        return "mas"
    if normalized.startswith("stooq_"):
        return "stooq"
    if normalized.startswith("yahoo_"):
        return "yahoo"
    return "web"


def _citation_field(citation: Any, field: str, default: Any = None) -> Any:
    if isinstance(citation, Citation):
        return getattr(citation, field, default)
    if isinstance(citation, dict):
        return citation.get(field, default)
    return default


def _build_refresh_report(
    *,
    brief_run_id: str,
    run_started_at: datetime,
    run_finished_at: datetime,
    refresh_attempted: bool,
    refresh_ok: bool,
    refresh_msg: str,
    graph_rows: list[dict[str, Any]],
    volume_rows: list[dict[str, Any]],
    sti_proxy_row: dict[str, Any] | None,
    vea_proxy_row: dict[str, Any] | None,
    data_recency_summary: dict[str, Any],
    provider_refresh: dict[str, Any] | None,
) -> dict[str, Any]:
    group_rows: dict[str, list[dict[str, Any]]] = {}
    all_rows = [*list(graph_rows or []), *list(volume_rows or [])]
    if isinstance(sti_proxy_row, dict) and sti_proxy_row:
        all_rows.append(sti_proxy_row)
    if isinstance(vea_proxy_row, dict) and vea_proxy_row:
        all_rows.append(vea_proxy_row)
    for row in all_rows:
        citation = row.get("citation")
        source_id = str(_citation_field(citation, "source_id", row.get("source_id")) or "")
        group = _series_group_from_source_id(source_id)
        group_rows.setdefault(group, []).append(dict(row))

    series_reports: list[dict[str, Any]] = []
    for row in all_rows:
        metric = _metric_name_from_row(row)
        role = _signal_role_for_metric(str(row.get("metric") or row.get("metric_code") or metric))
        reason_code = _series_freshness_reason_code(
            row,
            refresh_attempted=refresh_attempted,
            refresh_ok=refresh_ok,
            role=role,
        )
        citation = row.get("citation")
        series_reports.append(
            {
                "metric": metric,
                "role": role,
                "reason_code": reason_code,
                "observed_at": (
                    citation.observed_at.isoformat()
                    if isinstance(citation, Citation) and isinstance(citation.observed_at, datetime)
                    else str(_citation_field(citation, "observed_at", row.get("latest_date")) or "")
                ),
                "lag_days": _citation_field(citation, "lag_days", row.get("lag_days")),
                "lag_class": (
                    _citation_field(citation, "lag_class", row.get("lag_class"))
                ),
                "lag_cause": (
                    _citation_field(citation, "lag_cause", row.get("lag_cause"))
                ),
                "source_id": (
                    str(_citation_field(citation, "source_id", row.get("source_id")) or "")
                ),
            }
        )

    source_group_reports: list[dict[str, Any]] = []
    for group_name, file_names in _REFRESH_GROUP_FILES.items():
        touched = 0
        missing = 0
        for name in file_names:
            path = CACHE_DIR / name
            if not path.exists():
                missing += 1
                continue
            try:
                if datetime.fromtimestamp(path.stat().st_mtime, UTC) >= run_started_at - timedelta(seconds=2):
                    touched += 1
            except Exception:
                continue
        rows = group_rows.get(group_name, [])
        latest_dates = sorted(
            {
                str(_citation_field(row.get("citation"), "observed_at", row.get("latest_date") or row.get("as_of")) or "")
                for row in rows
                if str(_citation_field(row.get("citation"), "observed_at", row.get("latest_date") or row.get("as_of")) or "").strip()
            },
            reverse=True,
        )[:5]
        lag_causes = {
            str(_citation_field(row.get("citation"), "lag_cause", row.get("lag_cause")) or "").strip()
            for row in rows
            if str(_citation_field(row.get("citation"), "lag_cause", row.get("lag_cause")) or "").strip()
        }
        latest_available_reason = (
            "latest data is delayed by source publication cadence"
            if "expected_publication_lag" in lag_causes
            else None
        )
        failure_reason = None
        if refresh_attempted and not refresh_ok and (touched == 0 or group_name in {"fred", "web"}):
            failure_reason = refresh_msg or "refresh failed"
        elif refresh_attempted and missing == len(file_names) and rows:
            failure_reason = "cache files were not refreshed for this group"
        fresh_count = sum(
            1
            for row in rows
            if _series_freshness_reason_code(
                row,
                refresh_attempted=refresh_attempted,
                refresh_ok=refresh_ok,
                role=_signal_role_for_metric(str(row.get("metric") or row.get("metric_code") or _metric_name_from_row(row))),
            )
            == "refreshed_current"
        )
        lagged_count = sum(
            1
            for row in rows
            if _series_freshness_reason_code(
                row,
                refresh_attempted=refresh_attempted,
                refresh_ok=refresh_ok,
                role=_signal_role_for_metric(str(row.get("metric") or row.get("metric_code") or _metric_name_from_row(row))),
            )
            == "latest_available_source_lag"
        )
        failed_count = sum(
            1
            for row in rows
            if _series_freshness_reason_code(
                row,
                refresh_attempted=refresh_attempted,
                refresh_ok=refresh_ok,
                role=_signal_role_for_metric(str(row.get("metric") or row.get("metric_code") or _metric_name_from_row(row))),
            )
            == "refresh_failed_used_cache"
        )
        status = "ok"
        if failure_reason and not rows:
            status = "failed"
        elif failed_count:
            status = "partial_failed"
        elif lagged_count and not fresh_count:
            status = "latest_available_lagged"
        source_group_reports.append(
            {
                "source_group_name": group_name,
                "refresh_attempted": refresh_attempted,
                "refresh_succeeded": bool(refresh_ok and (touched > 0 or rows)),
                "refresh_status": status,
                "series_updated_count": touched,
                "series_failed_count": missing if refresh_attempted and missing else 0,
                "series_current_count": fresh_count,
                "series_lagged_count": lagged_count,
                "series_refresh_failed_count": failed_count,
                "latest_observation_dates": latest_dates,
                "failure_reason": failure_reason,
                "latest_available_reason": latest_available_reason,
            }
        )

    top_lagged = list(data_recency_summary.get("top_lagged_metrics") or [])
    stale_series = [item for item in series_reports if str(item.get("reason_code") or "") in {"refresh_failed_used_cache"}]
    lagged_series = [item for item in series_reports if str(item.get("reason_code") or "") == "latest_available_source_lag"]
    excluded_series = [item for item in series_reports if str(item.get("reason_code") or "") == "stale_excluded"]
    demoted_series = [item for item in series_reports if str(item.get("reason_code") or "") == "stale_demoted"]
    dominant_series = [item for item in series_reports if str(item.get("role") or "") == "dominant_signal"]
    return {
        "brief_run_id": brief_run_id,
        "run_started_at": run_started_at.isoformat(),
        "run_finished_at": run_finished_at.isoformat(),
        "refresh_attempted": refresh_attempted,
        "refresh_status": "ok" if refresh_ok else ("skipped" if not refresh_attempted else "failed"),
        "source_group_reports": source_group_reports,
        "series_reports": series_reports,
        "stale_series_after_refresh": stale_series or [item for item in top_lagged if str(item.get("lag_class") or "") == "stale"],
        "lagged_but_latest_available_series": lagged_series or [item for item in top_lagged if str(item.get("lag_class") or "") == "lagged"],
        "failed_refresh_series": stale_series or [item for item in top_lagged if str(item.get("lag_cause") or "") == "unexpected_ingestion_lag"],
        "excluded_from_brief_due_to_staleness": excluded_series,
        "demoted_from_main_path_due_to_staleness": demoted_series,
        "dominant_signal_freshness_summary": {
            "counts": dict(data_recency_summary.get("counts") or {}),
            "stale_present": bool(data_recency_summary.get("stale_present")),
            "provider_refresh_sufficiency": dict((provider_refresh or {}).get("sufficiency") or {}),
            "dominant_signal_counts": {
                "current": sum(1 for item in dominant_series if str(item.get("reason_code") or "") == "refreshed_current"),
                "source_lagged": sum(1 for item in dominant_series if str(item.get("reason_code") or "") == "latest_available_source_lag"),
                "refresh_failed_used_cache": sum(1 for item in dominant_series if str(item.get("reason_code") or "") == "refresh_failed_used_cache"),
                "demoted_or_excluded": sum(1 for item in dominant_series if str(item.get("reason_code") or "") in {"stale_demoted", "stale_excluded"}),
            },
        },
    }


def _build_mcp_updates(
    conn: sqlite3.Connection,
    mcp_result: MCPIngestionResult,
) -> dict[str, Any]:
    current_run_id = str(mcp_result.run_id or "")
    current_rows = conn.execute(
        """
        SELECT item_id, mcp_server_id, title, url, published_at, retrieved_at, content_hash, snippet
        FROM mcp_items
        WHERE run_id = ?
        """,
        (current_run_id,),
    ).fetchall()

    prior_run = conn.execute(
        """
        SELECT run_id, MAX(retrieved_at) AS ts
        FROM mcp_items
        WHERE run_id IS NOT NULL AND run_id != ?
        GROUP BY run_id
        ORDER BY ts DESC
        LIMIT 1
        """,
        (current_run_id,),
    ).fetchone()
    prior_run_id = str(prior_run["run_id"]) if prior_run else None

    previous_map: dict[str, dict[str, Any]] = {}
    if prior_run_id:
        rows = conn.execute(
            """
            SELECT item_id, content_hash
            FROM mcp_items
            WHERE run_id = ?
            """,
            (prior_run_id,),
        ).fetchall()
        for row in rows:
            item_id = str(row["item_id"] or "")
            if item_id:
                previous_map[item_id] = {"content_hash": str(row["content_hash"] or "")}

    snapshot_by_server = {snapshot.server_id: snapshot for snapshot in mcp_result.snapshots}
    new_items: list[dict[str, Any]] = []
    changed_items: list[dict[str, Any]] = []

    for row in current_rows:
        item_id = str(row["item_id"] or "")
        server_id = str(row["mcp_server_id"] or "")
        content_hash = str(row["content_hash"] or "")
        snapshot = snapshot_by_server.get(server_id)
        if snapshot is None:
            continue
        citation = _citation(
            url=snapshot.endpoint_url,
            source_id=f"mcp_{server_id}",
            retrieved_at=snapshot.retrieved_at,
            importance="MCP server source record for content delta",
            cached=snapshot.cached,
        )
        payload = {
            "server_id": server_id,
            "title": str(row["title"] or "untitled MCP item"),
            "url": str(row["url"] or ""),
            "published_at": str(row["published_at"] or ""),
            "retrieved_at": str(row["retrieved_at"] or ""),
            "snippet": str(row["snippet"] or ""),
            "citations": [citation],
        }
        prev = previous_map.get(item_id)
        if prev is None:
            new_items.append(payload)
        elif str(prev.get("content_hash", "")) != content_hash:
            changed_items.append(payload)

    return {
        "coverage": {
            "connectable": mcp_result.connectable_servers,
            "live_successes": mcp_result.live_success_count,
            "success_ratio": mcp_result.live_success_ratio,
        },
        "new_items": new_items[:10],
        "changed_items": changed_items[:5],
        "no_new_items": len(new_items) == 0 and len(changed_items) == 0,
    }


def _ensure_dual_horizon_tables(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS report_runs (
            run_id TEXT PRIMARY KEY,
            created_at TEXT NOT NULL,
            long_state TEXT NOT NULL,
            short_state TEXT NOT NULL,
            summary_json TEXT NOT NULL
        )
        """
    )

    columns = [row[1] for row in conn.execute("PRAGMA table_info(alert_events)").fetchall()]
    if "run_id" not in columns:
        conn.execute("ALTER TABLE alert_events ADD COLUMN run_id TEXT")
    conn.commit()


def _load_previous_run_summary(conn: sqlite3.Connection) -> dict[str, Any] | None:
    row = conn.execute(
        "SELECT summary_json FROM report_runs ORDER BY created_at DESC LIMIT 1"
    ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row[0])
    except Exception:
        return None


def _persist_run_and_alerts(
    conn: sqlite3.Connection,
    run_id: str,
    created_at: datetime,
    long_state: str,
    short_state: str,
    summary: dict[str, Any],
    alerts: list[dict[str, Any]],
) -> None:
    conn.execute(
        "INSERT INTO report_runs (run_id, created_at, long_state, short_state, summary_json) VALUES (?, ?, ?, ?, ?)",
        (run_id, created_at.isoformat(), long_state, short_state, json.dumps(summary)),
    )

    for item in alerts:
        alert_id = f"alert_{uuid.uuid4().hex[:12]}"
        conn.execute(
            """
            INSERT INTO alert_events (
                alert_id, severity, trigger_reason, citations_json, sent_channel, ack_state, created_at, run_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                alert_id,
                str(item.get("severity", "info")),
                str(item.get("trigger_reason", "")),
                json.dumps([citation.model_dump(mode="json") for citation in item.get("citations", [])]),
                "email",
                "pending",
                created_at.isoformat(),
                run_id,
            ),
        )
    conn.commit()


def _run_summary_payload(
    long_state: str,
    short_state: str,
    graph_rows: list[dict[str, Any]],
    alerts: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for row in graph_rows:
        code = str(row.get("series_code", ""))
        if not code:
            continue
        metrics[code] = {
            "pct5y": row.get("long_horizon", {}).get("percentile_5y"),
            "pct10y": row.get("long_horizon", {}).get("percentile_10y"),
            "pct60d": row.get("short_horizon", {}).get("percentile_60d"),
            "direction": row.get("short_horizon", {}).get("direction_tag"),
        }
    return {
        "long_state": long_state,
        "short_state": short_state,
        "metrics": metrics,
        "alert_triggers": [str(item.get("trigger_reason", "")) for item in alerts],
        "event_texts": [str(item.get("text", "")) for item in events],
    }


def _build_what_changed(
    previous_summary: dict[str, Any] | None,
    current_summary: dict[str, Any],
    default_citations: list[Citation],
) -> list[dict[str, Any]]:
    if not previous_summary:
        return [
            {
                "text": "No previous run available; current report establishes baseline for long-horizon percentile and short-horizon alert deltas.",
                "citations": default_citations[:2],
            }
        ]

    changes: list[dict[str, Any]] = []
    prev_metrics = previous_summary.get("metrics", {})
    curr_metrics = current_summary.get("metrics", {})
    for code, curr in curr_metrics.items():
        prev = prev_metrics.get(code)
        if not prev:
            continue
        prev_pct = prev.get("pct5y")
        curr_pct = curr.get("pct5y")
        if prev_pct is not None and curr_pct is not None and abs(float(curr_pct) - float(prev_pct)) >= 3.0:
            changes.append(
                {
                    "text": f"{code} long-horizon percentile shifted from {float(prev_pct):.1f} to {float(curr_pct):.1f} (5y window).",
                    "citations": default_citations[:1],
                }
            )

    if previous_summary.get("short_state") != current_summary.get("short_state"):
        changes.append(
            {
                "text": f"Short-horizon state changed from {previous_summary.get('short_state')} to {current_summary.get('short_state')}.",
                "citations": default_citations[:2],
            }
        )

    prev_events = set(previous_summary.get("event_texts", []))
    curr_events = set(current_summary.get("event_texts", []))
    new_events = [event for event in curr_events if event not in prev_events]
    if new_events:
        changes.append(
            {
                "text": f"New event text detected in current ingestion: {new_events[0][:170]}",
                "citations": default_citations[:2],
            }
        )

    if not changes:
        changes.append(
            {
                "text": "No material long-horizon percentile shift or short-horizon state transition versus the previous report.",
                "citations": default_citations[:1],
            }
        )
    return changes[:6]


def _graph_quality_audit(graph_rows: list[dict[str, Any]]) -> dict[str, Any]:
    now = datetime.now(UTC)
    citation_list = [row.get("citation") for row in graph_rows if row.get("citation") is not None]
    citations = [citation for citation in citation_list if isinstance(citation, Citation)]
    missing_citation_metrics = [str(row.get("metric", "unknown")) for row in graph_rows if row.get("citation") is None]

    data_ages: list[int] = []
    stale_metrics: list[str] = []
    for row in graph_rows:
        parsed = _parse_iso_date(row.get("latest_date"))
        if parsed is None:
            continue
        age_days = (now.date() - parsed.date()).days
        data_ages.append(age_days)
        if age_days > 7:
            stale_metrics.append(str(row.get("metric", "unknown")))

    if citations:
        age_hours = max((now - citation.retrieved_at).total_seconds() / 3600 for citation in citations)
        retrieval_fresh = age_hours <= 48
    else:
        retrieval_fresh = False

    max_data_age_days = max(data_ages) if data_ages else 999
    data_fresh = max_data_age_days <= 7
    up_to_date = retrieval_fresh and data_fresh

    cited = len(citations) == len(graph_rows) and len(graph_rows) > 0 and not missing_citation_metrics
    def _row_readable(row: dict[str, Any]) -> bool:
        if "long_horizon" in row and "short_horizon" in row:
            long = row.get("long_horizon", {})
            short = row.get("short_horizon", {})
            return bool(
                long.get("sparkline_3y_weekly")
                and long.get("range_bar_10y")
                and long.get("regime_classification")
                and short.get("sparkline_60d")
                and short.get("range_bar_60d")
                and short.get("direction_tag")
            )
        return bool(
            row.get("trajectory")
            and row.get("pattern")
            and row.get("sparkline")
            and row.get("range_bar")
        )

    easy = len(graph_rows) >= 5 and all(_row_readable(row) for row in graph_rows)

    note = (
        "Graph audit checks retrieval freshness (<=48h), data freshness (<=7d), citation completeness per row, and readability via trajectory/pattern labels."
    )
    return {
        "up_to_date": up_to_date,
        "retrieval_fresh": retrieval_fresh,
        "data_fresh": data_fresh,
        "max_data_age_days": max_data_age_days,
        "cited": cited,
        "easy_to_comprehend": easy,
        "note": note,
        "stale_metrics": stale_metrics[:5],
        "missing_citation_metrics": missing_citation_metrics[:5],
        "citations": citations[: min(5, len(citations))],
    }


def _extract_og_description(text: str) -> str:
    match = re.search(r'property="og:description" content="([^"]+)"', text)
    if not match:
        return "Description unavailable"
    return match.group(1).replace("&#39;", "'").strip()


def _extract_title(text: str) -> str:
    match = re.search(r"<title>(.*?)</title>", text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return "Title unavailable"
    return re.sub(r"\s+", " ", match.group(1)).strip()


def _strip_urls(text: str) -> str:
    compact = re.sub(r"https?://\S+", "", text or "")
    compact = re.sub(r"\s+", " ", compact).strip()
    return compact


def _extract_top_mcp_risk(mcp_result: MCPIngestionResult) -> tuple[str, list[Citation]]:
    for snapshot in mcp_result.snapshots:
        for item in snapshot.items:
            text = " ".join([item.title or "", item.content or ""]).lower()
            if any(word in text for word in ["risk", "volatility", "liquidity", "credit", "fragility"]):
                summary = _strip_urls(item.title or item.content or "MCP risk item")[:220]
                citations = [
                    _citation(
                        url=snapshot.endpoint_url,
                        source_id=f"mcp_{snapshot.server_id}",
                        retrieved_at=snapshot.retrieved_at,
                        importance="MCP extracted risk-context item",
                        cached=snapshot.cached,
                    )
                ]
                return summary, citations

    fallback_text = "No high-confidence risk item extracted from MCP content; server availability and capability differences may imply coverage variance."
    now = datetime.now(UTC)
    return (
        fallback_text,
        [
            _citation(
                url="https://registry.modelcontextprotocol.io/v0/servers",
                source_id="mcp_registry",
                retrieved_at=now,
                importance="MCP registry snapshot for coverage context",
                cached=mcp_result.cached_used,
            )
        ],
    )


def _build_big_players_section(
    series: dict[str, dict[str, Any]],
    mcp_result: MCPIngestionResult,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []

    sec_path = CACHE_DIR / "sec_13f_atom.xml"
    if sec_path.exists():
        text = _read_text(sec_path)
        titles = re.findall(r"<title>([^<]+)</title>", text, flags=re.IGNORECASE)
        filings = [title.strip() for title in titles if "13f" in title.lower()][:2]
        if filings:
            items.append(
                {
                    "text": "SEC 13F feed indicates recent filing activity: " + "; ".join(filings),
                    "citations": [
                        _citation(
                            url="https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&type=13F-HR&owner=include&count=10&output=atom",
                            source_id="sec_13f_feed",
                            retrieved_at=_file_timestamp(sec_path),
                            importance="Public SEC 13F filings proxy for large manager disclosures",
                            cached=False,
                        )
                    ],
                }
            )

    fed_path = CACHE_DIR / "fed_press_releases.html"
    if fed_path.exists():
        items.append(
            {
                "text": "Federal Reserve press-release index is tracked as a central-bank policy proxy in this run.",
                "citations": [
                    _citation(
                        url="https://www.federalreserve.gov/newsevents/pressreleases.htm",
                        source_id="fed_press_releases",
                        retrieved_at=_file_timestamp(fed_path),
                        importance="Central bank policy statement proxy",
                        cached=False,
                    )
                ],
            }
        )

    hy = series["BAMLH0A0HYM2"]
    items.append(
        {
            "text": (
                f"Credit-spread proxy (HY OAS) is {hy['latest_value']:.2f} on {hy['latest_date']} "
                f"({hy['change_5obs']:+.2f} over 5 observations), consistent with the current credit-risk backdrop."
            ),
            "citations": [
                _citation(
                    url="https://fred.stlouisfed.org/graph/fredgraph.csv?id=BAMLH0A0HYM2",
                    source_id="fred_bamlh0a0hym2",
                    retrieved_at=datetime.now(UTC),
                    importance="Public credit spread proxy",
                    cached=False,
                )
            ],
        }
    )

    for snapshot in mcp_result.snapshots:
        for item in snapshot.items[:10]:
            text = (item.title or "") + " " + (item.content[:300] if item.content else "")
            lower = text.lower()
            if any(keyword in lower for keyword in ["buyback", "issuance", "13f", "central bank"]):
                items.append(
                    {
                        "text": f"MCP proxy item ({snapshot.server_id}) references large-player activity context: {(item.title or 'untitled')[:160]}",
                        "citations": [
                            _citation(
                                url=snapshot.endpoint_url,
                                source_id=f"mcp_{snapshot.server_id}",
                                retrieved_at=snapshot.retrieved_at,
                                importance="MCP extracted issuer/policy activity proxy",
                                cached=snapshot.cached,
                            )
                        ],
                    }
                )
                break

    return items[:6]


def _tax_lens_text() -> tuple[str, list[Citation]]:
    profile = TaxResidencyProfile(
        profile_id="sg_individual",
        tax_residency="SG",
        base_currency="SGD",
        dta_flags={"ireland_us_treaty_path": True},
        estate_risk_flags={"us_situs_cap_enabled": True},
    )
    candidates = [
        InstrumentTaxProfile(
            instrument_id="us_domiciled_sp500",
            domicile="US",
            us_dividend_exposure=True,
            expected_withholding_rate=0.30,
            us_situs_risk_flag=True,
            expense_ratio=0.0003,
            liquidity_score=0.98,
        ),
        InstrumentTaxProfile(
            instrument_id="ireland_ucits_sp500",
            domicile="IE",
            us_dividend_exposure=True,
            expected_withholding_rate=0.15,
            us_situs_risk_flag=False,
            expense_ratio=0.0007,
            liquidity_score=0.90,
        ),
    ]
    ranked = compare_equivalent_exposures(profile, candidates)

    lines = [
        "The Singapore tax lens compares equivalent exposures using withholding drag, costs, liquidity, and estate-risk flags.",
        "Under this scoring model, the IE UCITS pathway ranks above the US-domiciled equivalent, largely because modeled withholding drag and estate-risk penalties are lower.",
        "This does not imply a universal vehicle preference; it indicates that net-return implementation details can materially change realized outcomes even when gross index exposure is similar.",
        "Current model table:",
    ]
    for score in ranked:
        lines.append(
            f"{score.instrument_id}: score={score.score:.2f}, withholding_drag={score.withholding_drag:.2f}, estate_penalty={score.estate_risk_penalty:.2f}."
        )
    lines.append(
        "The framework is consistent with objective portfolio construction: compare equivalent risk exposures through a net-after-tax lens, then map to policy constraints without issuing execution directives."
    )

    citations = [
        _citation(
            url="https://www.iras.gov.sg/taxes/individual-income-tax/basics-of-individual-income-tax/what-is-taxable-what-is-not/income-received-from-overseas",
            source_id="iras_overseas_income",
            retrieved_at=datetime.now(UTC),
            importance="Primary Singapore tax context reference",
        ),
        _citation(
            url="https://www.irs.gov/individuals/international-taxpayers/federal-income-tax-withholding-and-reporting-on-other-kinds-of-us-source-income-paid-to-nonresident-aliens",
            source_id="irs_withholding_nra",
            retrieved_at=datetime.now(UTC),
            importance="Primary US withholding context reference",
        ),
    ]

    return " ".join(lines), citations


def _build_source_appendix(
    web_records: list[SourceRecord],
    mcp_result: MCPIngestionResult,
    extra_records: list[SourceRecord] | None = None,
) -> list[SourceRecord]:
    dedup: dict[str, SourceRecord] = {}
    for record in [*web_records, *mcp_result.source_records, *(extra_records or [])]:
        dedup[record.source_id] = record
    return list(dedup.values())


def _count_citations(payload_sections: list[list[dict[str, Any]]]) -> int:
    count = 0
    for section in payload_sections:
        for row in section:
            count += len(row.get("citations", []))
    return count


def _as_of_lag_text(citation: Citation | None) -> str:
    if citation is None:
        return "observed_at unavailable"
    observed = citation.observed_at or "observed_at unavailable"
    if citation.lag_days is None:
        return f"as of {observed}, lag unavailable"
    return f"as of {observed}, lag {citation.lag_days} day(s)"


def generate_mcp_omni_email_brief(
    settings: Settings | None = None,
    force_cache_only: bool = False,
    inject_uncited_section: bool = False,
    brief_mode: str | None = None,
    audience_preset: str | None = None,
) -> dict[str, Any]:
    settings = settings or Settings.from_env()
    run_started_at = datetime.now(UTC)
    db_path = get_db_path(settings=settings)
    conn = connect(db_path)
    init_db(conn, SCHEMA_PATH)
    effective_mode = (brief_mode or settings.daily_brief_default_mode or "daily").strip().lower()
    if effective_mode not in {"daily", "weekly", "monthly"}:
        effective_mode = "daily"
    effective_audience = (audience_preset or settings.daily_brief_default_audience or "pm").strip().lower()
    if effective_audience not in {"pm", "client", "client_friendly", "internal", "internal_diagnostic"}:
        effective_audience = "pm"
    if effective_audience == "client":
        effective_audience = "client_friendly"
    if effective_audience == "internal":
        effective_audience = "internal_diagnostic"

    refresh_attempted = bool(settings.refresh_live_cache_on_brief) and not force_cache_only
    refresh_ok, refresh_msg = _refresh_live_cache(settings) if not force_cache_only else (False, "forced cache")
    provider_refresh_result: dict[str, Any] | None = None
    if not force_cache_only:
        try:
            provider_refresh_result = refresh_daily_brief_provider_snapshots(conn, settings, force_refresh=False)
        except Exception:
            provider_refresh_result = {
                "surface_name": "daily_brief",
                "refreshed_at": datetime.now(UTC).isoformat(),
                "items": [],
                "sufficiency": {"status": "failed", "reason": "provider refresh failed"},
            }

    series, fred_citations, cached_used = _load_series_from_cache(
        force_cache_only=force_cache_only or not refresh_ok
    )
    volume_rows, volume_citations, volume_source_records = _load_volume_indicators_from_cache(
        force_cache_only=force_cache_only or not refresh_ok
    )
    sti_proxy_row, sti_proxy_citation, sti_proxy_record = _load_sti_proxy_from_cache(
        force_cache_only=force_cache_only or not refresh_ok
    )
    vea_proxy_row, vea_proxy_citation, vea_proxy_record = _load_vea_proxy_from_cache(
        force_cache_only=force_cache_only or not refresh_ok
    )

    mcp_result = ingest_mcp_omni(settings=settings, enforce_live_gate=True)
    cached_used = cached_used or mcp_result.cached_used

    web_records = fetch_web_sources(settings)
    graph_rows = _build_dual_horizon_graph_rows(series, fred_citations)
    if not graph_rows:
        # Test-friendly fallback when mocked series omit full history payload.
        for code, label, _url in FRED_SERIES:
            row = series.get(code, {})
            citation = next((item for item in fred_citations if item.source_id.endswith(code.lower())), None)
            if not row or citation is None:
                continue
            graph_rows.append(
                {
                    "series_code": code,
                    "metric": f"{label} ({row.get('latest_date', 'n/a')})",
                    "latest_date": row.get("latest_date"),
                    "citation": citation,
                    "as_of": citation.observed_at or row.get("latest_date") or "observed_at unavailable",
                    "lag_days": citation.lag_days,
                    "lag_class": citation.lag_class,
                    "lag_cause": citation.lag_cause,
                    "daily_change_cue": (
                        "1d delta: n/a, series not updated since "
                        + str(citation.observed_at or row.get("latest_date") or "n/a")
                        if citation.lag_days is not None and citation.lag_days > 0
                        else (
                            f"1d delta: {float(row.get('change_1d', 0.0)):+.2f}"
                            if row.get("change_1d") is not None
                            else "1d delta: n/a, insufficient history"
                        )
                    ),
                    "latest": f"{float(row.get('latest_value', 0.0)):.2f}",
                    "delta_5": f"{float(row.get('change_5obs', 0.0)):+.2f}",
                    "trajectory": _trajectory_and_pattern(row.get("points", [0.0, 0.0, 0.0]))[0],
                    "pattern": _trajectory_and_pattern(row.get("points", [0.0, 0.0, 0.0]))[1],
                    "sparkline": row.get("sparkline", ""),
                    "range_bar": row.get("range_bar", ""),
                }
            )
    long_state = _derive_long_state(graph_rows)

    signal_methodology_state = list_regime_methodology(conn)
    signal_thresholds = {
        str(item.get("metric_key")): {
            "watch_threshold": item.get("watch_threshold"),
            "alert_threshold": item.get("alert_threshold"),
            "methodology_note": item.get("methodology_note"),
        }
        for item in list(signal_methodology_state.get("items") or [])
    }
    signals = extended_market_signals(series, threshold_registry=signal_thresholds)
    signal_state_legacy = summarize_signal_state(signals)
    signal_methodology = signal_methodology_registry(
        signal_thresholds,
        methodology_version=str(signal_methodology_state.get("version") or ""),
    )

    now_sgt = datetime.now(SGT)
    stamp = now_sgt.strftime("%Y%m%d_%H%M")
    run_id = f"run_{now_sgt.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

    dgs10 = series["DGS10"]
    t10yie = series["T10YIE"]
    sp500 = series["SP500"]
    vix = series["VIXCLS"]
    hy = series["BAMLH0A0HYM2"]

    oaktree_path = CACHE_DIR / "oaktree_sea_change.html"
    taleb_path = CACHE_DIR / "taleb_fat_tails.html"
    oaktree_text = _read_text(oaktree_path) if oaktree_path.exists() else ""
    taleb_text = _read_text(taleb_path) if taleb_path.exists() else ""
    oaktree_desc = _extract_og_description(oaktree_text) if oaktree_text else "Oaktree memo description unavailable"
    taleb_title = _extract_title(taleb_text) if taleb_text else "Taleb reference unavailable"
    oaktree_pub_date = _extract_publication_date(oaktree_text)
    taleb_pub_date = _extract_publication_date(taleb_text)
    oaktree_quote = _extract_quote(oaktree_desc, max_words=22)
    taleb_quote = _extract_quote(taleb_text, max_words=22)

    oaktree_citation = _citation(
        url="https://www.oaktreecapital.com/insights/memo/sea-change",
        source_id="oaktree_sea_change",
        retrieved_at=_file_timestamp(oaktree_path) if oaktree_path.exists() else datetime.now(UTC),
        importance="Primary Howard Marks cycle memo",
        cached=not refresh_ok,
    )
    taleb_citation = _citation(
        url="https://www.fooledbyrandomness.com/FatTails.html",
        source_id="taleb_fat_tails",
        retrieved_at=_file_timestamp(taleb_path) if taleb_path.exists() else datetime.now(UTC),
        importance="Primary Taleb fat-tail framing reference",
        cached=not refresh_ok,
    )

    mcp_risk_text, mcp_risk_citations = _extract_top_mcp_risk(mcp_result)
    big_players = _build_big_players_section(series, mcp_result)
    alerts_timeline = _generate_alert_events(graph_rows, mcp_risk_text, mcp_risk_citations, big_players)
    short_state = _short_state_from_alerts(alerts_timeline)
    mode_label = {"daily": "Daily", "weekly": "Weekly", "monthly": "Monthly"}[effective_mode]
    audience_label = {
        "pm": "PM",
        "client_friendly": "Client",
        "internal_diagnostic": "Internal",
    }.get(effective_audience, "PM")
    subject = (
        f"SG {mode_label} Market Brief ({audience_label}), {now_sgt.strftime('%Y-%m-%d %H:%M SGT')}, "
        f"Signals {{Long: {long_state}}} {{Short: {short_state}}}"
    )
    citation_by_source = {citation.source_id: citation for citation in fred_citations}
    dgs10_c = citation_by_source.get("fred_dgs10")
    curve_c = citation_by_source.get("fred_t10y2y")
    t10yie_c = citation_by_source.get("fred_t10yie")
    sp500_c = citation_by_source.get("fred_sp500")
    vix_c = citation_by_source.get("fred_vixcls")
    oil_c = citation_by_source.get("fred_dcoilwtico")
    usd_c = citation_by_source.get("fred_dtwexbgs")
    em_vol_c = citation_by_source.get("fred_vxeemcls")
    sgd_c = citation_by_source.get("fred_dexsius")
    sg10_c = citation_by_source.get("fred_irltlt01sgm156n")
    hy_c = citation_by_source.get("fred_bamlh0a0hym2")
    data_recency_summary = _build_data_recency_summary(graph_rows)
    refresh_report = _build_refresh_report(
        brief_run_id=run_id,
        run_started_at=run_started_at,
        run_finished_at=datetime.now(UTC),
        refresh_attempted=refresh_attempted,
        refresh_ok=refresh_ok,
        refresh_msg=refresh_msg,
        graph_rows=graph_rows,
        volume_rows=volume_rows,
        sti_proxy_row=sti_proxy_row,
        vea_proxy_row=vea_proxy_row,
        data_recency_summary=data_recency_summary,
        provider_refresh=provider_refresh_result,
    )
    curve = series.get("T10Y2Y", {})
    oil = series.get("DCOILWTICO", {})
    usd = series.get("DTWEXBGS", {})
    em_vol = series.get("VXEEMCLS", {})
    sgd = series.get("DEXSIUS", {})
    sg10 = series.get("IRLTLT01SGM156N", {})

    executive_policy_context = [
        {
            "text": (
                f"Policy context: DGS10 sits at {dgs10['latest_value']:.2f} ({_as_of_lag_text(dgs10_c)}) "
                f"and T10YIE at {t10yie['latest_value']:.2f} ({_as_of_lag_text(t10yie_c)}), "
                "both interpreted against 5y/10y percentile regimes."
            ),
            "citations": [fred_citations[0], fred_citations[1]],
        },
        {
            "text": (
                f"Policy context: SP500 at {sp500['latest_value']:.2f} ({_as_of_lag_text(sp500_c)}) and "
                f"HY OAS at {hy['latest_value']:.2f} ({_as_of_lag_text(hy_c)}) indicate current valuation "
                "and credit conditions within long-horizon percentile bands."
            ),
            "citations": [fred_citations[2], fred_citations[4]],
        },
        {
            "text": (
                f"Policy context: VIX at {vix['latest_value']:.2f} ({_as_of_lag_text(vix_c)}) is assessed as a regime variable "
                "over 5y and 10y windows for drawdown-resilience monitoring."
            ),
            "citations": [fred_citations[3], fred_citations[0]],
        },
    ]
    if curve:
        executive_policy_context.append(
            {
                "text": (
                    f"Policy context: US 10Y-2Y curve at {float(curve.get('latest_value', 0.0)):.2f} ({_as_of_lag_text(curve_c)}) "
                    "adds curve-shape context so the brief does not rely on rates level alone."
                ),
                "citations": [curve_c] if curve_c is not None else [fred_citations[0]],
            }
        )
    if sgd or sg10:
        local_citations = [citation for citation in [sgd_c, sg10_c] if citation is not None]
        executive_policy_context.append(
            {
                "text": (
                    f"Singapore context: SGD per USD at {float(sgd.get('latest_value', 0.0)):.4f} ({_as_of_lag_text(sgd_c)}) "
                    f"and Singapore 10Y yield at {float(sg10.get('latest_value', 0.0)):.2f} ({_as_of_lag_text(sg10_c)}) "
                    "frame FX translation and local opportunity-cost context for an SGD-based allocator."
                ),
                "citations": local_citations or [fred_citations[0]],
            }
        )
    if sti_proxy_row is not None:
        sti_citations = [citation for citation in [sti_proxy_citation, sgd_c, sg10_c, sp500_c] if citation is not None][:2]
        executive_policy_context.append(
            {
                "text": (
                    f"Singapore context: STI proxy at {float(sti_proxy_row.get('latest_value', 0.0)):.2f} "
                    f"({_as_of_lag_text(sti_proxy_citation)}) adds local-equity context rather than leaving SGD allocators with US-only equity cues."
                ),
                "citations": sti_citations or [fred_citations[2]],
            }
        )
    if data_recency_summary.get("stale_present"):
        executive_policy_context.append(
            {
                "text": "Some inputs are stale. See Data Recency Summary.",
                "citations": [fred_citations[0], fred_citations[1]],
            }
        )

    executive_monitoring_now = [
        {
            "text": (
                f"Monitoring now: short-horizon moves show DGS10 {dgs10['change_5obs']:+.2f} over 5 observations "
                f"({_as_of_lag_text(dgs10_c)}) and SP500 {sp500['change_5obs']:+.2f} ({_as_of_lag_text(sp500_c)}), "
                "flagged for follow-through monitoring."
            ),
            "citations": [fred_citations[0], fred_citations[2]],
        },
        {
            "text": (
                f"Monitoring now: VIX {vix['change_5obs']:+.2f} ({_as_of_lag_text(vix_c)}) and HY OAS {hy['change_5obs']:+.2f} "
                f"({_as_of_lag_text(hy_c)}) over 5 observations may imply short-horizon risk repricing."
            ),
            "citations": [fred_citations[3], fred_citations[4]],
        },
        {
            "text": (
                f"Monitoring now: oil {float(oil.get('change_5obs', 0.0)):+.2f} ({_as_of_lag_text(oil_c)}) and USD broad index "
                f"{float(usd.get('change_5obs', 0.0)):+.2f} ({_as_of_lag_text(usd_c)}) broaden inflation and financial-conditions context."
            ),
            "citations": [citation for citation in [oil_c, usd_c] if citation is not None] or [fred_citations[1]],
        },
        {
            "text": (
                f"Monitoring now: EM volatility at {float(em_vol.get('latest_value', 0.0)):.2f} ({_as_of_lag_text(em_vol_c)}) "
                "helps distinguish broad volatility stress from EM-specific risk conditions."
            ),
            "citations": [em_vol_c] if em_vol_c is not None else [fred_citations[3]],
        },
        {
            "text": f"Monitoring now: MCP risk extraction notes {mcp_risk_text}",
            "citations": mcp_risk_citations[:1] + [fred_citations[1]],
        },
        {
            "text": (
                f"Monitoring now: MCP live coverage is {mcp_result.live_success_count} "
                f"of {mcp_result.connectable_servers} connectable servers."
            ),
            "citations": [fred_citations[0], fred_citations[1]],
        },
    ]
    if volume_rows:
        volume_refs = [row["citation"] for row in volume_rows[:2]]
        executive_monitoring_now.append(
            {
                "text": (
                    f"Monitoring now: index/ETF participation proxies show {volume_rows[0]['metric']} at {volume_rows[0]['latest']}"
                    + (
                        f" and {volume_rows[1]['metric']} at {volume_rows[1]['latest']}"
                        if len(volume_rows) > 1
                        else ""
                    )
                    + f", used as short-horizon participation context (as of {volume_rows[0].get('as_of', 'observed_at unavailable')}, lag {volume_rows[0].get('lag_days', 'n/a')} day(s))."
                ),
                "citations": volume_refs if volume_refs else [fred_citations[2]],
            }
        )

    graph_quality = _graph_quality_audit(graph_rows)
    long_horizon_context = [
        {
            "text": "Percentiles are interpreted as location within long-window distributions; values above 80 or below 20 imply regime extremes relative to the selected historical window.",
            "citations": [fred_citations[0], fred_citations[3]],
        },
        {
            "text": "A moderate-growth policy framework uses multi-year windows to reduce sensitivity to short-lived noise and emphasize structural risk-budget context.",
            "citations": [fred_citations[1], oaktree_citation],
        },
        {
            "text": "A structural regime shift is treated as persistent percentile migration across rates, volatility, inflation expectations, and credit together rather than isolated daily moves.",
            "citations": [fred_citations[0], fred_citations[1], fred_citations[3], fred_citations[4]],
        },
    ]

    market_lens = (
        f"As of {now_sgt.strftime('%Y-%m-%d')}, cross-asset metrics indicate rates at {dgs10['latest_value']:.2f}, VIX at {vix['latest_value']:.2f}, and HY OAS at {hy['latest_value']:.2f}. "
        "The long-horizon percentile framing is consistent with regime-aware policy monitoring, while short-horizon momentum highlights near-term follow-through risk. "
        "This lens remains objective and does not imply directional calls."
    )

    marks_lens = (
        f"As of {now_sgt.strftime('%Y-%m-%d')}, Howard Marks source language emphasizes potential structural shifts linked to inflation and policy response. "
        "The cycle-aware framing aligns with monitoring dispersion and risk-budget discipline over multi-year horizons."
    )

    taleb_lens = (
        f"As of {now_sgt.strftime('%Y-%m-%d')}, Taleb framing is anchored on fat-tail uncertainty and fragility control. "
        "This lens emphasizes survivability under non-linear shocks while keeping monitoring objective and citation-driven."
    )

    tax_lens_body, tax_lens_citations = _tax_lens_text()

    lenses = [
        {
            "title": "Market implied regime lens",
            "body": market_lens,
            "source_title": "FRED Macro Series Composite",
            "source_date": datetime.now(UTC).date().isoformat(),
            "source_date_available": True,
            "quote": None,
            "emphasis": "Cross-asset signal coherence across rates, volatility, inflation expectations, and credit spread pricing.",
            "key_takeaways": [
                "Rates remain elevated while implied volatility is above calm-regime levels.",
                "Inflation expectations eased over the recent window, but credit spread context remains material.",
                "Joint signals are consistent with watch-state monitoring rather than a single-factor narrative.",
            ],
            "what_changes_view": [
                "Sustained normalization in VIX and HY OAS with stable rates momentum.",
                "A persistent shift in inflation expectation momentum relative to rates direction.",
            ],
            "near_term_monitor": [
                {
                    "text": f"Monitor VIX short-horizon percentile and 20-observation momentum for follow-through in volatility regime behavior ({vix['change_5obs']:+.2f} over 5 observations).",
                    "citations": [fred_citations[3]],
                },
                {
                    "text": f"Monitor HY OAS short-horizon percentile for credit-spread confirmation of risk-pricing pressure ({hy['change_5obs']:+.2f} over 5 observations).",
                    "citations": [fred_citations[4]],
                },
            ],
            "citations": [fred_citations[0], fred_citations[1], fred_citations[3], fred_citations[4]],
        },
        {
            "title": "Howard Marks lens",
            "body": marks_lens,
            "source_title": "Sea Change Memo",
            "source_date": oaktree_pub_date,
            "source_date_available": oaktree_pub_date is not None,
            "quote": oaktree_quote,
            "emphasis": "Cycle awareness and regime-shift calibration over prediction confidence.",
            "key_takeaways": [
                "The cited Oaktree framing is consistent with structural regime-transition language.",
                "Cycle-aware monitoring emphasizes dispersion and risk-budget discipline.",
                "Interpretation remains objective and evidence-aggregated rather than directional.",
            ],
            "what_changes_view": [
                "Policy and inflation backdrop converges toward a stable low-volatility regime.",
                "Credit and liquidity proxies detach from cycle-sensitive warning behavior.",
            ],
            "near_term_monitor": [
                {
                    "text": "Monitor short-horizon rate and inflation expectation momentum for signs that cycle-sensitive pricing is either reinforcing or fading.",
                    "citations": [fred_citations[0], fred_citations[1]],
                },
                {
                    "text": "Monitor policy-risk event flow in MCP and central-bank references for corroboration of cycle-transition narratives.",
                    "citations": [oaktree_citation, mcp_risk_citations[0] if mcp_risk_citations else fred_citations[0]],
                },
            ],
            "citations": [oaktree_citation, fred_citations[0]],
        },
        {
            "title": "Taleb lens",
            "body": taleb_lens,
            "source_title": taleb_title,
            "source_date": taleb_pub_date,
            "source_date_available": taleb_pub_date is not None,
            "quote": taleb_quote,
            "emphasis": "Fat-tail uncertainty and fragility control through survivability-oriented portfolio structure.",
            "key_takeaways": [
                "Tail-risk framing prioritizes robustness under discontinuous outcomes.",
                "Current proxy mix does not confirm a tail event, but supports convex capacity monitoring.",
                "The lens informs resilience diagnostics without implying directional calls.",
            ],
            "what_changes_view": [
                "Tail-risk proxies and realized volatility jointly compress for a sustained period.",
                "Convex sleeve cost-benefit deteriorates materially relative to current assumptions.",
            ],
            "near_term_monitor": [
                {
                    "text": "Monitor short-horizon volatility percentile and dislocation alerts for early warning of non-linear stress paths.",
                    "citations": [fred_citations[3]],
                },
                {
                    "text": "Monitor multi-asset confirmation between volatility and credit spread behavior before classifying a persistent tail-risk regime.",
                    "citations": [fred_citations[3], fred_citations[4]],
                },
            ],
            "citations": [taleb_citation, fred_citations[3]],
        },
        {
            "title": "Singapore tax lens",
            "body": tax_lens_body,
            "source_title": "IRAS/IRS Withholding and Overseas Income References",
            "source_date": None,
            "source_date_available": False,
            "quote": None,
            "emphasis": "Net-after-tax implementation differences across equivalent exposures for SG-resident investors.",
            "key_takeaways": [
                "Withholding drag and estate risk assumptions can materially affect net outcomes.",
                "The ranking framework compares equivalent exposures without issuing execution directives.",
                "Implementation details remain relevant even when gross benchmark exposure is similar.",
            ],
            "what_changes_view": [
                "Treaty, withholding, or tax-treatment assumptions change from cited references.",
                "Vehicle-level costs and liquidity profile diverge from current model inputs.",
            ],
            "near_term_monitor": [
                {
                    "text": "Monitor source updates in withholding and overseas income references for changes that alter modeled drag assumptions.",
                    "citations": tax_lens_citations,
                },
            ],
            "citations": tax_lens_citations,
        },
    ]

    if inject_uncited_section:
        lenses.append({"title": "Injected uncited section", "body": "This should fail citation gate.", "citations": []})

    _validate_cited_items("Alerts timeline", alerts_timeline)
    opportunities = _generate_opportunity_observations(graph_rows, tax_lens_citations)
    _validate_cited_items("Opportunity observations", opportunities)
    implementation_mapping = build_implementation_mapping(opportunities=opportunities, retrieved_at=datetime.now(UTC))
    _validate_implementation_mapping(implementation_mapping)

    allocation = {
        "global_equities": 0.35,
        "satellite_equities": 0.15,
        "ig_bonds": 0.20,
        "cash_bills": 0.10,
        "real_assets": 0.10,
        "alternatives": 0.07,
        "convex": 0.03,
    }

    portfolio_mapping = [
        {
            "text": "Allocation drift placeholder: no holdings provided. Policy target remains 35/15/20/10/10/7/3 across sleeves.",
            "citations": [fred_citations[0], oaktree_citation],
        },
        {
            "text": "Primary sleeve sensitivities: long-horizon percentile shifts in rates and inflation map to policy sleeves, while short-horizon volatility and credit moves map to monitoring overlays.",
            "citations": [fred_citations[1], fred_citations[3], fred_citations[4]],
        },
        {
            "text": (
                "Target return context: the stated 6%-10% average objective is most consistent with maintaining growth/risk balance over multi-year horizons; "
                "current index and ETF volume trajectories should be monitored for participation regime changes rather than used as stand-alone timing signals."
            ),
            "citations": [fred_citations[2], volume_rows[0]["citation"]]
            if volume_rows
            else [fred_citations[2], fred_citations[3]],
        },
    ]

    convex_positions = [
        ConvexSleevePosition(
            symbol="DBMF",
            allocation_weight=0.02,
            retail_accessible=True,
            margin_required=False,
            max_loss_known=True,
            instrument_type="managed_futures_etf",
        ),
        ConvexSleevePosition(
            symbol="TAIL",
            allocation_weight=0.007,
            retail_accessible=True,
            margin_required=False,
            max_loss_known=True,
            instrument_type="tail_hedge_fund",
        ),
        ConvexSleevePosition(
            symbol="SPX_PUT_LADDER",
            allocation_weight=0.003,
            retail_accessible=True,
            margin_required=False,
            max_loss_known=True,
            instrument_type="long_put_option",
        ),
    ]
    convex_check = validate_retail_safe_convex(convex_positions)
    convex_targets = {
        "Managed Futures (2.0%)": 0.020,
        "Tail Hedge (0.7%)": 0.007,
        "Long Puts (0.3%)": 0.003,
    }
    convex_actuals = {
        "Managed Futures (2.0%)": sum(
            position.allocation_weight
            for position in convex_positions
            if position.instrument_type == "managed_futures_etf"
        ),
        "Tail Hedge (0.7%)": sum(
            position.allocation_weight
            for position in convex_positions
            if position.instrument_type == "tail_hedge_fund"
        ),
        "Long Puts (0.3%)": sum(
            position.allocation_weight
            for position in convex_positions
            if position.instrument_type == "long_put_option"
        ),
    }
    target_breakdown = []
    for component, target in convex_targets.items():
        actual = convex_actuals.get(component, 0.0)
        target_breakdown.append(
            {
                "component": component,
                "target": target,
                "actual": actual,
                "within_target": abs(actual - target) < 1e-6,
            }
        )

    convex_report = {
        "total_weight": convex_check.total_weight,
        "valid": convex_check.valid,
        "errors": convex_check.errors,
        "target_breakdown": target_breakdown,
        "margin_required_any": any(position.margin_required for position in convex_positions),
        "max_loss_known_all": all(position.max_loss_known for position in convex_positions),
    }

    source_appendix = _build_source_appendix(
        web_records,
        mcp_result,
        extra_records=[
            *volume_source_records,
            *([sti_proxy_record] if sti_proxy_record is not None else []),
            *([vea_proxy_record] if vea_proxy_record is not None else []),
            *implementation_mapping.get("source_records", []),
        ],
    )
    source_data_asof = {
        FRED_SOURCE_BY_CODE.get(code, f"fred_{code.lower()}"): str(series.get(code, {}).get("latest_date") or "")
        for code, _label, _url in FRED_SERIES
    }

    db_errors: list[dict[str, str]] = []
    mcp_updates: dict[str, Any] = {
        "coverage": {
            "connectable": mcp_result.connectable_servers,
            "live_successes": mcp_result.live_success_count,
            "success_ratio": mcp_result.live_success_ratio,
        },
        "new_items": [],
        "changed_items": [],
        "no_new_items": True,
    }
    what_changed: list[dict[str, Any]] = [
        {
            "text": "No previous run comparison available; persistence step did not complete.",
            "citations": fred_citations[:1],
        }
    ]
    policy_pack: dict[str, Any] = {}
    chart_payloads: list[dict[str, Any]] = []
    approval_record: dict[str, Any] | None = None
    version_record: dict[str, Any] | None = None
    ips_snapshot_record: dict[str, Any] | None = None
    try:
        _ensure_dual_horizon_tables(conn)
        _ensure_brief_metadata_tables(conn)
        _persist_series_observations(conn, series, fred_citations)
        _persist_metric_snapshots(conn, run_id=run_id, graph_rows=graph_rows)

        previous_summary = _load_previous_run_summary(conn)
        current_summary = _run_summary_payload(
            long_state=long_state,
            short_state=short_state,
            graph_rows=graph_rows,
            alerts=alerts_timeline,
            events=big_players,
        )
        regime_delta_lines = _build_what_changed(previous_summary, current_summary, fred_citations)
        quantified_movers = _build_quantified_top_movers(
            conn=conn,
            run_id=run_id,
            graph_rows=graph_rows,
            default_citations=fred_citations,
        )
        what_changed = regime_delta_lines + [
            {
                "text": "Top 5 quantified changes since last report:",
                "citations": fred_citations[:2],
            },
            *quantified_movers,
        ]
        mcp_updates = _build_mcp_updates(conn=conn, mcp_result=mcp_result)
        _persist_run_and_alerts(
            conn=conn,
            run_id=run_id,
            created_at=datetime.now(UTC),
            long_state=long_state,
            short_state=short_state,
            summary=current_summary,
            alerts=alerts_timeline,
        )
        policy_pack = build_policy_pack(
            conn,
            brief_run_id=run_id,
            default_allocation=allocation,
            long_state=long_state,
            short_state=short_state,
        )
        policy_citations = [
            citation.model_dump(mode="json")
            for section_key in ("expected_returns", "benchmark", "aggregate_drawdown", "stress")
            for citation in list((policy_pack.get(section_key) or {}).get("citations") or [])
        ]
        policy_citation_health = summarize_policy_citation_health(policy_citations)
        policy_pack["policy_citation_health"] = policy_citation_health
        if not bool(policy_citation_health.get("guidance_ready")):
            policy_pack["trust_banner"] = {
                **dict(policy_pack.get("trust_banner") or {}),
                "trust_level": "market_monitoring_only",
                "label": "Market monitoring only",
                "guidance_ready": False,
            }
        source_appendix = _build_source_appendix(
            web_records,
            mcp_result,
            extra_records=[
                *volume_source_records,
                *([sti_proxy_record] if sti_proxy_record is not None else []),
                *([vea_proxy_record] if vea_proxy_record is not None else []),
                *implementation_mapping.get("source_records", []),
                *list(policy_pack.get("policy_source_records") or []),
            ],
        )
        scenario_snapshots = record_scenario_comparisons(
            conn,
            current_run_id=run_id,
            prior_run_id=((policy_pack.get("history_compare") or {}).get("prior") or {}).get("brief_run_id"),
            comparisons=dict(policy_pack.get("scenario_compare") or {}),
        )
        policy_pack["scenario_comparison_snapshots"] = scenario_snapshots
        record_regime_history(
            conn,
            brief_run_id=run_id,
            as_of_ts=now_sgt.isoformat(),
            long_state=long_state,
            short_state=short_state,
            change_summary=" | ".join(
                [first["text"] for first in what_changed[:2] if isinstance(first, dict) and first.get("text")]
            )
            if what_changed
            else None,
            confidence_label="medium",
        )
        policy_pack["history_compare"] = build_history_compare(conn, current_run_id=run_id)
        ips_snapshot_record = persist_ips_snapshot(
            conn,
            brief_run_id=run_id,
            snapshot=dict(policy_pack.get("ips_snapshot") or {}),
            benchmark_definition_id=(policy_pack.get("benchmark") or {}).get("benchmark_definition_id"),
            cma_version=current_cma_version(conn),
        )
        allocation_items = [
            (
                str(item.get("sleeve_key") or ""),
                float(item.get("target_weight") or 0.0),
            )
            for item in list((policy_pack.get("core_satellite_summary") or []))
            if float(item.get("target_weight") or 0.0) > 0
        ]
        benchmark_items = [
            (
                str(item.get("component_key") or ""),
                float(item.get("weight") or 0.0),
            )
            for item in list((policy_pack.get("benchmark") or {}).get("components") or [])
        ]
        stress_items = [
            (
                str(item.get("name") or item.get("scenario_id") or ""),
                float(item.get("estimated_impact_pct") or 0.0),
            )
            for item in list((policy_pack.get("stress") or {}).get("scenarios") or [])[:6]
        ]
        chart_payloads = build_brief_charts(
            allocation_items=allocation_items,
            benchmark_items=benchmark_items,
            stress_items=stress_items,
            regime_history=list(policy_pack.get("history_compare", {}).get("history") or []),
        )
        persisted_charts: list[dict[str, Any]] = []
        for item in chart_payloads:
            persisted = store_chart_artifact(
                conn,
                brief_run_id=run_id,
                chart_key=str(item.get("chart_key")),
                title=str(item.get("title")),
                svg=str(item.get("svg") or ""),
                source_as_of=now_sgt.date().isoformat(),
                freshness_note="Rendered from policy and scenario inputs available at brief generation time.",
            )
            persisted_charts.append({**item, **persisted})
        chart_payloads = persisted_charts
        approval_record = create_or_refresh_approval(
            conn,
            brief_run_id=run_id,
            status="generated",
            notes="Auto-generated Daily Brief awaiting optional review and approval workflow.",
        )
        version_record = record_brief_versions(
            conn,
            brief_run_id=run_id,
            content_version="2026.03.07",
            policy_pack_version=POLICY_PACK_VERSION,
            benchmark_definition_version=str((policy_pack.get("benchmark") or {}).get("version") or "2026.03"),
            cma_version=current_cma_version(conn),
            chart_version="2026.03.07",
            payload=_jsonable({
                "brief_mode": effective_mode,
                "audience_preset": effective_audience,
                "chart_count": len(chart_payloads),
                "variant_label": f"{effective_mode}:{effective_audience}:baseline_a",
                "sections": ["top_sheet", "policy_layer", "execution_layer", "evidence_layer"],
                "signal_methodology": signal_methodology,
                "policy_weights": dict(policy_pack.get("policy_weights") or {}),
                "scenario_compare": dict(policy_pack.get("scenario_compare") or {}),
                "executive_monitoring_now": _jsonable(executive_monitoring_now),
                "what_changed": _jsonable(what_changed),
                "alerts_timeline": _jsonable(alerts_timeline),
                "series_state": {
                    code: {
                        "latest_date": row.get("latest_date"),
                        "latest_value": row.get("latest_value"),
                        "lag_days": row.get("lag_days"),
                        "lag_class": row.get("lag_class"),
                    }
                    for code, row in series.items()
                },
                "long_state": long_state,
                "short_state": short_state,
                "implication_summary": [str(item.get("text") or "") for item in executive_monitoring_now[:6]],
                "action_guidance_summary": [str(item.get("what_to_consider") or item.get("action_tag") or "") for item in what_changed[:12]],
                "graph_rows": [
                    {
                        **{
                            key: value
                            for key, value in dict(row).items()
                            if key != "citation"
                        },
                        "citation": row.get("citation").model_dump(mode="json") if row.get("citation") is not None else None,
                    }
                    for row in graph_rows
                ],
                "singapore_context": {
                    "sti_proxy_row": _jsonable(
                        {
                            **dict(sti_proxy_row or {}),
                            "series_code": "STI_PROXY",
                            "citation": sti_proxy_citation.model_dump(mode="json") if sti_proxy_citation is not None else None,
                        }
                    )
                    if sti_proxy_row
                    else None,
                    "developed_ex_us_proxy_row": _jsonable(
                        {
                            **dict(vea_proxy_row or {}),
                            "series_code": "VEA_PROXY",
                            "citation": vea_proxy_citation.model_dump(mode="json") if vea_proxy_citation is not None else None,
                        }
                    )
                    if vea_proxy_row
                    else None,
                    "sg_local_rate_row": _jsonable(
                        {
                            "series_code": "IRLTLT01SGM156N",
                            "metric": "Singapore 10Y Government Yield",
                            "latest_value": sg10.get("latest_value"),
                            "latest_date": sg10.get("latest_date"),
                            "lag_days": sg10_c.lag_days if sg10_c is not None else None,
                            "lag_class": sg10_c.lag_class if sg10_c is not None else None,
                            "lag_cause": sg10_c.lag_cause if sg10_c is not None else None,
                            "change_5obs": sg10.get("change_5obs"),
                            "citation": sg10_c.model_dump(mode="json") if sg10_c is not None else None,
                        }
                    )
                    if sg10
                    else None,
                    "sgd_row": _jsonable(
                        {
                            "series_code": "DEXSIUS",
                            "metric": "SGD per USD",
                            "latest_value": sgd.get("latest_value"),
                            "latest_date": sgd.get("latest_date"),
                            "lag_days": sgd_c.lag_days if sgd_c is not None else None,
                            "lag_class": sgd_c.lag_class if sgd_c is not None else None,
                            "lag_cause": sgd_c.lag_cause if sgd_c is not None else None,
                            "change_5obs": sgd.get("change_5obs"),
                            "citation": sgd_c.model_dump(mode="json") if sgd_c is not None else None,
                        }
                    )
                    if sgd
                    else None,
                },
                "policy_pack": _jsonable({
                    "version": policy_pack.get("version"),
                    "trust_banner": policy_pack.get("trust_banner"),
                    "policy_truth_state": policy_pack.get("policy_truth_state"),
                    "policy_labels": policy_pack.get("policy_labels"),
                    "policy_source_records": policy_pack.get("policy_source_records"),
                    "expected_returns": policy_pack.get("expected_returns"),
                    "benchmark": policy_pack.get("benchmark"),
                    "aggregate_drawdown": policy_pack.get("aggregate_drawdown"),
                    "rebalancing_policy": policy_pack.get("rebalancing_policy"),
                    "ips_snapshot": policy_pack.get("ips_snapshot"),
                    "core_satellite_summary": policy_pack.get("core_satellite_summary"),
                    "sub_sleeve_breakdown": policy_pack.get("sub_sleeve_breakdown"),
                    "fund_selection": policy_pack.get("fund_selection"),
                    "tax_location_guidance": policy_pack.get("tax_location_guidance"),
                    "dca_guidance": policy_pack.get("dca_guidance"),
                    "review_queue": policy_pack.get("review_queue"),
                    "history_compare": policy_pack.get("history_compare"),
                    "scenario_compare": policy_pack.get("scenario_compare"),
                    "scenario_registry": policy_pack.get("scenario_registry"),
                    "portfolio_relevance": policy_pack.get("portfolio_relevance"),
                    "policy_citation_health": policy_pack.get("policy_citation_health"),
                    "stress": policy_pack.get("stress"),
                }),
            }),
        )
        conn.execute(
            """
            INSERT INTO daily_brief_runs (
              brief_run_id, source_run_id, generated_at, status, brief_mode, audience_preset,
              delivery_state, approval_required, summary, diagnostics_json, content_version,
              policy_pack_version, benchmark_definition_version, cma_version, chart_version
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(brief_run_id) DO UPDATE SET
              source_run_id=excluded.source_run_id,
              generated_at=excluded.generated_at,
              status=excluded.status,
              brief_mode=excluded.brief_mode,
              audience_preset=excluded.audience_preset,
              delivery_state=excluded.delivery_state,
              approval_required=excluded.approval_required,
              summary=excluded.summary,
              diagnostics_json=excluded.diagnostics_json,
              content_version=excluded.content_version,
              policy_pack_version=excluded.policy_pack_version,
              benchmark_definition_version=excluded.benchmark_definition_version,
              cma_version=excluded.cma_version,
              chart_version=excluded.chart_version
            """,
            (
                run_id,
                run_id,
                now_sgt.isoformat(),
                "ok",
                effective_mode,
                effective_audience,
                "generated",
                1 if settings.daily_brief_require_approval_before_send else 0,
                subject,
                json.dumps(
                    {
                        "md_prefix": f"mcp_omni_email_{stamp}",
                        "long_state": long_state,
                        "short_state": short_state,
                        "alert_count": len(alerts_timeline),
                        "chart_count": len(chart_payloads),
                        "audience_preset": effective_audience,
                        "brief_mode": effective_mode,
                        "stale_present": bool(data_recency_summary.get("stale_present")),
                        "policy_trust_banner": dict(policy_pack.get("trust_banner") or {}),
                        "policy_citation_health": dict(policy_pack.get("policy_citation_health") or {}),
                        "refresh_report": refresh_report,
                        "signal_methodology_version": signal_methodology.get("version"),
                        "policy_assumption_version": current_cma_version(conn),
                        "benchmark_profile_version": str((policy_pack.get("benchmark") or {}).get("version") or "n/a"),
                        "stress_methodology_version": ",".join(
                            sorted(
                                {
                                    str(item.get("scenario_version") or "1.0")
                                    for item in list((policy_pack.get("stress") or {}).get("scenarios") or [])
                                }
                            )
                        ),
                        "implication_summary": [str(item.get("text") or "") for item in executive_monitoring_now[:5]],
                    }
                ),
                "2026.03.07",
                POLICY_PACK_VERSION,
                str((policy_pack.get("benchmark") or {}).get("version") or "2026.03"),
                current_cma_version(conn),
                "2026.03.07",
            ),
        )
        conn.commit()
        conn.close()
    except Exception as exc:
        db_errors.append({"stage": "report_state_persist", "error": str(exc)})
        if "conn" in locals():
            try:
                conn.close()
            except Exception:
                pass

    try:
        executive_policy_context = tag_items(executive_policy_context)
        executive_monitoring_now = tag_items(executive_monitoring_now)
        what_changed = tag_items(what_changed)
        portfolio_mapping = tag_items(portfolio_mapping)
        alerts_timeline = tag_items(alerts_timeline)
        opportunities = tag_items(opportunities)
        big_players = tag_items(big_players)
        rendered = build_narrated_email_brief(
            subject=subject,
            generated_at_sgt=now_sgt.isoformat(),
            executive_snapshot=[*executive_policy_context, *executive_monitoring_now],
            graph_rows=graph_rows,
            graph_quality_audit=graph_quality,
            lenses=lenses,
            big_players=big_players,
            portfolio_mapping=portfolio_mapping,
            convex_report=convex_report,
            source_appendix=source_appendix,
            allocation=allocation,
            long_state=long_state,
            short_state=short_state,
            executive_policy_context=executive_policy_context,
            executive_monitoring_now=executive_monitoring_now,
            what_changed=what_changed,
            long_horizon_context=long_horizon_context,
            alerts_timeline=alerts_timeline,
            opportunities=opportunities,
            implementation_mapping=implementation_mapping,
            data_recency_summary=data_recency_summary,
            mcp_updates=mcp_updates,
            source_data_asof=source_data_asof,
            policy_pack=policy_pack,
            chart_payloads=chart_payloads,
            approval_record=approval_record,
            brief_mode=effective_mode,
            audience_preset=effective_audience,
        )
    except (CitationPolicyError, ReportBuildError) as exc:
        raise OmniBriefError(f"Citation validation gate failure: {exc}") from exc

    try:
        assert_no_directive_language([rendered["markdown"], rendered["html"]])
    except ValueError as exc:
        raise OmniBriefError(f"Language safety gate failure: {exc}") from exc

    files = write_narrated_email_files(
        prefix=f"mcp_omni_email_{stamp}",
        markdown=rendered["markdown"],
        html=rendered["html"],
    )

    citations_count = (
        _count_citations(
            [
                executive_policy_context,
                executive_monitoring_now,
                what_changed,
                long_horizon_context,
                lenses,
                big_players,
                portfolio_mapping,
                alerts_timeline,
                opportunities,
                [
                    {"citations": candidate.citations}
                    for payload in implementation_mapping.get("sleeves", {}).values()
                    for candidate in payload.get("candidates", [])
                ],
                [
                    {"citations": item.get("citations", [])}
                    for payload in implementation_mapping.get("sleeves", {}).values()
                    for item in payload.get("sg_tax_observations", [])
                ],
                [
                    {"citations": item.get("citations", [])}
                    for item in implementation_mapping.get("watchlist_candidates", [])
                ],
                [
                    {"citations": list((policy_pack.get(section_key) or {}).get("citations") or [])}
                    for section_key in ("expected_returns", "benchmark", "aggregate_drawdown", "stress")
                ],
            ]
        )
        + len(source_appendix)
        + len(graph_quality.get("citations", []))
    )
    graph_metadata = []
    for row in graph_rows:
        normalized_row = dict(row)
        citation = normalized_row.get("citation")
        if citation is not None:
            normalized_row["citation"] = citation.model_dump(mode="json")
        graph_metadata.append(normalized_row)

    return {
        "subject": subject,
        "md_path": files["md_path"],
        "html_path": files["html_path"],
        "pdf_path": files["pdf_path"],
        "mcp_connected_count": mcp_result.connected_servers,
        "mcp_total_count": mcp_result.total_servers,
        "mcp_connectable_count": mcp_result.connectable_servers,
        "mcp_live_success_ratio": mcp_result.live_success_ratio,
        "run_id": run_id,
        "long_state": long_state,
        "short_state": short_state,
        "alert_count": len(alerts_timeline),
        "opportunity_count": len(opportunities),
        "signals_summary": {
            "state": signal_state_legacy,
            "long_state": long_state,
            "short_state": short_state,
            "signals": [signal.model_dump(mode="json") for signal in signals],
            "graph_metadata": graph_metadata,
            "graph_quality": {
                **graph_quality,
                "citations": [citation.model_dump(mode="json") for citation in graph_quality.get("citations", [])],
            },
        },
        "citations_count": citations_count,
        "cached_used": cached_used,
        "policy_pack": policy_pack,
        "chart_artifacts": chart_payloads,
        "approval": approval_record,
        "brief_versions": version_record,
        "ips_snapshot": ips_snapshot_record,
        "brief_mode": effective_mode,
        "audience_preset": effective_audience,
        "freshness_ok": not bool(data_recency_summary.get("stale_present")),
        "refresh_report": refresh_report,
        "policy_guidance_ready": bool((policy_pack.get("trust_banner") or {}).get("guidance_ready")),
        "policy_trust_banner": dict(policy_pack.get("trust_banner") or {}),
        "policy_citation_health": dict(policy_pack.get("policy_citation_health") or {}),
        "mcp_live_gate_passed": bool(mcp_result.live_success_ratio > 0),
        "errors": mcp_result.errors
        + db_errors
        + ([{"stage": "cache_refresh", "error": refresh_msg}] if not refresh_ok else []),
    }


def generate_real_world_sample_email_doc() -> dict[str, Any]:
    """Backward-compatible wrapper for previous endpoint behavior."""
    result = generate_mcp_omni_email_brief()
    return {
        "subject": result["subject"],
        "md_path": result["md_path"],
        "html_path": result["html_path"],
        "pdf_path": result["pdf_path"],
        "mcp_count": result["mcp_total_count"],
        "series": {
            row["metric"]: {
                "latest": row["latest"],
                "delta_5": row["delta_5"],
            }
            for row in result["signals_summary"]["graph_metadata"]
        },
    }
