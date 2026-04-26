from __future__ import annotations

from datetime import UTC, datetime, timedelta
import sqlite3
from typing import Any, Literal

from pydantic import Field

from app.config import get_db_path
from app.services.provider_cache import get_cached_provider_snapshot
from app.services.provider_registry import provider_support_status, routed_provider_candidates
from app.services.provider_refresh import fetch_routed_family
from app.v2.core.domain_objects import (
    EvidenceCitation,
    EvidencePack,
    MarketDataPoint,
    MarketSeriesTruth,
    V2Model,
    utc_now_iso,
)
from app.v2.forecasting.capabilities import ForecastBundle
from app.v2.sources.execution_envelope import build_provider_execution, payload_execution_profile
from app.v2.truth.envelopes import build_truth_envelope


ChartSeriesType = Literal["line", "area", "histogram"]


class ChartPointContract(V2Model):
    timestamp: str
    value: float


class ChartSeriesContract(V2Model):
    chart_id: str
    series_type: ChartSeriesType
    label: str
    points: list[ChartPointContract] = Field(default_factory=list)
    unit: str
    source_family: str
    source_label: str
    freshness_state: str
    trust_state: str


class ChartBandContract(V2Model):
    band_id: str
    label: str
    upper_points: list[ChartPointContract] = Field(default_factory=list)
    lower_points: list[ChartPointContract] = Field(default_factory=list)
    meaning: str
    degraded_state: str | None = None


class ChartMarkerContract(V2Model):
    marker_id: str
    timestamp: str
    label: str
    marker_type: str
    linked_object_id: str | None = None
    linked_surface: str | None = None
    summary: str


class ChartThresholdContract(V2Model):
    threshold_id: str
    label: str
    value: float
    threshold_type: str
    action_if_crossed: str
    what_it_means: str


class ChartCalloutContract(V2Model):
    callout_id: str
    label: str
    tone: str
    detail: str


class ChartLogicContract(V2Model):
    current_value: float | None = None
    previous_value: float | None = None
    trigger_level: float | None = None
    confirm_above: bool | None = None
    break_below: bool | None = None
    bands: list[dict[str, Any]] = Field(default_factory=list)
    current_band: str | None = None
    release_date: str | None = None
    as_of_date: str = ""
    # decomposition family: each element is {label, value, target, low, high, unit}
    allocation_bars: list[dict[str, Any]] = Field(default_factory=list)


class ChartPanelContract(V2Model):
    panel_id: str
    title: str
    chart_type: str
    chart_mode: str = "market_context"
    primary_series: ChartSeriesContract | None = None
    comparison_series: ChartSeriesContract | None = None
    bands: list[ChartBandContract] = Field(default_factory=list)
    markers: list[ChartMarkerContract] = Field(default_factory=list)
    thresholds: list[ChartThresholdContract] = Field(default_factory=list)
    callouts: list[ChartCalloutContract] = Field(default_factory=list)
    summary: str
    what_to_notice: str
    degraded_state: str | None = None
    freshness_state: str = "degraded_monitoring_mode"
    trust_state: str = "bounded_support"
    chart_logic: ChartLogicContract | None = None


def _connection() -> sqlite3.Connection:
    conn = sqlite3.connect(get_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _cached_family_payload(
    conn: sqlite3.Connection,
    *,
    surface_name: str,
    endpoint_family: str,
    identifier: str,
) -> dict[str, Any]:
    cache_keys = [str(identifier)]
    upper_identifier = str(identifier or "").strip().upper()
    if upper_identifier and upper_identifier not in cache_keys:
        cache_keys.append(upper_identifier)
    for provider_name in routed_provider_candidates(endpoint_family, identifier=identifier):
        supported, _ = provider_support_status(provider_name, endpoint_family, identifier)
        if not supported:
            continue
        for cache_key in cache_keys:
            snapshot = get_cached_provider_snapshot(
                conn,
                provider_name=provider_name,
                endpoint_family=endpoint_family,
                cache_key=cache_key,
                surface_name=surface_name,
            )
            if snapshot is None:
                continue
            payload = dict(snapshot.get("payload") or {})
            payload.setdefault("provider_name", provider_name)
            payload.setdefault("retrieval_path", "routed_cache")
            payload.setdefault("cache_status", str(snapshot.get("cache_status") or "hit"))
            payload.setdefault("freshness_state", snapshot.get("freshness_state"))
            return payload
    return {
        "provider_name": None,
        "endpoint_family": endpoint_family,
        "identifier": identifier,
        "cache_status": "unavailable",
        "freshness_state": "unavailable",
        "error_state": "cached_series_missing",
        "retrieval_path": "routed_unavailable",
    }


def _with_provider_execution(payload: dict[str, Any], *, endpoint_family: str, identifier: str) -> dict[str, Any]:
    enriched = dict(payload)
    if isinstance(enriched.get("provider_execution"), dict) and enriched.get("provider_execution"):
        execution = dict(enriched.get("provider_execution") or {})
        if endpoint_family == "market_close" and str(enriched.get("provider_name") or execution.get("provider_name") or "").strip() == "yahoo_finance":
            execution["provenance_strength"] = "public_verified_close"
            enriched["provenance_strength"] = "public_verified_close"
        if not isinstance(enriched.get("truth_envelope"), dict) or not enriched.get("truth_envelope"):
            if endpoint_family == "market_close":
                observed_at = str(enriched.get("observed_at") or execution.get("observed_at") or "").strip() or None
                observed_day = observed_at[:10] if observed_at else None
                enriched["truth_envelope"] = build_truth_envelope(
                    as_of_utc=observed_day,
                    observation_date=observed_day,
                    reference_period=observed_day,
                    source_authority=str(enriched.get("provider_name") or execution.get("provider_name") or "").strip() or None,
                    acquisition_mode=str(execution.get("live_or_cache") or "live"),
                    degradation_reason=str(enriched.get("error_state") or execution.get("error_state") or "").strip() or None,
                    recommendation_critical=True,
                    retrieved_at_utc=str(enriched.get("fetched_at") or execution.get("fetched_at") or utc_now_iso()),
                    period_clock_class="daily_market_close",
                )
        enriched["provider_execution"] = execution
        return enriched
    if endpoint_family in {"quote_latest", "benchmark_proxy", "fx_reference", "macro_fx_proxy", "usd_strength_fallback"}:
        derived_change = _derived_change_pct(enriched)
        if derived_change is not None and enriched.get("change_pct_1d") is None:
            enriched["change_pct_1d"] = derived_change
    path_used = str(enriched.get("retrieval_path") or "routed_unavailable")
    cache_status = str(enriched.get("cache_status") or "").strip() or None
    execution_profile = payload_execution_profile(payload=enriched, source_family=endpoint_family)
    execution = build_provider_execution(
        provider_name=str(enriched.get("provider_name") or "").strip() or None,
        source_family=endpoint_family,
        identifier=str(identifier or "").strip(),
        provider_symbol=str(enriched.get("provider_symbol") or identifier or "").strip() or None,
        observed_at=str(enriched.get("observed_at") or "").strip() or None,
        fetched_at=str(enriched.get("fetched_at") or "").strip() or None,
        cache_status=cache_status,
        fallback_used="fallback" in path_used or "unavailable" in path_used,
        error_state=str(enriched.get("error_state") or "").strip() or None,
        freshness_class=_series_freshness(enriched),
        path_used=path_used,
        live_or_cache="cache" if cache_status in {"hit", "stale_reuse"} else "fallback" if "fallback" in path_used or "unavailable" in path_used else "live",
        usable_truth=bool(execution_profile.get("usable_truth")),
        semantic_grade=str(execution_profile.get("semantic_grade") or "").strip() or None,
        sufficiency_state=str(execution_profile.get("sufficiency_state") or "").strip() or None,
        data_mode=str(execution_profile.get("data_mode") or "").strip() or None,
        authority_level=str(execution_profile.get("authority_level") or "").strip() or None,
        provenance_strength=str(enriched.get("provenance_strength") or "").strip() or None,
        insufficiency_reason=str(enriched.get("error_state") or "").strip() or None,
    )
    enriched["provider_execution"] = execution
    enriched["usable_truth"] = execution.get("usable_truth")
    enriched["sufficiency_state"] = execution.get("sufficiency_state")
    enriched["data_mode"] = execution.get("data_mode")
    enriched["authority_level"] = execution.get("authority_level")
    enriched["provenance_strength"] = execution.get("provenance_strength")
    if not isinstance(enriched.get("truth_envelope"), dict) or not enriched.get("truth_envelope"):
        if endpoint_family == "market_close":
            observed_at = str(enriched.get("observed_at") or "").strip() or None
            observed_day = observed_at[:10] if observed_at else None
            enriched["truth_envelope"] = build_truth_envelope(
                as_of_utc=observed_day,
                observation_date=observed_day,
                reference_period=observed_day,
                source_authority=str(enriched.get("provider_name") or "").strip() or None,
                acquisition_mode=str(execution.get("live_or_cache") or "live"),
                degradation_reason=str(enriched.get("error_state") or "").strip() or None,
                recommendation_critical=True,
                retrieved_at_utc=str(enriched.get("fetched_at") or utc_now_iso()),
                period_clock_class="daily_market_close",
            )
    return enriched


def _safe_float(value: Any) -> float | None:
    try:
        if value in {None, ""}:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _parse_iso(text: str | None) -> datetime:
    raw = str(text or "").strip()
    if not raw:
        return datetime.now(UTC)
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return datetime.now(UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _timestamp_value(row: dict[str, Any]) -> str:
    for key in ("date", "datetime", "timestamp", "observed_at", "t"):
        value = row.get(key)
        if value not in {None, ""}:
            if isinstance(value, (int, float)):
                return datetime.fromtimestamp(float(value), tz=UTC).isoformat()
            return _parse_iso(str(value)).isoformat()
    return utc_now_iso()


def _history_points(rows: Any, *, lookback: int = 180) -> list[MarketDataPoint]:
    if not isinstance(rows, list):
        return []
    parsed: list[tuple[datetime, float]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        value = _safe_float(
            row.get("close")
            or row.get("adjClose")
            or row.get("adj_close")
            or row.get("c")
            or row.get("value")
            or row.get("y")
        )
        if value is None:
            continue
        parsed.append((_parse_iso(_timestamp_value(row)), value))
    parsed.sort(key=lambda item: item[0])
    trimmed = parsed[-lookback:]
    return [MarketDataPoint(at=timestamp.isoformat(), value=value) for timestamp, value in trimmed]


def _movement_points(payload: dict[str, Any]) -> list[MarketDataPoint]:
    current_value = _safe_float(payload.get("value") or payload.get("price") or payload.get("close"))
    previous_close = _safe_float(payload.get("previous_close") or payload.get("previousClose") or payload.get("pc"))
    if current_value is None:
        return []
    observed_at = _parse_iso(str(payload.get("observed_at") or utc_now_iso()))
    if previous_close is not None:
        return [
            MarketDataPoint(at=(observed_at - timedelta(days=1)).isoformat(), value=previous_close),
            MarketDataPoint(at=observed_at.isoformat(), value=current_value),
        ]
    open_value = _safe_float(payload.get("open") or payload.get("o"))
    if open_value is not None:
        return [
            MarketDataPoint(at=(observed_at - timedelta(hours=6)).isoformat(), value=open_value),
            MarketDataPoint(at=observed_at.isoformat(), value=current_value),
        ]
    return [MarketDataPoint(at=observed_at.isoformat(), value=current_value)]


def _derived_change_pct(payload: dict[str, Any]) -> float | None:
    direct = _safe_float(payload.get("change_pct_1d") or payload.get("dp") or payload.get("changePercent"))
    if direct is not None:
        return direct
    current_value = _safe_float(payload.get("value") or payload.get("price") or payload.get("close"))
    previous_close = _safe_float(payload.get("previous_close") or payload.get("previousClose") or payload.get("pc"))
    open_value = _safe_float(payload.get("open") or payload.get("o"))
    try:
        if current_value is not None and previous_close not in {None, 0.0}:
            return ((current_value - previous_close) / abs(previous_close)) * 100.0
        if current_value is not None and open_value not in {None, 0.0}:
            return ((current_value - open_value) / abs(open_value)) * 100.0
    except Exception:
        return None
    return None


def _series_freshness(payload: dict[str, Any]) -> str:
    return str(payload.get("freshness_state") or payload.get("governance", {}).get("freshness_state") or "degraded_monitoring_mode")


def _trust_state(endpoint_family: str) -> str:
    if endpoint_family in {"quote_latest", "ohlcv_history"}:
        return "direct_support"
    if endpoint_family in {"benchmark_proxy", "fx"}:
        return "proxy_support"
    return "bounded_support"


def load_routed_market_series(
    *,
    surface_name: str,
    endpoint_family: str,
    identifier: str,
    label: str | None = None,
    units: str = "price",
    lookback: int = 180,
    allow_live_fetch: bool = True,
) -> tuple[MarketSeriesTruth, dict[str, Any]]:
    cache_surface_name = "blueprint" if surface_name in {"candidate_report", "blueprint_explorer", "compare"} else surface_name
    with _connection() as conn:
        if allow_live_fetch:
            payload = fetch_routed_family(
                conn,
                surface_name=cache_surface_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
                triggered_by_job="chart_support",
                force_refresh=False,
            )
        else:
            payload = _cached_family_payload(
                conn,
                surface_name=cache_surface_name,
                endpoint_family=endpoint_family,
                identifier=identifier,
            )
    payload = _with_provider_execution(payload, endpoint_family=endpoint_family, identifier=identifier)
    points = _history_points(payload.get("series"), lookback=lookback)
    if not points:
        points = _movement_points(payload)
    freshness_state = _series_freshness(payload)
    source_label = str(payload.get("source_name") or payload.get("provider_name") or endpoint_family).strip() or endpoint_family
    truth = MarketSeriesTruth(
        series_id=f"{endpoint_family}:{identifier.lower()}",
        label=label or identifier,
        frequency="daily",
        units=units,
        points=points,
        evidence=[
            EvidencePack(
                evidence_id=f"evidence_chart_{endpoint_family}_{identifier.lower()}",
                thesis=f"{label or identifier} chart support",
                summary=f"Normalized {endpoint_family} series used for V2 chart support.",
                freshness=freshness_state,
                citations=[
                    EvidenceCitation(
                        source_id=str(payload.get("provider_name") or endpoint_family),
                        label=source_label,
                    )
                ],
                facts={
                    "source_family": endpoint_family,
                    "source_label": source_label,
                    "source_provider": payload.get("provider_name"),
                    "freshness_state": freshness_state,
                    "trust_state": _trust_state(endpoint_family),
                    "retrieval_path": payload.get("retrieval_path"),
                    "change_pct_1d": payload.get("change_pct_1d"),
                    "provider_execution": dict(payload.get("provider_execution") or {}),
                    "usable_truth": payload.get("usable_truth"),
                    "sufficiency_state": payload.get("sufficiency_state"),
                    "data_mode": payload.get("data_mode"),
                    "authority_level": payload.get("authority_level"),
                    "truth_envelope": dict(payload.get("truth_envelope") or {}) or None,
                },
                observed_at=points[-1].at if points else utc_now_iso(),
            )
        ],
        as_of=points[-1].at if points else utc_now_iso(),
    )
    return truth, payload


def _series_meta(
    truth: MarketSeriesTruth,
    *,
    source_family: str | None = None,
    source_label: str | None = None,
    freshness_state: str | None = None,
    trust_state: str | None = None,
) -> tuple[str, str, str, str]:
    facts = dict(truth.evidence[0].facts) if truth.evidence else {}
    resolved_family = source_family or str(facts.get("source_family") or "market_truth")
    resolved_label = source_label or str(facts.get("source_label") or truth.label)
    resolved_freshness = freshness_state or str(facts.get("freshness_state") or truth.evidence[0].freshness if truth.evidence else "degraded_monitoring_mode")
    resolved_trust = trust_state or str(facts.get("trust_state") or "bounded_support")
    return resolved_family, resolved_label, resolved_freshness, resolved_trust


def _rebase_points(points: list[ChartPointContract]) -> list[ChartPointContract]:
    if not points:
        return []
    base = points[0].value
    if base in {0.0, -0.0}:
        return points
    return [
        ChartPointContract(timestamp=point.timestamp, value=round((point.value / base) * 100.0, 4))
        for point in points
    ]


def chart_series_from_truth(
    *,
    chart_id: str,
    series_type: ChartSeriesType,
    label: str,
    truth: MarketSeriesTruth,
    unit: str | None = None,
    source_family: str | None = None,
    source_label: str | None = None,
    freshness_state: str | None = None,
    trust_state: str | None = None,
    rebase_to_index: bool = False,
) -> dict[str, Any]:
    resolved_family, resolved_label, resolved_freshness, resolved_trust = _series_meta(
        truth,
        source_family=source_family,
        source_label=source_label,
        freshness_state=freshness_state,
        trust_state=trust_state,
    )
    points = [ChartPointContract(timestamp=point.at, value=round(float(point.value), 6)) for point in truth.points]
    if rebase_to_index:
        points = _rebase_points(points)
    return ChartSeriesContract(
        chart_id=chart_id,
        series_type=series_type,
        label=label,
        points=points,
        unit=unit or ("indexed" if rebase_to_index else truth.units or "value"),
        source_family=resolved_family,
        source_label=resolved_label,
        freshness_state=resolved_freshness,
        trust_state=resolved_trust,
    ).model_dump()


def chart_marker(
    *,
    marker_id: str,
    timestamp: str,
    label: str,
    marker_type: str,
    summary: str,
    linked_object_id: str | None = None,
    linked_surface: str | None = None,
) -> dict[str, Any]:
    return ChartMarkerContract(
        marker_id=marker_id,
        timestamp=timestamp,
        label=label,
        marker_type=marker_type,
        linked_object_id=linked_object_id,
        linked_surface=linked_surface,
        summary=summary,
    ).model_dump()


def chart_threshold(
    *,
    threshold_id: str,
    label: str,
    value: float | None,
    threshold_type: str,
    action_if_crossed: str,
    what_it_means: str,
) -> dict[str, Any] | None:
    if value is None:
        return None
    return ChartThresholdContract(
        threshold_id=threshold_id,
        label=label,
        value=float(value),
        threshold_type=threshold_type,
        action_if_crossed=action_if_crossed,
        what_it_means=what_it_means,
    ).model_dump()


def chart_callout(
    *,
    callout_id: str,
    label: str,
    tone: str,
    detail: str,
) -> dict[str, Any]:
    return ChartCalloutContract(
        callout_id=callout_id,
        label=label,
        tone=tone,
        detail=detail,
    ).model_dump()


def degraded_chart_panel(
    *,
    panel_id: str,
    title: str,
    chart_type: str,
    summary: str,
    what_to_notice: str,
    degraded_state: str,
    freshness_state: str = "degraded_monitoring_mode",
    trust_state: str = "bounded_support",
) -> dict[str, Any]:
    return ChartPanelContract(
        panel_id=panel_id,
        title=title,
        chart_type=chart_type,
        summary=summary,
        what_to_notice=what_to_notice,
        degraded_state=degraded_state,
        freshness_state=freshness_state,
        trust_state=trust_state,
    ).model_dump()


def chart_panel(
    *,
    panel_id: str,
    title: str,
    chart_type: str,
    chart_mode: str = "market_context",
    primary_series: dict[str, Any] | None,
    comparison_series: dict[str, Any] | None = None,
    bands: list[dict[str, Any]] | None = None,
    markers: list[dict[str, Any]] | None = None,
    thresholds: list[dict[str, Any] | None] | None = None,
    callouts: list[dict[str, Any]] | None = None,
    summary: str,
    what_to_notice: str,
    degraded_state: str | None = None,
    freshness_state: str = "fresh_full_rebuild",
    trust_state: str = "direct_support",
    chart_logic: dict[str, Any] | None = None,
) -> dict[str, Any]:
    filtered_thresholds = [threshold for threshold in list(thresholds or []) if threshold]
    return ChartPanelContract(
        panel_id=panel_id,
        title=title,
        chart_type=chart_type,
        chart_mode=chart_mode,
        primary_series=None if primary_series is None else ChartSeriesContract.model_validate(primary_series),
        comparison_series=None if comparison_series is None else ChartSeriesContract.model_validate(comparison_series),
        bands=[ChartBandContract.model_validate(band) for band in list(bands or [])],
        markers=[ChartMarkerContract.model_validate(marker) for marker in list(markers or [])],
        thresholds=[ChartThresholdContract.model_validate(threshold) for threshold in filtered_thresholds],
        callouts=[ChartCalloutContract.model_validate(callout) for callout in list(callouts or [])],
        summary=summary,
        what_to_notice=what_to_notice,
        degraded_state=degraded_state,
        freshness_state=freshness_state,
        trust_state=trust_state,
        chart_logic=ChartLogicContract.model_validate(chart_logic) if chart_logic else None,
    ).model_dump()


def comparison_chart_panel(
    *,
    panel_id: str,
    title: str,
    primary_truth: MarketSeriesTruth,
    comparison_truth: MarketSeriesTruth | None,
    summary: str,
    what_to_notice: str,
    primary_label: str,
    comparison_label: str | None = None,
    source_family: str = "ohlcv_history",
) -> dict[str, Any]:
    primary_series = chart_series_from_truth(
        chart_id=f"{panel_id}_primary",
        series_type="line",
        label=primary_label,
        truth=primary_truth,
        source_family=source_family,
        rebase_to_index=True,
    )
    comparison_series = (
        chart_series_from_truth(
            chart_id=f"{panel_id}_comparison",
            series_type="line",
            label=comparison_label or comparison_truth.label,
            truth=comparison_truth,
            source_family="benchmark_proxy",
            rebase_to_index=True,
        )
        if comparison_truth and comparison_truth.points
        else None
    )
    freshness_state = str(primary_series["freshness_state"])
    trust_state = str(primary_series["trust_state"])
    degraded_state = None if primary_truth.points else "no_series_available"
    if comparison_truth is None or not comparison_truth.points:
        degraded_state = degraded_state or "benchmark_unavailable"
    return chart_panel(
        panel_id=panel_id,
        title=title,
        chart_type="comparison_line",
        chart_mode="comparison",
        primary_series=primary_series,
        comparison_series=comparison_series,
        summary=summary,
        what_to_notice=what_to_notice,
        degraded_state=degraded_state,
        freshness_state=freshness_state,
        trust_state=trust_state,
    )


def snapshot_comparison_panel(
    *,
    panel_id: str,
    title: str,
    rows: list[dict[str, Any]],
    primary_label: str,
    comparison_label: str | None,
    primary_key: str,
    comparison_key: str | None = None,
    unit: str = "percent",
    summary: str,
    what_to_notice: str,
    threshold_values: list[tuple[str, float, str, str]] | None = None,
) -> dict[str, Any]:
    base_date = datetime(2026, 1, 1, tzinfo=UTC)
    primary_points: list[ChartPointContract] = []
    comparison_points: list[ChartPointContract] = []
    markers: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        timestamp = (base_date + timedelta(days=index)).isoformat()
        primary_value = _safe_float(row.get(primary_key))
        comparison_value = _safe_float(row.get(comparison_key)) if comparison_key else None
        if primary_value is not None:
            primary_points.append(ChartPointContract(timestamp=timestamp, value=primary_value))
        if comparison_value is not None:
            comparison_points.append(ChartPointContract(timestamp=timestamp, value=comparison_value))
        markers.append(
            chart_marker(
                marker_id=f"{panel_id}_marker_{index}",
                timestamp=timestamp,
                label=str(row.get("label") or row.get("sleeve_id") or f"Point {index + 1}"),
                marker_type="context",
                summary=str(row.get("note") or row.get("status") or "Snapshot comparison point."),
            )
        )

    if not primary_points:
        return degraded_chart_panel(
            panel_id=panel_id,
            title=title,
            chart_type="snapshot_compare",
            summary=summary,
            what_to_notice=what_to_notice,
            degraded_state="no_series_available",
        )

    primary_series = ChartSeriesContract(
        chart_id=f"{panel_id}_primary",
        series_type="histogram",
        label=primary_label,
        points=primary_points,
        unit=unit,
        source_family="portfolio_snapshot",
        source_label="Portfolio snapshot",
        freshness_state="fresh_full_rebuild",
        trust_state="direct_support",
    ).model_dump()
    comparison_series = (
        ChartSeriesContract(
            chart_id=f"{panel_id}_comparison",
            series_type="line",
            label=comparison_label or "Comparison",
            points=comparison_points,
            unit=unit,
            source_family="portfolio_snapshot",
            source_label="Portfolio snapshot",
            freshness_state="fresh_full_rebuild",
            trust_state="direct_support",
        ).model_dump()
        if comparison_points
        else None
    )
    thresholds = [
        chart_threshold(
            threshold_id=f"{panel_id}_{index}",
            label=label,
            value=value,
            threshold_type=threshold_type,
            action_if_crossed=action_if_crossed,
            what_it_means=what_it_means,
        )
        for index, (label, value, threshold_type, what_it_means) in enumerate(list(threshold_values or []))
        for action_if_crossed in ["Review the affected sleeve before treating the book as back on plan."]
    ]
    return chart_panel(
        panel_id=panel_id,
        title=title,
        chart_type="snapshot_compare",
        chart_mode="decomposition",
        primary_series=primary_series,
        comparison_series=comparison_series,
        markers=markers,
        thresholds=thresholds,
        summary=summary,
        what_to_notice=what_to_notice,
        freshness_state="fresh_full_rebuild",
        trust_state="direct_support",
    )


def _future_timestamps(base_timestamp: str | None, *, count: int, frequency: str) -> list[str]:
    start = _parse_iso(base_timestamp)
    step = timedelta(days=1)
    if str(frequency or "").lower().startswith("week"):
        step = timedelta(days=7)
    elif str(frequency or "").lower().startswith("month"):
        step = timedelta(days=30)
    return [(start + (step * (index + 1))).isoformat() for index in range(count)]


def forecast_panel_from_bundle(
    *,
    panel_id: str,
    title: str,
    bundle: ForecastBundle,
    summary: str,
    what_to_notice: str,
    history_truth: MarketSeriesTruth | None = None,
) -> dict[str, Any]:
    future_timestamps = _future_timestamps(
        bundle.request.timestamps[-1] if bundle.request.timestamps else None,
        count=len(bundle.result.point_path or []),
        frequency=bundle.request.frequency,
    )
    forecast_truth = MarketSeriesTruth(
        series_id=f"forecast:{bundle.request.series_id}",
        label=f"{bundle.request.series_id} forecast",
        frequency=bundle.request.frequency,
        units="price",
        points=[
            MarketDataPoint(at=timestamp, value=float(value))
            for timestamp, value in zip(future_timestamps, bundle.result.point_path, strict=False)
        ],
        evidence=[
            EvidencePack(
                evidence_id=f"evidence_forecast_{bundle.request.request_id}",
                thesis=f"{bundle.request.series_id} forecast path",
                summary="Normalized forecast path for chart support.",
                freshness=bundle.result.freshness_state,
                citations=[
                    EvidenceCitation(
                        source_id=bundle.support.provider,
                        label=bundle.support.model_name,
                    )
                ],
                facts={
                    "source_family": bundle.request.series_family,
                    "source_label": bundle.support.provider,
                    "freshness_state": bundle.result.freshness_state,
                    "trust_state": "weak_support" if bundle.support.degraded_state else "proxy_support",
                },
                observed_at=bundle.result.generated_at,
            )
        ],
        as_of=bundle.result.generated_at,
    )
    primary_series = chart_series_from_truth(
        chart_id=f"{panel_id}_forecast",
        series_type="line",
        label=f"{bundle.support.provider} path",
        truth=forecast_truth,
        source_family=bundle.request.series_family,
        source_label=bundle.support.provider,
        freshness_state=bundle.result.freshness_state,
        trust_state="weak_support" if bundle.support.degraded_state else "proxy_support",
    )
    comparison_series = (
        chart_series_from_truth(
            chart_id=f"{panel_id}_history",
            series_type="area",
            label=history_truth.label,
            truth=history_truth,
            source_family=bundle.request.series_family,
            rebase_to_index=False,
        )
        if history_truth and history_truth.points
        else None
    )
    bands: list[dict[str, Any]] = []
    upper = bundle.result.quantiles.get("0.9")
    lower = bundle.result.quantiles.get("0.1")
    if upper and lower and len(upper) == len(future_timestamps) and len(lower) == len(future_timestamps):
        bands.append(
            ChartBandContract(
                band_id=f"{panel_id}_band",
                label="Forecast band",
                upper_points=[ChartPointContract(timestamp=timestamp, value=float(value)) for timestamp, value in zip(future_timestamps, upper, strict=False)],
                lower_points=[ChartPointContract(timestamp=timestamp, value=float(value)) for timestamp, value in zip(future_timestamps, lower, strict=False)],
                meaning=bundle.result.confidence_band,
                degraded_state=bundle.support.degraded_state,
            ).model_dump()
        )
    markers = [
        chart_marker(
            marker_id=f"{panel_id}_generated",
            timestamp=future_timestamps[0] if future_timestamps else utc_now_iso(),
            label="Forecast starts",
            marker_type="forecast_start",
            summary=f"{bundle.support.provider} generated a {bundle.request.horizon}-step support path.",
            linked_object_id=bundle.request.object_id,
            linked_surface=bundle.request.object_type,
        )
    ]
    thresholds = [
        chart_threshold(
            threshold_id=f"{panel_id}_{index}",
            label=f"{item.trigger_type.replace('_', ' ').title()} threshold",
            value=_safe_float(item.threshold),
            threshold_type=item.trigger_type,
            action_if_crossed=item.next_action_if_hit,
            what_it_means=item.next_action_if_broken,
        )
        for index, item in enumerate(bundle.trigger_support)
    ]
    return chart_panel(
        panel_id=panel_id,
        title=title,
        chart_type="forecast_path",
        chart_mode="forecast",
        primary_series=primary_series,
        comparison_series=comparison_series,
        bands=bands,
        markers=markers,
        thresholds=thresholds,
        summary=summary,
        what_to_notice=what_to_notice,
        degraded_state=bundle.support.degraded_state,
        freshness_state=bundle.result.freshness_state,
        trust_state="weak_support" if bundle.support.degraded_state else "proxy_support",
    )
