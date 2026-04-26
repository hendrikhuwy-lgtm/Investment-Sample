from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo


CHINA_TZ = ZoneInfo("Asia/Shanghai")
LAG_CLASSES = {"fresh", "lagged", "stale"}
LAG_CAUSES = {"expected_publication_lag", "unexpected_ingestion_lag", "unknown"}


@dataclass(frozen=True)
class SeriesUpdatePolicy:
    expected_update_days: tuple[int, ...]
    expected_max_lag_days: int = 3


def _weekday_set(days: tuple[int, ...] | list[int] | set[int]) -> tuple[int, ...]:
    cleaned = sorted({int(day) for day in days if int(day) in {0, 1, 2, 3, 4, 5, 6}})
    if not cleaned:
        return (0, 1, 2, 3, 4)
    return tuple(cleaned)


SERIES_UPDATE_POLICY: dict[str, SeriesUpdatePolicy] = {
    "DGS10": SeriesUpdatePolicy(expected_update_days=_weekday_set((0, 1, 2, 3, 4)), expected_max_lag_days=4),
    "T10YIE": SeriesUpdatePolicy(expected_update_days=_weekday_set((0, 1, 2, 3, 4)), expected_max_lag_days=4),
    "SP500": SeriesUpdatePolicy(expected_update_days=_weekday_set((0, 1, 2, 3, 4)), expected_max_lag_days=4),
    "VIXCLS": SeriesUpdatePolicy(expected_update_days=_weekday_set((0, 1, 2, 3, 4)), expected_max_lag_days=4),
    "BAMLH0A0HYM2": SeriesUpdatePolicy(expected_update_days=_weekday_set((0, 1, 2, 3, 4)), expected_max_lag_days=4),
    "STOOQ_SPY_VOLUME": SeriesUpdatePolicy(expected_update_days=_weekday_set((0, 1, 2, 3, 4)), expected_max_lag_days=3),
    "STOOQ_QQQ_VOLUME": SeriesUpdatePolicy(expected_update_days=_weekday_set((0, 1, 2, 3, 4)), expected_max_lag_days=3),
    "YAHOO_GSPC_VOLUME": SeriesUpdatePolicy(expected_update_days=_weekday_set((0, 1, 2, 3, 4)), expected_max_lag_days=3),
    "YAHOO_SPY_VOLUME": SeriesUpdatePolicy(expected_update_days=_weekday_set((0, 1, 2, 3, 4)), expected_max_lag_days=3),
}

DEFAULT_KEY_METRICS: tuple[str, ...] = (
    "DGS10",
    "T10YIE",
    "SP500",
    "VIXCLS",
    "BAMLH0A0HYM2",
)


def normalize_series_key(value: str | None) -> str:
    raw = str(value or "").strip().upper()
    if not raw:
        return ""
    alias_map = {
        "HY OAS": "BAMLH0A0HYM2",
        "HY_OAS": "BAMLH0A0HYM2",
        "BAMLH0A0HYM2": "BAMLH0A0HYM2",
    }
    return alias_map.get(raw, raw)


def _parse_observation_date(observed_at: str | None) -> datetime.date | None:
    value = str(observed_at or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def parse_datetime(value: str | datetime | None) -> datetime | None:
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value or "").strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def compute_lag_days(
    observation_date: str | None,
    retrieved_at: str | datetime | None,
    timezone: str = "Asia/Shanghai",
) -> tuple[int | None, str | None]:
    observed = _parse_observation_date(observation_date)
    retrieved = parse_datetime(retrieved_at)
    if observed is None or retrieved is None:
        return None, None

    try:
        tz = ZoneInfo(timezone)
    except Exception:
        tz = CHINA_TZ
    local_retrieved_date = retrieved.astimezone(tz).date()
    lag_days = max(0, (local_retrieved_date - observed).days)
    if lag_days <= 1:
        return lag_days, "fresh"
    if lag_days <= 4:
        return lag_days, "lagged"
    return lag_days, "stale"


def classify_lag_cause(
    *,
    series_key: str,
    observed_at: str | None,
    retrieved_at: str | datetime | None,
    lag_days: int | None,
    retrieval_succeeded: bool,
    cache_fallback_used: bool,
    latest_available_matches_observed: bool,
    previous_observed_at: str | None = None,
    policy_map: dict[str, SeriesUpdatePolicy] | None = None,
) -> str:
    key = normalize_series_key(series_key)
    policies = policy_map or SERIES_UPDATE_POLICY
    policy = policies.get(key, SeriesUpdatePolicy(expected_update_days=(0, 1, 2, 3, 4), expected_max_lag_days=3))
    retrieved = parse_datetime(retrieved_at)

    if (not retrieval_succeeded) and cache_fallback_used:
        return "unexpected_ingestion_lag"
    if lag_days is None:
        return "unknown"
    if lag_days > int(policy.expected_max_lag_days):
        return "unexpected_ingestion_lag"

    if retrieved is not None:
        local_weekday = retrieved.astimezone(CHINA_TZ).weekday()
        stagnated = (
            bool(previous_observed_at)
            and str(previous_observed_at).strip()
            and str(previous_observed_at).strip() == str(observed_at or "").strip()
        )
        if stagnated and local_weekday in set(policy.expected_update_days) and lag_days > int(policy.expected_max_lag_days):
            return "unexpected_ingestion_lag"

    if lag_days <= int(policy.expected_max_lag_days) and retrieval_succeeded and latest_available_matches_observed:
        return "expected_publication_lag"
    return "unknown"


def build_data_trust_badge(
    metric_rows: list[dict[str, Any]],
    key_metrics: tuple[str, ...] = DEFAULT_KEY_METRICS,
) -> dict[str, Any]:
    by_key = {normalize_series_key(str(item.get("metric_key") or "")): item for item in metric_rows}
    missing = [key for key in key_metrics if key not in by_key]

    unexpected = 0
    stale = 0
    unknown = 0
    for key in key_metrics:
        item = by_key.get(key)
        if not item:
            continue
        lag_cause = str(item.get("lag_cause") or "unknown")
        lag_days = item.get("lag_days")
        lag_class = str(item.get("lag_class") or "")
        if lag_cause == "unexpected_ingestion_lag":
            unexpected += 1
        elif lag_cause == "unknown":
            unknown += 1
        if lag_class == "stale":
            stale += 1
        elif lag_days is not None:
            try:
                if int(lag_days) >= 5:
                    stale += 1
            except Exception:
                pass

    if stale > 0 or unexpected > 2:
        level = "low"
    elif unexpected in {1, 2} or unknown > 0 or bool(missing):
        level = "medium"
    else:
        level = "high"

    summary = (
        f"{unexpected} unexpected lag metric(s), "
        f"{stale} stale key metric(s), "
        f"{len(missing)} missing key metric(s)."
    )
    return {
        "level": level,
        "label": f"Data trust: {level}",
        "summary": summary,
        "unexpected_count": unexpected,
        "stale_key_metrics_count": stale,
        "missing_key_metrics_count": len(missing),
        "missing_key_metrics": missing,
    }
