from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


DEFAULT_CHANGES_TIMEZONE = "Asia/Singapore"
VALID_WINDOWS = {"today", "3d", "7d"}
WINDOW_DAYS = {
    "3d": 3,
    "7d": 7,
}
VALID_CATEGORIES = {
    "all",
    "requires_review",
    "upgrades",
    "downgrades",
    "blocker_changes",
    "evidence",
    "sleeve",
    "freshness_risk",
    "decision",
    "market_impact",
    "portfolio_drift",
    "source_evidence",
    "blocker",
    "timing",
    "audit_only",
    "system",
}


@dataclass(frozen=True)
class ChangeFeedQuery:
    surface_id: str
    since_utc: str | None = None
    window: str | None = None
    candidate_id: str | None = None
    sleeve_id: str | None = None
    category: str = "all"
    needs_review: bool | None = None
    limit: int | None = None
    cursor: str | None = None
    timezone: str = DEFAULT_CHANGES_TIMEZONE


def normalize_window(value: str | None) -> str | None:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return None
    if normalized not in VALID_WINDOWS:
        raise ValueError(f"Unsupported changes window: {value}")
    return normalized


def normalize_category(value: str | None) -> str:
    normalized = str(value or "all").strip().lower() or "all"
    if normalized not in VALID_CATEGORIES:
        raise ValueError(f"Unsupported changes category: {value}")
    return normalized


def normalize_limit(value: int | None, *, max_limit: int = 100) -> int | None:
    if value is None:
        return None
    if value <= 0:
        return None
    return min(int(value), max_limit)


def normalize_timezone(value: str | None) -> str:
    normalized = str(value or DEFAULT_CHANGES_TIMEZONE).strip() or DEFAULT_CHANGES_TIMEZONE
    try:
        ZoneInfo(normalized)
    except ZoneInfoNotFoundError:
        return DEFAULT_CHANGES_TIMEZONE
    return normalized


def resolve_since_utc(
    *,
    since_utc: str | None,
    window: str | None,
    timezone: str | None = None,
    now: datetime | None = None,
) -> str | None:
    if since_utc:
        return since_utc
    normalized_window = normalize_window(window)
    if normalized_window is None:
        return None

    zone = ZoneInfo(normalize_timezone(timezone))
    current = (now or datetime.now(UTC)).astimezone(UTC)
    if normalized_window == "today":
        local_now = current.astimezone(zone)
        local_start = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        start = local_start.astimezone(UTC)
    else:
        start = current - timedelta(days=WINDOW_DAYS[normalized_window])
    return start.isoformat()


def normalize_query(
    *,
    surface_id: str,
    since_utc: str | None = None,
    window: str | None = None,
    candidate_id: str | None = None,
    sleeve_id: str | None = None,
    category: str | None = None,
    needs_review: bool | None = None,
    limit: int | None = None,
    cursor: str | None = None,
    timezone: str | None = None,
) -> ChangeFeedQuery:
    normalized_window = normalize_window(window)
    normalized_timezone = normalize_timezone(timezone)
    effective_since_utc = resolve_since_utc(since_utc=since_utc, window=normalized_window, timezone=normalized_timezone)
    return ChangeFeedQuery(
        surface_id=str(surface_id or "").strip(),
        since_utc=effective_since_utc,
        window=normalized_window,
        candidate_id=str(candidate_id or "").strip() or None,
        sleeve_id=str(sleeve_id or "").strip() or None,
        category=normalize_category(category),
        needs_review=needs_review,
        limit=normalize_limit(limit),
        cursor=str(cursor or "").strip() or None,
        timezone=normalized_timezone,
    )
