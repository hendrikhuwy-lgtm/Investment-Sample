from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Iterable
from zoneinfo import ZoneInfo


CHINA_TZ = ZoneInfo("Asia/Shanghai")
DEFAULT_SLOT_HOURS = (8, 20)
_SLOT_NAME_BY_HOUR = {
    8: "morning",
    20: "evening",
}


def _normalize_hour(value: Any, default: int) -> int:
    try:
        return min(23, max(0, int(value)))
    except Exception:
        return default


def configured_slot_hours(primary: Any = 8, secondary: Any = 20) -> tuple[int, ...]:
    normalized = {
        _normalize_hour(primary, DEFAULT_SLOT_HOURS[0]),
        _normalize_hour(secondary, DEFAULT_SLOT_HOURS[1]),
    }
    return tuple(sorted(normalized)) or DEFAULT_SLOT_HOURS


def settings_slot_hours(settings: Any) -> tuple[int, ...]:
    return configured_slot_hours(
        getattr(settings, "auto_daily_brief_run_hour_china", DEFAULT_SLOT_HOURS[0]),
        getattr(settings, "auto_daily_brief_second_run_hour_china", DEFAULT_SLOT_HOURS[1]),
    )


def _ensure_utc(value: datetime | None = None) -> datetime:
    current = value or datetime.now(UTC)
    if current.tzinfo is None:
        return current.replace(tzinfo=UTC)
    return current.astimezone(UTC)


def current_slot_start(now: datetime | None = None, *, hours: Iterable[int] = DEFAULT_SLOT_HOURS) -> datetime:
    current = _ensure_utc(now)
    local = current.astimezone(CHINA_TZ)
    normalized_hours = tuple(sorted({int(hour) for hour in hours})) or DEFAULT_SLOT_HOURS
    for hour in reversed(normalized_hours):
        candidate = local.replace(hour=hour, minute=0, second=0, microsecond=0)
        if local >= candidate:
            return candidate.astimezone(UTC)
    previous_day = local - timedelta(days=1)
    return previous_day.replace(hour=normalized_hours[-1], minute=0, second=0, microsecond=0).astimezone(UTC)


def next_slot_start(now: datetime | None = None, *, hours: Iterable[int] = DEFAULT_SLOT_HOURS) -> datetime:
    current = _ensure_utc(now)
    local = current.astimezone(CHINA_TZ)
    normalized_hours = tuple(sorted({int(hour) for hour in hours})) or DEFAULT_SLOT_HOURS
    for hour in normalized_hours:
        candidate = local.replace(hour=hour, minute=0, second=0, microsecond=0)
        if candidate > local:
            return candidate.astimezone(UTC)
    next_day = local + timedelta(days=1)
    return next_day.replace(hour=normalized_hours[0], minute=0, second=0, microsecond=0).astimezone(UTC)


def slot_interval_seconds(hours: Iterable[int] = DEFAULT_SLOT_HOURS) -> int:
    normalized_hours = tuple(sorted({int(hour) for hour in hours})) or DEFAULT_SLOT_HOURS
    if len(normalized_hours) == 1:
        return 24 * 60 * 60
    deltas: list[int] = []
    for index, hour in enumerate(normalized_hours):
        next_hour = normalized_hours[(index + 1) % len(normalized_hours)]
        delta = (next_hour - hour) % 24
        deltas.append(delta or 24)
    return min(deltas) * 60 * 60


def current_slot_info(now: datetime | None = None, *, hours: Iterable[int] = DEFAULT_SLOT_HOURS) -> dict[str, Any]:
    normalized_hours = tuple(sorted({int(hour) for hour in hours})) or DEFAULT_SLOT_HOURS
    started_at = current_slot_start(now, hours=normalized_hours)
    ends_at = next_slot_start(started_at + timedelta(seconds=1), hours=normalized_hours)
    local_start = started_at.astimezone(CHINA_TZ)
    slot_hour = local_start.hour
    slot_name = _SLOT_NAME_BY_HOUR.get(slot_hour, f"slot_{slot_hour:02d}00")
    slot_date = local_start.date().isoformat()
    slot_key = f"{slot_date}@{slot_hour:02d}:00"
    return {
        "slot_key": slot_key,
        "slot_name": slot_name,
        "slot_hour_china": slot_hour,
        "slot_date_china": slot_date,
        "slot_label": f"{slot_name.capitalize()} brief ({slot_hour:02d}:00 China)",
        "slot_started_at": started_at.isoformat(),
        "slot_ends_at": ends_at.isoformat(),
    }
