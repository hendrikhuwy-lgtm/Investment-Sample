from datetime import UTC, datetime

import pytest

from app.v2.surfaces.changes.query import normalize_window, resolve_since_utc


def test_today_window_uses_requested_timezone_midnight() -> None:
    since = resolve_since_utc(
        since_utc=None,
        window="today",
        timezone="Asia/Singapore",
        now=datetime(2026, 4, 24, 18, 30, tzinfo=UTC),
    )

    assert since == "2026-04-24T16:00:00+00:00"


def test_three_day_window_is_supported() -> None:
    since = resolve_since_utc(
        since_utc=None,
        window="3d",
        timezone="Asia/Singapore",
        now=datetime(2026, 4, 24, 18, 30, tzinfo=UTC),
    )

    assert since == "2026-04-21T18:30:00+00:00"
    assert normalize_window("3d") == "3d"


def test_thirty_day_window_is_not_supported() -> None:
    with pytest.raises(ValueError):
        normalize_window("30d")
