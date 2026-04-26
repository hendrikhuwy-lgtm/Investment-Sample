from __future__ import annotations

import sqlite3
from datetime import UTC, datetime, timedelta


def test_daily_brief_market_close_cache_expires_on_next_china_slot(monkeypatch) -> None:
    from app.services.provider_cache import get_cached_provider_snapshot, put_provider_snapshot

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    fixed_now = datetime(2026, 4, 8, 1, 30, tzinfo=UTC)  # 09:30 CST
    monkeypatch.setattr("app.services.provider_cache._now", lambda: fixed_now)

    put_provider_snapshot(
        conn,
        provider_name="yahoo_finance",
        endpoint_family="market_close",
        cache_key="DAILY_BRIEF:MARKET_CLOSE:^GSPC",
        payload={"identifier": "^GSPC", "price": 6611.83, "observed_at": "2026-04-07T00:00:00+00:00"},
        surface_name="daily_brief",
        freshness_state="fresh",
        confidence_tier="support",
        source_ref="https://finance.yahoo.com/quote/%5EGSPC",
        ttl_seconds=43200,
    )

    row = conn.execute(
        """
        SELECT fetched_at, expires_at
        FROM provider_cache_snapshots
        WHERE provider_name = 'yahoo_finance' AND endpoint_family = 'market_close'
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    assert row["fetched_at"] == "2026-04-08T01:30:00+00:00"
    assert row["expires_at"] == "2026-04-08T12:00:00+00:00"

    cached = get_cached_provider_snapshot(
        conn,
        provider_name="yahoo_finance",
        endpoint_family="market_close",
        cache_key="DAILY_BRIEF:MARKET_CLOSE:^GSPC",
        surface_name="daily_brief",
    )
    assert cached is not None

    monkeypatch.setattr("app.services.provider_cache._now", lambda: datetime(2026, 4, 8, 12, 0, 1, tzinfo=UTC))
    assert get_cached_provider_snapshot(
        conn,
        provider_name="yahoo_finance",
        endpoint_family="market_close",
        cache_key="DAILY_BRIEF:MARKET_CLOSE:^GSPC",
        surface_name="daily_brief",
    ) is None
    assert get_cached_provider_snapshot(
        conn,
        provider_name="yahoo_finance",
        endpoint_family="market_close",
        cache_key="DAILY_BRIEF:MARKET_CLOSE:^GSPC",
        surface_name="daily_brief",
        allow_expired=True,
    ) is not None


def test_non_slot_family_keeps_rolling_ttl(monkeypatch) -> None:
    from app.services.provider_cache import put_provider_snapshot

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    fixed_now = datetime(2026, 4, 8, 1, 30, tzinfo=UTC)
    monkeypatch.setattr("app.services.provider_cache._now", lambda: fixed_now)

    put_provider_snapshot(
        conn,
        provider_name="yahoo_finance",
        endpoint_family="quote_latest",
        cache_key="DAILY_BRIEF:QUOTE_LATEST:SPY",
        payload={"identifier": "SPY", "price": 658.21, "observed_at": "2026-04-08T01:30:00+00:00"},
        surface_name="daily_brief",
        freshness_state="fresh",
        confidence_tier="support",
        source_ref="https://finance.yahoo.com/quote/SPY",
        ttl_seconds=900,
    )

    row = conn.execute(
        """
        SELECT expires_at
        FROM provider_cache_snapshots
        WHERE provider_name = 'yahoo_finance' AND endpoint_family = 'quote_latest'
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    assert row["expires_at"] == "2026-04-08T01:45:00+00:00"


def test_daily_brief_surface_rebuilds_immediately_after_slot_boundary(monkeypatch) -> None:
    from app.v2 import router

    now = datetime.now(UTC)
    stored_at = now - timedelta(minutes=1)
    slot_boundary = now - timedelta(seconds=30)

    monkeypatch.setattr(router, "_mem_get_any", lambda key: (stored_at, {"cached": True}))
    monkeypatch.setattr(router, "_db_get_any", lambda surface_id, object_id: None)
    monkeypatch.setattr(router, "_surface_slot_boundary", lambda surface_id, *, now: slot_boundary)
    monkeypatch.setattr(router, "_fire_rebuild", lambda key, fn: None)

    rebuilt = {"count": 0}

    def _rebuild() -> dict[str, object]:
        rebuilt["count"] += 1
        return {"rebuilt": True}

    payload = router._serve_cached(
        "daily_brief",
        "daily_brief",
        "daily_brief",
        max_seconds=600,
        rebuild_fn=_rebuild,
    )

    assert payload == {"rebuilt": True}
    assert rebuilt["count"] == 1
