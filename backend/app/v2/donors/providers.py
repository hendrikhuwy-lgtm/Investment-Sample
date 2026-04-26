from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from app.config import Settings


def _provider_refresh_exports():
    from app.services.provider_refresh import (
        build_cached_external_upstream_payload,
        refresh_blueprint_provider_snapshots,
        refresh_daily_brief_provider_snapshots,
        refresh_dashboard_provider_snapshots,
    )

    return {
        "build_cached_external_upstream_payload": build_cached_external_upstream_payload,
        "refresh_blueprint_provider_snapshots": refresh_blueprint_provider_snapshots,
        "refresh_daily_brief_provider_snapshots": refresh_daily_brief_provider_snapshots,
        "refresh_dashboard_provider_snapshots": refresh_dashboard_provider_snapshots,
    }


@dataclass(slots=True)
class SQLiteProviderDonor:
    """Provider cache/refresh donor wrapper for Layer 1 surfaces."""

    conn: sqlite3.Connection
    settings: Settings

    def build_cached_payload(self, *, surface_name: str | None = None) -> dict[str, object]:
        exports = _provider_refresh_exports()
        return exports["build_cached_external_upstream_payload"](
            self.conn,
            self.settings,
            surface_name=surface_name,
        )

    def refresh_blueprint(self, *, force_refresh: bool = False) -> dict[str, object]:
        exports = _provider_refresh_exports()
        return exports["refresh_blueprint_provider_snapshots"](
            self.conn,
            self.settings,
            force_refresh=force_refresh,
        )

    def refresh_daily_brief(self, *, force_refresh: bool = False) -> dict[str, object]:
        exports = _provider_refresh_exports()
        return exports["refresh_daily_brief_provider_snapshots"](
            self.conn,
            self.settings,
            force_refresh=force_refresh,
        )

    def refresh_dashboard(
        self,
        *,
        account_id: str | None = None,
        force_refresh: bool = False,
    ) -> dict[str, object]:
        exports = _provider_refresh_exports()
        return exports["refresh_dashboard_provider_snapshots"](
            self.conn,
            self.settings,
            account_id=account_id,
            force_refresh=force_refresh,
        )
