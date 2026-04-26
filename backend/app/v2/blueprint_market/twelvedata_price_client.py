from __future__ import annotations

import sqlite3
from typing import Any

from app.config import get_db_path
from app.services.provider_adapters import ProviderAdapterError, fetch_provider_data
from app.services.symbol_resolution import (
    record_resolution_failure,
    record_resolution_success,
    resolve_provider_identifiers,
)


class TwelveDataPriceClient:
    provider_name = "twelve_data"

    def _connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(get_db_path())
        conn.row_factory = sqlite3.Row
        return conn

    def fetch_daily_ohlcv(self, provider_symbol: str) -> dict[str, Any]:
        normalized_symbol = str(provider_symbol or "").strip().upper()
        with self._connection() as conn:
            resolution = resolve_provider_identifiers(
                conn,
                provider_name=self.provider_name,
                endpoint_family="ohlcv_history",
                identifier=normalized_symbol,
            )
            attempts: list[str] = []
            ordered_candidates = [
                str(resolution.get("provider_symbol") or normalized_symbol).strip().upper(),
                *[str(item).strip().upper() for item in list(resolution.get("fallback_aliases") or [])],
            ]
            candidates: list[str] = []
            for candidate in ordered_candidates:
                if candidate and candidate not in attempts:
                    attempts.append(candidate)
                    candidates.append(candidate)
            last_error: ProviderAdapterError | None = None
            for candidate in candidates[:4]:
                try:
                    payload = dict(fetch_provider_data(self.provider_name, "ohlcv_history", candidate) or {})
                    record_resolution_success(
                        conn,
                        canonical_symbol=normalized_symbol,
                        provider_name=self.provider_name,
                        endpoint_family="ohlcv_history",
                        provider_symbol=candidate,
                        fallback_aliases=[item for item in candidates if item != candidate],
                        resolution_confidence=float(resolution.get("resolution_confidence") or 0.75),
                        resolution_reason="blueprint_market_verified_runtime",
                    )
                    payload.setdefault("provider_name", self.provider_name)
                    payload.setdefault("provider_symbol", candidate)
                    payload.setdefault("requested_provider_symbol", normalized_symbol)
                    payload.setdefault("interval", "1day")
                    return payload
                except ProviderAdapterError as exc:
                    last_error = exc
                    record_resolution_failure(
                        conn,
                        canonical_symbol=normalized_symbol,
                        provider_name=self.provider_name,
                        endpoint_family="ohlcv_history",
                        provider_symbol=candidate,
                        error_class=str(exc.error_class or "provider_error"),
                    )
                    continue
        if last_error is not None:
            raise last_error
        raise ProviderAdapterError(self.provider_name, "ohlcv_history", "No usable Twelve Data series was returned", error_class="empty_response")
