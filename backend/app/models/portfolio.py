from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class PortfolioUploadSummary(BaseModel):
    run_id: str
    filename: str | None = None
    source_name: str | None = None
    uploaded_at: datetime
    holdings_as_of_date: str
    status: Literal["ok", "partial", "failed", "deleted"] | str
    is_active: bool = False
    is_deleted: bool = False
    snapshot_id: str | None = None
    raw_row_count: int = 0
    parsed_row_count: int = 0
    normalized_position_count: int = 0
    total_market_value: float = 0.0
    stale_price_count: int = 0
    mapping_issue_count: int = 0
    warning_count: int = 0


class PortfolioUploadDetail(PortfolioUploadSummary):
    deleted_at: datetime | None = None
    deleted_reason: str | None = None
    warnings: list[str] = []
    errors: list[str] = []
