from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class LimitProfile(BaseModel):
    limit_id: str
    blueprint_id: str | None = None
    strategy_id: str | None = None
    limit_type: str
    scope: str
    threshold_value: float
    warning_threshold: float | None = None
    breach_severity: str = "medium"
    enabled: bool = True
    effective_from: datetime | None = None
    effective_to: datetime | None = None
    created_at: datetime
    updated_at: datetime


class ExposureSnapshotRow(BaseModel):
    exposure_id: str
    run_id: str
    snapshot_id: str | None = None
    exposure_type: str
    scope_key: str
    label: str
    market_value: float = 0.0
    weight: float = 0.0
    metadata: dict[str, Any] = {}
    created_at: datetime


class LimitBreach(BaseModel):
    breach_id: str
    limit_id: str
    snapshot_id: str | None = None
    run_id: str | None = None
    scope_key: str | None = None
    label: str | None = None
    current_value: float
    threshold_value: float
    warning_threshold: float | None = None
    severity: str
    breach_status: str
    first_detected_at: datetime
    last_detected_at: datetime
    resolved_at: datetime | None = None
    linked_review_id: str | None = None
