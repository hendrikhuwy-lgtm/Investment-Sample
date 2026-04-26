from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class ScenarioRegistryRecord(BaseModel):
    scenario_id: str
    scenario_name: str
    status: str
    source_rationale: str | None = None
    policy_notes: str | None = None
    created_at: datetime
    approved_at: datetime | None = None
    retired_at: datetime | None = None


class ScenarioVersionRecord(BaseModel):
    scenario_version_id: str
    scenario_id: str
    version_label: str
    is_active: bool
    probability_weight: float | None = None
    confidence_rating: str
    review_cadence_days: int | None = None
    last_reviewed_at: datetime | None = None
    reviewed_by: str | None = None
    shocks_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime
