from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class SecurityClassification(BaseModel):
    classification_id: str
    run_id: str
    security_key: str
    normalized_symbol: str
    issuer_key: str | None = None
    country: str | None = None
    region: str | None = None
    sector: str | None = None
    industry: str | None = None
    classification_source: str
    confidence: str
    provenance_json: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class FactorExposureSnapshot(BaseModel):
    factor_snapshot_id: str
    run_id: str
    factor_name: str
    exposure_value: float
    exposure_type: str
    confidence: str
    provenance_json: dict[str, Any]
    created_at: datetime
