from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel


class AuditEvent(BaseModel):
    audit_event_id: str
    actor: str
    action_type: str
    object_type: str
    object_id: str | None = None
    before_json: dict[str, Any] | None = None
    after_json: dict[str, Any] | None = None
    source_ip: str | None = None
    user_agent: str | None = None
    occurred_at: datetime


class AuditExport(BaseModel):
    export_id: str
    export_scope: str
    generated_at: datetime
    generated_by: str
    filters_json: dict[str, Any]
    file_path: str
