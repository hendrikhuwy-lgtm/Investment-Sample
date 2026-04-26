from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class AuthUser(BaseModel):
    user_id: str
    username: str
    display_name: str
    email: str | None = None
    status: str
    created_at: datetime
    last_active_at: datetime | None = None
    roles: list[str] = []


class AuthSession(BaseModel):
    session_id: str
    user_id: str
    issued_at: datetime
    expires_at: datetime
    revoked_at: datetime | None = None

