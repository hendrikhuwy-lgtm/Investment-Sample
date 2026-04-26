from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel


class AccountEntity(BaseModel):
    account_id: str
    custodian_name: str | None = None
    base_currency: str | None = None
    created_at: datetime
    updated_at: datetime
