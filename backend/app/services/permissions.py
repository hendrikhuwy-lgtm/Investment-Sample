from __future__ import annotations

from typing import Any

from fastapi import HTTPException, Request


PERMISSIONS: dict[str, set[str]] = {
    "portfolio.manage": {"admin", "portfolio_manager"},
    "blueprint.manage": {"admin", "portfolio_manager"},
    "review.manage": {"admin", "portfolio_manager", "reviewer"},
    "audit.export": {"admin", "portfolio_manager", "reviewer"},
    "sla.manage": {"admin", "portfolio_manager"},
    "scenario.manage": {"admin", "portfolio_manager"},
    "policy.manage": {"admin", "portfolio_manager"},
    "account.manage": {"admin", "portfolio_manager"},
    "user.manage": {"admin"},
}


def request_identity(request: Request | None) -> dict[str, Any] | None:
    if request is None:
        return None
    return getattr(request.state, "auth_identity", None)


def require_authenticated(request: Request | None) -> dict[str, Any]:
    identity = request_identity(request)
    if not identity or not identity.get("user"):
        raise HTTPException(status_code=401, detail="Authentication required.")
    return identity


def require_permission(request: Request | None, permission: str) -> dict[str, Any]:
    identity = require_authenticated(request)
    roles = set(identity.get("user", {}).get("roles") or [])
    allowed = PERMISSIONS.get(permission, set())
    if not roles.intersection(allowed):
        raise HTTPException(status_code=403, detail=f"Permission denied for {permission}.")
    return identity
