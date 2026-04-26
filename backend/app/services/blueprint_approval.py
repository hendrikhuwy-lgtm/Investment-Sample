"""Compatibility stub — blueprint_approval module was removed; stubs preserve import-ability."""
from __future__ import annotations

from typing import Any


def check_policy_escalation_allowed(
    conn: Any,
    *,
    entity_id: str = "",
    sleeve_key: str = "",
    change_type: str = "",
    directness_class: str = "",
    authority_class: str = "",
    policy_action_class: str = "",
    policy_restriction_codes: list[str] | None = None,
    requires_approval: bool = False,
) -> dict[str, Any]:
    """Stub — escalation not available; returns policy_escalation_allowed=False."""
    return {
        "policy_escalation_allowed": False,
        "reason": "policy_authority_unavailable",
    }
