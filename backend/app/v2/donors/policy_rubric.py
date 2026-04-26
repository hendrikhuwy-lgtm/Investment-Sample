from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.v2.core.domain_objects import PolicyBoundary


def get_policy_boundary(sleeve_id: str) -> "PolicyBoundary":
    """Returns PolicyBoundary. Wraps policy_authority + framework lenses donors."""
    raise NotImplementedError("Donor interface not yet implemented")
