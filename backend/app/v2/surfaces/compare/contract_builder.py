from __future__ import annotations

from app.v2.surfaces.blueprint.compare_contract_builder import build as build_blueprint_compare


def build(candidate_a: str, candidate_b: str) -> dict[str, object]:
    """Compatibility wrapper for legacy compare callers.

    Compare is now owned by the Blueprint candidate workflow. The old route shape
    stays available so existing callers do not break while the frontend moves to
    the newer ids-based compare contract.
    """

    return build_blueprint_compare([candidate_a, candidate_b])
