from __future__ import annotations

"""Thin Blueprint orchestration entry point.

This module is intentionally kept small. Canonical meaning is owned by the
pipeline and canonical decision modules; payload assembly lives in
`blueprint_payload_assembler`.
"""

from app.services.blueprint_payload_assembler import (
    BLUEPRINT_PAYLOAD_INTEGRITY_VERSION,
    _candidate_truth_state,
    build_portfolio_blueprint_payload,
)

__all__ = [
    "BLUEPRINT_PAYLOAD_INTEGRITY_VERSION",
    "_candidate_truth_state",
    "build_portfolio_blueprint_payload",
]
