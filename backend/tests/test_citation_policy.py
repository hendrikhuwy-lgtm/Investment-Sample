from datetime import UTC, datetime

import pytest

from app.models.types import Citation, InsightRecord
from app.services.normalize import CitationPolicyError, validate_citations


def test_actionable_insight_requires_multiple_sources_or_primary() -> None:
    insight = InsightRecord(
        insight_id="x",
        theme="market",
        summary="Test insight with insufficient source diversity for actionable use.",
        stance="neutral",
        confidence=0.5,
        time_horizon="short",
        citations=[
            Citation(
                url="https://example.com/a",
                source_id="same",
                retrieved_at=datetime.now(UTC),
                importance="secondary analysis",
            )
        ],
    )

    with pytest.raises(CitationPolicyError):
        validate_citations(insight, actionable=True)


def test_actionable_insight_passes_with_two_sources() -> None:
    insight = InsightRecord(
        insight_id="y",
        theme="market",
        summary="Actionable insight with two independent sources.",
        stance="neutral",
        confidence=0.5,
        time_horizon="short",
        citations=[
            Citation(
                url="https://example.com/a",
                source_id="a",
                retrieved_at=datetime.now(UTC),
                importance="secondary",
            ),
            Citation(
                url="https://example.com/b",
                source_id="b",
                retrieved_at=datetime.now(UTC),
                importance="secondary",
            ),
        ],
    )

    validate_citations(insight, actionable=True)
