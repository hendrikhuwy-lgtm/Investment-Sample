from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine, select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import DeclarativeBase, Session

from app.config import get_db_path, get_repo_root
from app.v2.core.domain_objects import ChangeEvent, ChangeEventRow, SurfaceChangeSummary


logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class ChangeEventRecord(Base):
    __tablename__ = "v2_change_events"

    event_id = Column(String, primary_key=True)
    event_type = Column(String, nullable=False)
    surface_id = Column(String, nullable=False)
    changed_at_utc = Column(DateTime, nullable=False)
    summary = Column(String, nullable=False)
    candidate_id = Column(String, nullable=True)
    sleeve_id = Column(String, nullable=True)
    sleeve_name = Column(String, nullable=True)
    change_trigger = Column(Text, nullable=True)
    reason_summary = Column(Text, nullable=True)
    previous_state = Column(String, nullable=True)
    current_state = Column(String, nullable=True)
    implication_summary = Column(Text, nullable=True)
    portfolio_consequence = Column(Text, nullable=True)
    next_action = Column(Text, nullable=True)
    what_would_reverse = Column(Text, nullable=True)
    requires_review = Column(Integer, nullable=False, default=0)
    report_tab = Column(String, nullable=True)
    impact_level = Column(String, nullable=True)
    deep_link_target_json = Column(Text, nullable=True)


def _parse_since_utc(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        logger.warning("Ignoring invalid since_utc filter for change ledger: %s", value)
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _ledger_path() -> Path:
    env_db_path = os.getenv("IA_DB_PATH", "").strip()
    if env_db_path:
        return get_db_path()

    primary_path = get_db_path()
    if primary_path.exists():
        return primary_path

    fallback = get_repo_root() / "storage" / "v2_changes.sqlite3"
    fallback.parent.mkdir(parents=True, exist_ok=True)
    return fallback.resolve()


@lru_cache(maxsize=None)
def _engine_for_url(database_url: str):
    return create_engine(database_url, future=True)


def _engine():
    return _engine_for_url(f"sqlite:///{_ledger_path()}")


def _ensure_schema() -> None:
    Base.metadata.create_all(_engine())
    required_columns = {
        "candidate_id": "TEXT",
        "sleeve_id": "TEXT",
        "sleeve_name": "TEXT",
        "change_trigger": "TEXT",
        "reason_summary": "TEXT",
        "previous_state": "TEXT",
        "current_state": "TEXT",
        "implication_summary": "TEXT",
        "portfolio_consequence": "TEXT",
        "next_action": "TEXT",
        "what_would_reverse": "TEXT",
        "requires_review": "INTEGER NOT NULL DEFAULT 0",
        "report_tab": "TEXT",
        "impact_level": "TEXT",
        "deep_link_target_json": "TEXT",
    }
    with _engine().begin() as connection:
        table_info = connection.exec_driver_sql("PRAGMA table_info(v2_change_events)").fetchall()
        existing_columns = {str(row[1]) for row in table_info}
        for column_name, ddl in required_columns.items():
            if column_name in existing_columns:
                continue
            connection.exec_driver_sql(f"ALTER TABLE v2_change_events ADD COLUMN {column_name} {ddl}")


def _parse_json(value: str | None) -> dict[str, object] | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _row_to_event(row: ChangeEventRecord) -> ChangeEventRow:
    changed_at = row.changed_at_utc
    if changed_at.tzinfo is None:
        changed_at = changed_at.replace(tzinfo=UTC)
    else:
        changed_at = changed_at.astimezone(UTC)
    return ChangeEventRow(
        event_id=row.event_id,
        event_type=row.event_type,
        summary=row.summary,
        changed_at_utc=changed_at.isoformat(),
        candidate_id=row.candidate_id,
        sleeve_id=row.sleeve_id,
        sleeve_name=row.sleeve_name,
        change_trigger=row.change_trigger,
        reason_summary=row.reason_summary,
        previous_state=row.previous_state,
        current_state=row.current_state,
        implication_summary=row.implication_summary,
        portfolio_consequence=row.portfolio_consequence,
        next_action=row.next_action,
        what_would_reverse=row.what_would_reverse,
        requires_review=bool(row.requires_review),
        report_tab=row.report_tab,
        impact_level=row.impact_level,
        deep_link_target=_parse_json(row.deep_link_target_json),
    )


def record_change(
    event_type: str,
    surface_id: str,
    summary: str,
    *,
    candidate_id: str | None = None,
    sleeve_id: str | None = None,
    sleeve_name: str | None = None,
    change_trigger: str | None = None,
    reason_summary: str | None = None,
    previous_state: str | None = None,
    current_state: str | None = None,
    implication_summary: str | None = None,
    portfolio_consequence: str | None = None,
    next_action: str | None = None,
    what_would_reverse: str | None = None,
    requires_review: bool | None = None,
    report_tab: str | None = None,
    impact_level: str | None = None,
    deep_link_target: dict[str, object] | None = None,
) -> str:
    """Insert a change event and return its event id."""
    event_id = f"change_{uuid.uuid4().hex}"
    review_flag = requires_review
    if review_flag is None:
        review_flag = str(impact_level or "").strip().lower() in {"high", "medium"}
    record = ChangeEventRecord(
        event_id=event_id,
        event_type=event_type,
        surface_id=surface_id,
        changed_at_utc=datetime.now(UTC),
        summary=summary,
        candidate_id=candidate_id,
        sleeve_id=sleeve_id,
        sleeve_name=sleeve_name,
        change_trigger=change_trigger,
        reason_summary=reason_summary,
        previous_state=previous_state,
        current_state=current_state,
        implication_summary=implication_summary,
        portfolio_consequence=portfolio_consequence,
        next_action=next_action,
        what_would_reverse=what_would_reverse,
        requires_review=1 if review_flag else 0,
        report_tab=report_tab,
        impact_level=impact_level,
        deep_link_target_json=json.dumps(deep_link_target, ensure_ascii=True) if deep_link_target else None,
    )
    try:
        _ensure_schema()
        with Session(_engine()) as session:
            session.add(record)
            session.commit()
    except SQLAlchemyError:
        logger.exception("Failed to record change event for surface_id=%s", surface_id)
    return event_id


def get_diffs(surface_id: str, since_utc: str | None = None) -> list[ChangeEventRow]:
    """Return persisted change events for a surface, ordered newest-first."""
    try:
        _ensure_schema()
        stmt = (
            select(ChangeEventRecord)
            .where(ChangeEventRecord.surface_id == surface_id)
            .order_by(ChangeEventRecord.changed_at_utc.desc(), ChangeEventRecord.event_id.desc())
        )
        parsed_since = _parse_since_utc(since_utc)
        if parsed_since is not None:
            stmt = stmt.where(ChangeEventRecord.changed_at_utc >= parsed_since)
        with Session(_engine()) as session:
            rows = session.scalars(stmt).all()
        return [_row_to_event(row) for row in rows]
    except SQLAlchemyError:
        logger.exception("Failed to fetch change events for surface_id=%s", surface_id)
        return []


class ChangeLedger:
    """Compatibility wrapper for the current demo router until M6 persistence lands."""

    def __init__(self) -> None:
        self._events: list[ChangeEvent] = []

    def record(self, event: ChangeEvent) -> ChangeEvent:
        self._events.append(event)
        return event

    def list_events(self, *, surface_id: str | None = None) -> list[ChangeEvent]:
        if surface_id is None:
            return list(self._events)
        return [event for event in self._events if event.surface_id == surface_id]

    def summarize(self, surface_id: str) -> SurfaceChangeSummary:
        events = self.list_events(surface_id=surface_id)
        decision_diffs = [event.decision_diff for event in events if event.decision_diff is not None]
        trust_diffs = [event.trust_diff for event in events if event.trust_diff is not None]
        changed_entity_ids = sorted({event.entity_id for event in events})
        return SurfaceChangeSummary(
            surface_id=surface_id,
            event_count=len(events),
            changed_entity_ids=changed_entity_ids,
            decisions=decision_diffs,
            trust_diffs=trust_diffs,
            events=events,
        )
