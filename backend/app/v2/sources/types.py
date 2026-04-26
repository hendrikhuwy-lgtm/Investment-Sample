from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal, Mapping


SourceTier = Literal["1A", "1B", "2", "3", "4-Optional", "Migration-only", "Demote"]
SurfaceName = Literal["blueprint", "daily_brief", "shared"]
ConnectorKind = Literal["service", "database", "file", "mcp"]
FreshnessState = Literal["fresh", "aging", "stale", "expired", "unknown", "not_applicable"]
UpstreamState = Literal["available", "degraded", "unavailable", "planned", "manual"]
IssueSeverity = Literal["info", "warning", "error"]


@dataclass(frozen=True, slots=True)
class FreshnessPolicy:
    fresh_seconds: int | None = None
    stale_seconds: int | None = None
    expires_seconds: int | None = None
    not_applicable: bool = False
    notes: str = ""


@dataclass(frozen=True, slots=True)
class FreshnessEvaluation:
    state: FreshnessState
    age_seconds: int | None
    observed_at: datetime | None
    retrieved_at: datetime | None
    policy: FreshnessPolicy


@dataclass(frozen=True, slots=True)
class SourceDefinition:
    key: str
    name: str
    tier: SourceTier
    surface: SurfaceName
    donor: str
    connector_kind: ConnectorKind
    authoritative_fields: tuple[str, ...] = ()
    freshness_policy: FreshnessPolicy = FreshnessPolicy()
    notes: str = ""


@dataclass(frozen=True, slots=True)
class SourceCitation:
    label: str
    locator: str
    retrieved_at: datetime | None = None
    observed_at: datetime | None = None
    publisher: str | None = None


@dataclass(frozen=True, slots=True)
class SourceRecord:
    source_key: str
    source_name: str
    source_tier: SourceTier
    surface: SurfaceName
    donor_name: str
    connector_kind: ConnectorKind
    freshness: FreshnessEvaluation
    citations: tuple[SourceCitation, ...] = ()
    payload: Mapping[str, Any] = field(default_factory=dict)
    upstream_state: UpstreamState = "available"
    retrieved_at: datetime | None = None
    observed_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class TranslationIssue:
    code: str
    message: str
    severity: IssueSeverity = "warning"
    field_name: str | None = None


@dataclass(frozen=True, slots=True)
class TranslationResult:
    source_key: str
    record: SourceRecord
    issues: tuple[TranslationIssue, ...] = ()


@dataclass(frozen=True, slots=True)
class MCPConnectorRecord:
    name: str
    publisher: str
    category: str
    tier: SourceTier
    status: str
    source_file: str
    rationale: str
    requires_adapter: bool = False
    allow_missing_env: bool = True

