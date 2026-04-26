"""MCP Source Policy - Allowlist and validation for investment analysis pipeline.

This module enforces strict quality controls on MCP sources used for portfolio
decisions and investment analysis. Only vetted, high-quality sources meeting
institutional standards are permitted in production.

Design principles:
1. Deny by default - Sources must be explicitly allowlisted
2. Tier-based filtering - Only Primary/Secondary tiers for production
3. Metadata requirements - Publisher, license, uptime tracking mandatory
4. Sandbox separation - Exploratory sources isolated from production
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


# Sources explicitly banned from investment analysis pipeline
BANNED_SOURCE_PATTERNS = [
    "reddit",
    "x-spaces",
    "twitter",
    "lenny-rachitsky-podcast",
    "petstore",
    "podcast",
]

# Production allowlist: Government/Primary sources
PRODUCTION_ALLOWLIST_PRIMARY = [
    "federalreserve",
    "sec.gov",
    "irs.gov",
    "iras.gov.sg",
    "treasury.gov",
    "fred.stlouisfed.org",
    "cboe.com",  # Exchange data
]

# Production allowlist: Secondary/Research sources
PRODUCTION_ALLOWLIST_SECONDARY = [
    "bloomberg",
    "reuters",
    "wsj",
    "ssrn",
    "arxiv",
    "quantconnect",
]

# Sandbox-only sources (cannot reach production)
SANDBOX_ONLY_SOURCES = [
    "google-news",
    "alt-sentiment",
    "social-macro",
]

SourceMode = Literal["production", "sandbox"]
CredibilityTier = Literal["primary", "secondary", "tertiary"]


@dataclass(frozen=True)
class MCPSourcePolicy:
    """Policy decision for an MCP source."""

    allowed: bool
    mode: SourceMode
    tier: CredibilityTier
    reason: str
    requires_metadata: bool = True


def is_banned_source(server_id: str, url: str, publisher: str) -> bool:
    """Check if source matches banned patterns."""
    combined = f"{server_id} {url} {publisher}".lower()
    return any(pattern in combined for pattern in BANNED_SOURCE_PATTERNS)


def classify_tier(server_id: str, url: str, publisher: str) -> CredibilityTier:
    """Classify source credibility tier."""
    combined = f"{server_id} {url} {publisher}".lower()

    # Primary: Government and regulatory
    if any(pattern in combined for pattern in PRODUCTION_ALLOWLIST_PRIMARY):
        return "primary"

    # Secondary: Reputable publishers and research
    if any(pattern in combined for pattern in PRODUCTION_ALLOWLIST_SECONDARY):
        return "secondary"

    return "tertiary"


def is_production_allowed(
    server_id: str,
    url: str,
    publisher: str,
    tier: CredibilityTier,
) -> bool:
    """Check if source is allowed in production investment analysis."""
    # Banned sources never allowed
    if is_banned_source(server_id, url, publisher):
        return False

    # Sandbox-only sources never in production
    combined = f"{server_id} {url} {publisher}".lower()
    if any(pattern in combined for pattern in SANDBOX_ONLY_SOURCES):
        return False

    # Only Primary and Secondary tiers allowed in production
    return tier in ("primary", "secondary")


def validate_metadata(metadata: dict[str, any]) -> tuple[bool, list[str]]:
    """Validate required metadata fields for MCP sources.

    Required fields:
    - publisher_name: Known entity with regulatory oversight
    - license_url or license_text: Explicit data licensing terms
    - uptime_90d: Minimum 90% uptime over 3 months
    - tier: Credibility classification
    - maintainer_contact: Publisher/maintainer identification

    Returns:
        (is_valid, missing_fields)
    """
    required_fields = {
        "publisher_name": str,
        "uptime_90d": (int, float),
        "tier": str,
        "maintainer_contact": str,
    }

    missing = []
    for field, expected_type in required_fields.items():
        value = metadata.get(field)
        if value is None:
            missing.append(field)
            continue
        if not isinstance(value, expected_type):
            missing.append(f"{field} (wrong type)")

    # License validation: must have either license_url or license_text
    has_license = bool(metadata.get("license_url")) or bool(metadata.get("license_text"))
    if not has_license:
        missing.append("license_url or license_text")

    # Uptime validation: must be >= 90%
    uptime = metadata.get("uptime_90d", 0)
    if isinstance(uptime, (int, float)) and uptime < 0.90:
        missing.append("uptime_90d (below 90%)")

    return len(missing) == 0, missing


def evaluate_source(
    server_id: str,
    url: str,
    publisher: str,
    metadata: dict[str, any] | None = None,
    mode: SourceMode = "production",
) -> MCPSourcePolicy:
    """Evaluate MCP source against policy requirements.

    Args:
        server_id: MCP server identifier
        url: Server endpoint URL
        publisher: Publisher name
        metadata: Optional metadata for validation
        mode: "production" or "sandbox"

    Returns:
        MCPSourcePolicy with decision and reasoning
    """
    metadata = metadata or {}

    # Check banned sources first
    if is_banned_source(server_id, url, publisher):
        return MCPSourcePolicy(
            allowed=False,
            mode=mode,
            tier="tertiary",
            reason=f"Banned source: {server_id} matches excluded pattern",
            requires_metadata=False,
        )

    # Classify tier
    tier = classify_tier(server_id, url, publisher)

    # Sandbox mode allows tertiary sources
    if mode == "sandbox":
        # Even in sandbox, validate metadata if available
        if metadata:
            valid, missing = validate_metadata(metadata)
            if not valid:
                return MCPSourcePolicy(
                    allowed=False,
                    mode=mode,
                    tier=tier,
                    reason=f"Missing required metadata: {', '.join(missing)}",
                    requires_metadata=True,
                )
        return MCPSourcePolicy(
            allowed=True,
            mode=mode,
            tier=tier,
            reason="Allowed in sandbox mode",
            requires_metadata=True,
        )

    # Production mode: strict filtering
    if not is_production_allowed(server_id, url, publisher, tier):
        return MCPSourcePolicy(
            allowed=False,
            mode=mode,
            tier=tier,
            reason=f"Production requires Primary or Secondary tier; got {tier}",
            requires_metadata=True,
        )

    # Validate metadata for production sources
    if metadata:
        valid, missing = validate_metadata(metadata)
        if not valid:
            return MCPSourcePolicy(
                allowed=False,
                mode=mode,
                tier=tier,
                reason=f"Missing required metadata for production: {', '.join(missing)}",
                requires_metadata=True,
            )

    return MCPSourcePolicy(
        allowed=True,
        mode=mode,
        tier=tier,
        reason="Approved for production - meets tier and metadata requirements",
        requires_metadata=True,
    )


def filter_sources_for_production(
    sources: list[dict[str, any]],
) -> tuple[list[dict[str, any]], list[dict[str, any]]]:
    """Filter sources for production use.

    Args:
        sources: List of MCP source records

    Returns:
        (production_sources, rejected_sources)
    """
    production = []
    rejected = []

    for source in sources:
        server_id = source.get("server_id", source.get("name", ""))
        url = source.get("url", source.get("endpoint_url", ""))
        publisher = source.get("publisher", "")
        metadata = source.get("metadata", {})

        policy = evaluate_source(
            server_id=server_id,
            url=url,
            publisher=publisher,
            metadata=metadata,
            mode="production",
        )

        if policy.allowed:
            production.append({**source, "mcp_policy": policy})
        else:
            rejected.append({**source, "mcp_policy": policy})

    return production, rejected
