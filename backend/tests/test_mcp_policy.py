"""Tests for MCP source policy enforcement."""

from __future__ import annotations

import pytest

from app.services.mcp_policy import (
    BANNED_SOURCE_PATTERNS,
    classify_tier,
    evaluate_source,
    filter_sources_for_production,
    is_banned_source,
    is_production_allowed,
    validate_metadata,
)


class TestBannedSources:
    """Test that banned sources are properly rejected."""

    def test_reddit_is_banned(self):
        assert is_banned_source(
            server_id="local.reddit-trends/macro-sentiment",
            url="https://reddit.com/api",
            publisher="Reddit Trends MCP Adapter",
        )

    def test_twitter_is_banned(self):
        assert is_banned_source(
            server_id="local.x-spaces/macro-sentiment",
            url="https://x.com/api",
            publisher="X Spaces MCP Adapter",
        )

    def test_petstore_is_banned(self):
        assert is_banned_source(
            server_id="ai.com.mcp/petstore",
            url="https://example.com/petstore",
            publisher="Pet Store Test",
        )

    def test_podcast_is_banned(self):
        assert is_banned_source(
            server_id="ai.com.mcp/lenny-rachitsky-podcast",
            url="https://example.com/podcast",
            publisher="Lenny Rachitsky Podcast",
        )

    def test_legitimate_source_not_banned(self):
        assert not is_banned_source(
            server_id="ai.federalreserve/fred",
            url="https://fred.stlouisfed.org/",
            publisher="Federal Reserve Economic Data",
        )


class TestTierClassification:
    """Test source tier classification."""

    def test_government_sources_are_primary(self):
        assert classify_tier(
            server_id="fred",
            url="https://fred.stlouisfed.org/",
            publisher="Federal Reserve",
        ) == "primary"

        assert classify_tier(
            server_id="sec",
            url="https://sec.gov/",
            publisher="SEC",
        ) == "primary"

    def test_reputable_publishers_are_secondary(self):
        assert classify_tier(
            server_id="bloomberg",
            url="https://bloomberg.com/",
            publisher="Bloomberg",
        ) == "secondary"

        assert classify_tier(
            server_id="reuters",
            url="https://reuters.com/",
            publisher="Reuters",
        ) == "secondary"

        assert classify_tier(
            server_id="ssrn",
            url="https://ssrn.com/",
            publisher="SSRN",
        ) == "secondary"

    def test_unknown_sources_are_tertiary(self):
        assert classify_tier(
            server_id="random-blog",
            url="https://example.com/",
            publisher="Random Blog",
        ) == "tertiary"


class TestProductionAllowlist:
    """Test production allowlist enforcement."""

    def test_primary_sources_allowed_in_production(self):
        assert is_production_allowed(
            server_id="fred",
            url="https://fred.stlouisfed.org/",
            publisher="Federal Reserve",
            tier="primary",
        )

    def test_secondary_sources_allowed_in_production(self):
        assert is_production_allowed(
            server_id="bloomberg",
            url="https://bloomberg.com/",
            publisher="Bloomberg",
            tier="secondary",
        )

    def test_tertiary_sources_blocked_in_production(self):
        assert not is_production_allowed(
            server_id="random-blog",
            url="https://example.com/",
            publisher="Random Blog",
            tier="tertiary",
        )

    def test_banned_sources_blocked_regardless_of_tier(self):
        # Even if classified as primary (shouldn't happen), banned sources are blocked
        assert not is_production_allowed(
            server_id="reddit",
            url="https://reddit.com/",
            publisher="Reddit",
            tier="primary",
        )


class TestMetadataValidation:
    """Test metadata validation requirements."""

    def test_valid_metadata_passes(self):
        metadata = {
            "publisher_name": "Federal Reserve",
            "license_url": "https://fred.stlouisfed.org/legal",
            "uptime_90d": 0.999,
            "tier": "primary",
            "maintainer_contact": "fred@stlouisfed.org",
        }
        is_valid, missing = validate_metadata(metadata)
        assert is_valid
        assert len(missing) == 0

    def test_missing_publisher_name_fails(self):
        metadata = {
            "license_url": "https://example.com/license",
            "uptime_90d": 0.95,
            "tier": "primary",
            "maintainer_contact": "admin@example.com",
        }
        is_valid, missing = validate_metadata(metadata)
        assert not is_valid
        assert "publisher_name" in missing

    def test_missing_license_fails(self):
        metadata = {
            "publisher_name": "Test Publisher",
            "uptime_90d": 0.95,
            "tier": "primary",
            "maintainer_contact": "admin@example.com",
        }
        is_valid, missing = validate_metadata(metadata)
        assert not is_valid
        assert "license_url or license_text" in missing

    def test_low_uptime_fails(self):
        metadata = {
            "publisher_name": "Test Publisher",
            "license_url": "https://example.com/license",
            "uptime_90d": 0.85,  # Below 90% threshold
            "tier": "primary",
            "maintainer_contact": "admin@example.com",
        }
        is_valid, missing = validate_metadata(metadata)
        assert not is_valid
        assert "uptime_90d (below 90%)" in missing


class TestPolicyEvaluation:
    """Test full policy evaluation."""

    def test_banned_source_rejected_in_production(self):
        policy = evaluate_source(
            server_id="reddit",
            url="https://reddit.com/",
            publisher="Reddit",
            mode="production",
        )
        assert not policy.allowed
        assert "Banned source" in policy.reason

    def test_tertiary_source_rejected_in_production(self):
        policy = evaluate_source(
            server_id="random-blog",
            url="https://example.com/",
            publisher="Random Blog",
            mode="production",
        )
        assert not policy.allowed
        assert "Primary or Secondary tier" in policy.reason

    def test_tertiary_source_allowed_in_sandbox(self):
        policy = evaluate_source(
            server_id="random-blog",
            url="https://example.com/",
            publisher="Random Blog",
            mode="sandbox",
        )
        assert policy.allowed
        assert policy.mode == "sandbox"

    def test_primary_source_with_valid_metadata_allowed(self):
        metadata = {
            "publisher_name": "Federal Reserve",
            "license_url": "https://fred.stlouisfed.org/legal",
            "uptime_90d": 0.999,
            "tier": "primary",
            "maintainer_contact": "fred@stlouisfed.org",
        }
        policy = evaluate_source(
            server_id="fred",
            url="https://fred.stlouisfed.org/",
            publisher="Federal Reserve",
            metadata=metadata,
            mode="production",
        )
        assert policy.allowed
        assert policy.tier == "primary"

    def test_primary_source_with_missing_metadata_rejected(self):
        metadata = {
            "publisher_name": "Federal Reserve",
            # Missing license, uptime, tier, maintainer_contact
        }
        policy = evaluate_source(
            server_id="fred",
            url="https://fred.stlouisfed.org/",
            publisher="Federal Reserve",
            metadata=metadata,
            mode="production",
        )
        assert not policy.allowed
        assert "Missing required metadata" in policy.reason


class TestProductionFiltering:
    """Test filtering sources for production pipeline."""

    def test_filter_removes_banned_sources(self):
        sources = [
            {
                "server_id": "fred",
                "url": "https://fred.stlouisfed.org/",
                "publisher": "Federal Reserve",
            },
            {
                "server_id": "reddit",
                "url": "https://reddit.com/",
                "publisher": "Reddit",
            },
        ]
        production, rejected = filter_sources_for_production(sources)
        assert len(production) == 1
        assert production[0]["server_id"] == "fred"
        assert len(rejected) == 1
        assert rejected[0]["server_id"] == "reddit"

    def test_filter_removes_tertiary_sources(self):
        sources = [
            {
                "server_id": "bloomberg",
                "url": "https://bloomberg.com/",
                "publisher": "Bloomberg",
            },
            {
                "server_id": "random-blog",
                "url": "https://example.com/",
                "publisher": "Random Blog",
            },
        ]
        production, rejected = filter_sources_for_production(sources)
        assert len(production) == 1
        assert production[0]["server_id"] == "bloomberg"
        assert len(rejected) == 1
        assert rejected[0]["server_id"] == "random-blog"

    def test_filter_keeps_primary_and_secondary(self):
        sources = [
            {
                "server_id": "fred",
                "url": "https://fred.stlouisfed.org/",
                "publisher": "Federal Reserve",
            },
            {
                "server_id": "bloomberg",
                "url": "https://bloomberg.com/",
                "publisher": "Bloomberg",
            },
            {
                "server_id": "ssrn",
                "url": "https://ssrn.com/",
                "publisher": "SSRN",
            },
        ]
        production, rejected = filter_sources_for_production(sources)
        assert len(production) == 3
        assert len(rejected) == 0


class TestBannedPatterns:
    """Test that all banned patterns are covered."""

    def test_all_banned_patterns_work(self):
        """Verify each banned pattern actually blocks sources."""
        test_cases = [
            ("reddit", "reddit-trends", "https://reddit.com/", "Reddit"),
            ("x-spaces", "x-spaces", "https://x.com/", "X Spaces"),
            ("twitter", "twitter-api", "https://twitter.com/", "Twitter"),
            ("lenny-rachitsky-podcast", "lenny-podcast", "https://example.com/", "Lenny Podcast"),
            ("petstore", "petstore-test", "https://example.com/", "Pet Store"),
            ("podcast", "some-podcast", "https://example.com/", "Some Podcast"),
        ]

        for pattern, server_id, url, publisher in test_cases:
            assert is_banned_source(server_id, url, publisher), \
                f"Pattern '{pattern}' failed to block server_id='{server_id}'"


def test_production_config_excludes_banned_sources():
    """Verify DEFAULT_MCP_PRIORITY_SERVERS doesn't include banned sources."""
    from app.config import DEFAULT_MCP_PRIORITY_SERVERS

    priority_servers = DEFAULT_MCP_PRIORITY_SERVERS.split(",")
    for server in priority_servers:
        server_clean = server.strip()
        assert "petstore" not in server_clean.lower(), \
            "petstore should not be in DEFAULT_MCP_PRIORITY_SERVERS"
        assert "podcast" not in server_clean.lower(), \
            "podcast should not be in DEFAULT_MCP_PRIORITY_SERVERS"
        assert "reddit" not in server_clean.lower(), \
            "reddit should not be in DEFAULT_MCP_PRIORITY_SERVERS"
        assert "twitter" not in server_clean.lower(), \
            "twitter should not be in DEFAULT_MCP_PRIORITY_SERVERS"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
