from __future__ import annotations

import hashlib
import json
import re
from collections import defaultdict
from datetime import UTC, datetime
from urllib.parse import urlparse

from app.models.types import Citation, InsightRecord, MCPItem


class CitationPolicyError(ValueError):
    pass


def extract_publisher_identity(server_name: str, remote_url: str) -> str:
    if server_name and "/" in server_name:
        return server_name.split("/", 1)[0]
    parsed = urlparse(remote_url)
    if parsed.hostname:
        host = parsed.hostname.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    return "unknown-publisher"


def sanitize_templated_url(url: str, fallback_url: str) -> str:
    if not url:
        return fallback_url
    if "{" in url or "}" in url:
        return fallback_url
    if not re.match(r"^https?://", url):
        return fallback_url
    return url


def compute_stable_hash(payload: object) -> str:
    normalized = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def mcp_items_to_insight_candidates(
    server_id: str,
    server_url: str,
    items: list[MCPItem],
) -> list[InsightRecord]:
    candidates: list[InsightRecord] = []
    now = datetime.now(UTC)

    keywords = {
        "macro": "macro",
        "inflation": "macro",
        "rates": "macro",
        "liquidity": "risk_event",
        "volatility": "risk_event",
        "credit": "risk_event",
        "valuation": "valuation",
        "central bank": "macro",
        "13f": "portfolio",
        "buyback": "portfolio",
    }

    for idx, item in enumerate(items[:30]):
        text = " ".join(
            part for part in [item.title or "", item.content or "", item.uri or ""] if part
        ).lower()
        chosen_theme = "market"
        for needle, theme in keywords.items():
            if needle in text:
                chosen_theme = theme
                break

        summary_src = (item.title or item.content or "").strip()
        if len(summary_src) < 20:
            continue
        summary = summary_src.replace("\n", " ")[:280]

        citation = Citation(
            url=sanitize_templated_url(item.uri or server_url, server_url),
            source_id=f"{server_id}_item_{idx}",
            retrieved_at=now,
            importance="MCP extracted item from server snapshot",
        )

        candidates.append(
            InsightRecord(
                insight_id=f"insight_{server_id}_{idx}",
                theme=chosen_theme,
                summary=summary,
                stance="neutral",
                confidence=0.45,
                time_horizon="short",
                citations=[citation],
            )
        )
    return candidates


def validate_citations(insight: InsightRecord, actionable: bool = False) -> None:
    if not insight.citations:
        raise CitationPolicyError(f"Insight {insight.insight_id} has no citations")

    source_counts = defaultdict(int)
    primary_count = 0
    for citation in insight.citations:
        source_counts[citation.source_id] += 1
        if "official" in citation.importance.lower() or "primary" in citation.importance.lower():
            primary_count += 1

    independent_sources = len(source_counts)
    if actionable and independent_sources < 2 and primary_count < 1:
        raise CitationPolicyError(
            "Action-relevant insights require >=2 independent sources or >=1 primary source"
        )


def validate_report_sections(sections: list[InsightRecord]) -> None:
    if not sections:
        raise CitationPolicyError("Report has no sections")
    for section in sections:
        validate_citations(section, actionable=True)


def validate_section_citations(
    section_name: str,
    citations: list[Citation],
    actionable: bool = True,
) -> None:
    if not citations:
        raise CitationPolicyError(f"Section '{section_name}' has no citations")

    source_ids = {citation.source_id for citation in citations}
    primary = any(
        "official" in citation.importance.lower() or "primary" in citation.importance.lower()
        for citation in citations
    )

    if actionable and len(source_ids) < 2 and not primary:
        raise CitationPolicyError(
            f"Section '{section_name}' requires >=2 independent sources or >=1 primary source"
        )
