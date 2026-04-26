from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Iterable, Mapping

from app.v2.sources.freshness import coerce_datetime, evaluate_freshness
from app.v2.sources.registry import get_source_definition
from app.v2.sources.types import SourceCitation, SourceRecord, TranslationIssue, TranslationResult


def _now() -> datetime:
    return datetime.now(UTC)


def _serialize_model(item: Any) -> Any:
    if hasattr(item, "model_dump"):
        return item.model_dump(mode="json")
    return item


def _coerce_payload(payload: Any) -> Mapping[str, Any]:
    if isinstance(payload, Mapping):
        return dict(payload)
    if isinstance(payload, list):
        return {"items": [_serialize_model(item) for item in payload]}
    return {"value": _serialize_model(payload)}


def _citations_from_pairs(
    pairs: Iterable[tuple[str, str]],
    *,
    retrieved_at: datetime | None,
    publisher: str | None = None,
) -> tuple[SourceCitation, ...]:
    citations: list[SourceCitation] = []
    seen: set[str] = set()
    for label, locator in pairs:
        normalized = str(locator).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        citations.append(
            SourceCitation(
                label=label,
                locator=normalized,
                retrieved_at=retrieved_at,
                publisher=publisher,
            )
        )
    return tuple(citations)


def _build_record(
    *,
    source_key: str,
    donor_name: str,
    payload: Mapping[str, Any],
    citations: tuple[SourceCitation, ...] = (),
    observed_at: Any = None,
    retrieved_at: Any = None,
    upstream_state: str = "available",
) -> SourceRecord:
    definition = get_source_definition(source_key)
    observed_dt = coerce_datetime(observed_at)
    retrieved_dt = coerce_datetime(retrieved_at) or _now()
    freshness = evaluate_freshness(
        observed_dt,
        retrieved_at=retrieved_dt,
        policy=definition.freshness_policy,
    )
    return SourceRecord(
        source_key=definition.key,
        source_name=definition.name,
        source_tier=definition.tier,
        surface=definition.surface,
        donor_name=donor_name,
        connector_kind=definition.connector_kind,
        freshness=freshness,
        citations=citations,
        payload=payload,
        upstream_state=upstream_state,
        retrieved_at=retrieved_dt,
        observed_at=observed_dt,
    )


def translate_candidate_registry(candidate_items: list[dict[str, Any]]) -> TranslationResult:
    retrieved_at = _now()
    citation_pairs = []
    for item in candidate_items:
        for link in list(item.get("source_links") or []):
            citation_pairs.append((str(item.get("symbol") or "candidate"), str(link)))
    record = _build_record(
        source_key="blueprint_candidate_registry",
        donor_name="SQLiteBlueprintDonor",
        payload={"candidates": candidate_items, "count": len(candidate_items)},
        citations=_citations_from_pairs(citation_pairs, retrieved_at=retrieved_at),
        observed_at=retrieved_at,
        retrieved_at=retrieved_at,
        upstream_state="available",
    )
    return TranslationResult(source_key=record.source_key, record=record)


def translate_candidate_truth(
    *,
    candidate_symbol: str,
    sleeve_key: str,
    resolved_truth: dict[str, dict[str, Any]],
    completeness: dict[str, Any] | None = None,
) -> TranslationResult:
    retrieved_at = _now()
    citation_pairs = []
    observed_values: list[Any] = []
    issues: list[TranslationIssue] = []
    for field_name, item in resolved_truth.items():
        source_url = str(item.get("source_url") or "").strip()
        if source_url:
            citation_pairs.append((field_name, source_url))
        observed_values.append(item.get("observed_at"))
        if str(item.get("value_type") or "") in {"proxy", "stale", "missing_fetchable", "missing_requires_source_expansion"}:
            issues.append(
                TranslationIssue(
                    code="candidate_truth_degraded",
                    message=f"{candidate_symbol}:{field_name} resolved as {item.get('value_type')}.",
                    field_name=field_name,
                )
            )
    observed_at = max((coerce_datetime(value) for value in observed_values), default=None)
    payload = {
        "candidate_symbol": candidate_symbol,
        "sleeve_key": sleeve_key,
        "fields": resolved_truth,
        "completeness": completeness or {},
    }
    record = _build_record(
        source_key="blueprint_candidate_truth",
        donor_name="SQLiteBlueprintDonor",
        payload=payload,
        citations=_citations_from_pairs(citation_pairs, retrieved_at=retrieved_at),
        observed_at=observed_at,
        retrieved_at=retrieved_at,
        upstream_state="available" if resolved_truth else "degraded",
    )
    return TranslationResult(source_key=record.source_key, record=record, issues=tuple(issues))


def translate_benchmark_assignment(
    *,
    candidate_symbol: str,
    sleeve_key: str,
    assignment: dict[str, Any],
) -> TranslationResult:
    record = _build_record(
        source_key="blueprint_benchmark_assignment",
        donor_name="SQLiteBlueprintDonor",
        payload={
            "candidate_symbol": candidate_symbol,
            "sleeve_key": sleeve_key,
            "assignment": assignment,
        },
        observed_at=_now(),
        retrieved_at=_now(),
        upstream_state="available" if assignment.get("benchmark_key") else "degraded",
    )
    issues: list[TranslationIssue] = []
    if not assignment.get("benchmark_key"):
        issues.append(
            TranslationIssue(
                code="benchmark_unassigned",
                message=f"No benchmark assignment found for {candidate_symbol} in sleeve {sleeve_key}.",
                severity="error",
            )
        )
    elif str(assignment.get("validation_status") or "") in {"mismatch", "proxy_disallowed"}:
        issues.append(
            TranslationIssue(
                code="benchmark_validation_warning",
                message=f"Benchmark assignment for {candidate_symbol} requires review: {assignment.get('validation_status')}.",
            )
        )
    return TranslationResult(source_key=record.source_key, record=record, issues=tuple(issues))


def translate_etf_document_record(
    *,
    symbol: str,
    document_payload: dict[str, Any],
) -> TranslationResult:
    retrieved_at = coerce_datetime(
        dict(document_payload.get("factsheet") or {}).get("retrieved_at")
    ) or _now()
    extracted = dict(document_payload.get("extracted") or {})
    locator_pairs = []
    for doc_key in ("factsheet", "kid", "prospectus"):
        doc_payload = dict(document_payload.get(doc_key) or {})
        doc_url = str(doc_payload.get("doc_url") or "").strip()
        if doc_url:
            locator_pairs.append((doc_key, doc_url))
        cache_file = str(doc_payload.get("cache_file") or "").strip()
        if cache_file:
            locator_pairs.append((f"{doc_key}_cache", cache_file))
    record = _build_record(
        source_key="etf_document_verification",
        donor_name="SQLiteEtfDonor",
        payload={
            "symbol": symbol,
            "document": document_payload,
            "extracted": extracted,
        },
        citations=_citations_from_pairs(locator_pairs, retrieved_at=retrieved_at),
        observed_at=extracted.get("factsheet_date"),
        retrieved_at=retrieved_at,
        upstream_state="available" if document_payload.get("verified") or extracted else "degraded",
    )
    issues: list[TranslationIssue] = []
    for proof_name in list(document_payload.get("verification_missing") or []):
        issues.append(
            TranslationIssue(
                code="document_proof_missing",
                message=f"{symbol} missing document proof for {proof_name}.",
                field_name=proof_name,
            )
        )
    return TranslationResult(source_key=record.source_key, record=record, issues=tuple(issues))


def translate_etf_market_state(
    *,
    symbol: str,
    market_data: dict[str, Any] | None,
    market_summary: dict[str, Any] | None = None,
    holdings_profile: dict[str, Any] | None = None,
) -> TranslationResult:
    combined = {
        "symbol": symbol,
        "market_data": market_data or {},
        "market_summary": market_summary or {},
        "holdings_profile": holdings_profile or {},
    }
    observed_at = (
        (market_data or {}).get("asof_date")
        or (market_summary or {}).get("latest_asof_date")
        or (holdings_profile or {}).get("as_of_date")
    )
    citation_pairs = []
    for payload in (market_data or {}, market_summary or {}, holdings_profile or {}):
        citation = dict(payload.get("citation") or {})
        url = str(citation.get("source_url") or "").strip()
        if url:
            citation_pairs.append((symbol, url))
    record = _build_record(
        source_key="etf_market_state",
        donor_name="SQLiteEtfDonor",
        payload=combined,
        citations=_citations_from_pairs(citation_pairs, retrieved_at=_now()),
        observed_at=observed_at,
        retrieved_at=_now(),
        upstream_state="available" if market_data or market_summary or holdings_profile else "degraded",
    )
    issues: list[TranslationIssue] = []
    if not market_data:
        issues.append(
            TranslationIssue(
                code="market_data_missing",
                message=f"No latest market data available for {symbol}.",
            )
        )
    return TranslationResult(source_key=record.source_key, record=record, issues=tuple(issues))


def translate_provider_surface_payload(
    *,
    surface_name: str,
    payload: dict[str, Any],
) -> TranslationResult:
    retrieved_at = _now()
    providers = list(payload.get("providers") or [])
    snapshots = list(payload.get("snapshots") or [])
    citation_pairs = []
    observed_values = []
    for snapshot in snapshots:
        snapshot_payload = dict(snapshot.get("payload") or {})
        citation = dict(snapshot_payload.get("citation") or {})
        source_id = str(citation.get("source_id") or snapshot.get("provider_name") or "provider")
        locator = str(citation.get("url") or source_id).strip()
        if locator:
            citation_pairs.append((source_id, locator))
        observed_values.append(snapshot_payload.get("observed_at"))
        observed_values.append(snapshot.get("fetched_at"))
    record = _build_record(
        source_key="provider_surface_context",
        donor_name="SQLiteProviderDonor",
        payload={"surface_name": surface_name, **payload},
        citations=_citations_from_pairs(citation_pairs, retrieved_at=retrieved_at),
        observed_at=max((coerce_datetime(value) for value in observed_values), default=None),
        retrieved_at=retrieved_at,
        upstream_state="available" if providers or snapshots else "degraded",
    )
    issues: list[TranslationIssue] = []
    if not snapshots:
        issues.append(
            TranslationIssue(
                code="provider_surface_empty",
                message=f"No cached provider snapshots found for {surface_name}.",
            )
        )
    return TranslationResult(source_key=record.source_key, record=record, issues=tuple(issues))


def translate_portfolio_holdings(holdings: list[Any]) -> TranslationResult:
    retrieved_at = _now()
    serialized = [_serialize_model(item) for item in holdings]
    record = _build_record(
        source_key="portfolio_holdings",
        donor_name="SQLitePortfolioDonor",
        payload={"holdings": serialized, "count": len(serialized)},
        observed_at=retrieved_at,
        retrieved_at=retrieved_at,
        upstream_state="available" if serialized else "manual",
    )
    issues = ()
    if not serialized:
        issues = (
            TranslationIssue(
                code="portfolio_empty",
                message="Portfolio holdings are empty; downstream portfolio-aware logic should treat this as missing context.",
            ),
        )
    return TranslationResult(source_key=record.source_key, record=record, issues=issues)


def translate_portfolio_snapshot(snapshot: Any | None) -> TranslationResult | None:
    if snapshot is None:
        return None
    serialized = _serialize_model(snapshot)
    created_at = serialized.get("created_at")
    record = _build_record(
        source_key="portfolio_snapshot",
        donor_name="SQLitePortfolioDonor",
        payload=serialized,
        observed_at=created_at,
        retrieved_at=_now(),
        upstream_state="available",
    )
    return TranslationResult(source_key=record.source_key, record=record)
