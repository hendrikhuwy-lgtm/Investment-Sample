from __future__ import annotations

from typing import Any

from app.v2.core.domain_objects import InstrumentTruth
from app.v2.sources.registry import get_news_adapter
from app.v2.storage.surface_snapshot_store import latest_surface_snapshot


def _text(value: Any) -> str:
    return str(value or "").strip()


def _slug(value: str) -> str:
    return _text(value).lower().replace(" ", "_").replace("/", "_").replace("-", "_") or "unknown"


def _field_label(field_name: str) -> str:
    return _text(field_name).replace("_", " ").title() or "Field"


def _unique(items: list[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for item in items:
        normalized = _text(item)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def _issuer_tokens(truth: InstrumentTruth) -> list[str]:
    issuer = _text(truth.metrics.get("issuer"))
    name = _text(truth.name)
    raw = [truth.symbol, issuer, name]
    tokens: list[str] = []
    for value in raw:
        for part in value.replace("/", " ").replace("-", " ").split():
            token = part.strip().lower()
            if len(token) >= 4:
                tokens.append(token)
    return _unique(tokens)[:6]


def _priority_bucket(field_name: str) -> str:
    normalized = _text(field_name).lower()
    if normalized in {"aum", "tracking_difference_1y", "launch_date", "primary_trading_currency", "primary_listing_exchange"}:
        return "high"
    if normalized in {"expense_ratio", "replication_method", "benchmark_key", "benchmark_name"}:
        return "medium"
    return "low"


def _relevance_channels(truth: InstrumentTruth, sleeve_key: str | None = None) -> set[str]:
    sleeve = _text(sleeve_key or truth.metrics.get("sleeve_key")).lower()
    asset_class = _text(truth.asset_class).lower()
    channels: set[str] = set()
    if sleeve in {"ig_bond", "cash", "cash_bills"} or "bond" in asset_class or "fixed" in asset_class:
        channels.update({"rates_policy", "credit", "funding"})
    if sleeve in {"real_asset", "real_assets"} or "commodity" in asset_class or "gold" in _text(truth.name).lower():
        channels.update({"inflation_energy", "commodities"})
    if sleeve in {"china_satellite", "emerging_markets"}:
        channels.update({"china_em", "dollar_fx", "equity"})
    if sleeve in {"global_equity", "developed_ex_us"} or "equity" in asset_class:
        channels.update({"equity", "dollar_fx", "rates_policy"})
    if not channels:
        channels.update({"equity", "rates_policy", "dollar_fx"})
    return channels


def _classify_news_cluster(headline: str) -> str:
    lowered = _text(headline).lower()
    if any(token in lowered for token in ("fed", "ecb", "rates", "yield", "treasury", "policy")):
        return "rates_policy"
    if any(token in lowered for token in ("inflation", "cpi", "prices", "oil", "brent", "wti", "energy")):
        return "inflation_energy"
    if any(token in lowered for token in ("china", "emerging", "yuan", "tariff", "trade")):
        return "china_em"
    if any(token in lowered for token in ("credit", "spread", "default", "funding", "liquidity")):
        return "credit"
    if any(token in lowered for token in ("dollar", "fx", "currency", "usd")):
        return "dollar_fx"
    if any(token in lowered for token in ("etf", "fund", "flows", "issuer")):
        return "candidate_specific"
    return "equity"


def _tone_from_headline(headline: str) -> str:
    lowered = _text(headline).lower()
    worsening = (
        "selloff",
        "slump",
        "stress",
        "widens",
        "widening",
        "jumps",
        "surges",
        "hawkish",
        "tariff",
        "war",
        "conflict",
        "pressure",
    )
    improving = (
        "rally",
        "eases",
        "easing",
        "cools",
        "stabilizes",
        "stabilising",
        "beat",
        "rebound",
        "recovery",
        "relief",
        "cuts",
    )
    if any(token in lowered for token in worsening):
        return "worsening"
    if any(token in lowered for token in improving):
        return "improving"
    return "neutral"


def _cluster_label(cluster_id: str) -> str:
    return {
        "rates_policy": "Rates and policy",
        "inflation_energy": "Inflation and energy",
        "china_em": "China and EM",
        "credit": "Credit and funding",
        "dollar_fx": "Dollar and FX",
        "candidate_specific": "ETF and issuer",
        "equity": "Equity market",
    }.get(cluster_id, cluster_id.replace("_", " ").title())


def _market_context_summary(*, truth: InstrumentTruth, benchmark_label: str | None = None) -> dict[str, Any] | None:
    provenance = dict(truth.metrics.get("source_provenance") or {})
    price = truth.metrics.get("price")
    change = truth.metrics.get("change_pct_1d")
    if price is None and change is None and not benchmark_label:
        return None
    provider = _text((provenance.get("price") or {}).get("provider"))
    path = _text((provenance.get("price") or {}).get("path"))
    freshness = _text((provenance.get("price") or {}).get("freshness") or (provenance.get("change_pct_1d") or {}).get("freshness"))
    price_text = f"{float(price):,.2f}" if isinstance(price, (int, float)) else "price unavailable"
    if isinstance(change, (int, float)):
        change_text = f"{float(change):+.2f}% on the day"
    else:
        change_text = "daily move unavailable"
    benchmark_line = (
        f"Benchmark frame: {benchmark_label} remains the main comparison anchor."
        if _text(benchmark_label)
        else None
    )
    provenance_bits = [bit for bit in [provider, path.replace("_", " ") if path else None, freshness.replace("_", " ") if freshness else None] if bit]
    return {
        "title": "Market context",
        "summary": f"{truth.symbol} is trading at {price_text}; {change_text}.",
        "instrument_line": f"Current market support comes through {' · '.join(provenance_bits)}." if provenance_bits else None,
        "benchmark_line": benchmark_line,
        "freshness_note": "Use this as supporting market context, not as the primary recommendation gate.",
    }


def build_retrieval_guides(
    *,
    truth: InstrumentTruth,
    source_authority_fields: list[dict[str, Any]] | None = None,
    reconciliation_report: list[dict[str, Any]] | None = None,
    primary_document_manifest: list[dict[str, Any]] | None = None,
    target_surface: str,
) -> list[dict[str, Any]]:
    issuer = _text(truth.metrics.get("issuer")) or _text(truth.name)
    symbol = _text(truth.symbol)
    benchmark_label = _text(truth.metrics.get("benchmark_label") or truth.benchmark_id)
    guides: list[dict[str, Any]] = []
    for field in list(source_authority_fields or []):
        field_name = _text(field.get("field_name")).lower()
        if not field_name:
            continue
        if not bool(field.get("recommendation_critical")) and _text(field.get("document_support_state")).lower() not in {"missing", "preferred_missing"}:
            continue
        label = _field_label(field_name)
        query = f"{symbol} {label} {issuer}".strip()
        preferred_sources = ["issuer factsheet", "issuer KID", "issuer annual report"]
        guides.append(
            {
                "guide_id": f"retrieval_{_slug(symbol)}_{_slug(field_name)}",
                "label": f"Resolve {label}",
                "query": query,
                "reason": f"{label} still needs stronger backing before the candidate is fully recommendation-ready.",
                "priority": _priority_bucket(field_name),
                "preferred_sources": preferred_sources,
                "target_surface": target_surface,
            }
        )
    for item in list(reconciliation_report or []):
        status = _text(item.get("status")).lower()
        field_name = _text(item.get("field_name")).lower()
        if status not in {"hard_conflict", "soft_drift", "critical_missing", "weak_authority", "stale"} or not field_name:
            continue
        label = _field_label(field_name)
        query = f"{symbol} {label} official source {issuer}".strip()
        guides.append(
            {
                "guide_id": f"reconcile_{_slug(symbol)}_{_slug(field_name)}",
                "label": f"Reconcile {label}",
                "query": query,
                "reason": _text(item.get("summary")) or f"{label} currently disagrees across sources.",
                "priority": "high" if status in {"hard_conflict", "critical_missing"} else "medium",
                "preferred_sources": ["issuer document", "benchmark methodology", "official ETF profile"],
                "target_surface": target_surface,
            }
        )
    if benchmark_label:
        guides.append(
            {
                "guide_id": f"benchmark_{_slug(symbol)}",
                "label": "Validate benchmark methodology",
                "query": f"{benchmark_label} benchmark methodology factsheet",
                "reason": "Benchmark methodology is part of the comparison frame and should stay easy to verify.",
                "priority": "medium",
                "preferred_sources": ["benchmark factsheet", "index methodology"],
                "target_surface": target_surface,
            }
        )
    if not guides and list(primary_document_manifest or []):
        doc = dict(list(primary_document_manifest or [])[0] or {})
        doc_type = _text(doc.get("doc_type") or "issuer document").replace("_", " ")
        guides.append(
            {
                "guide_id": f"document_refresh_{_slug(symbol)}",
                "label": "Refresh issuer documentation",
                "query": f"{symbol} {issuer} {doc_type}",
                "reason": "Current evidence is document-backed, so refreshing issuer documentation remains the cleanest next step.",
                "priority": "low",
                "preferred_sources": [doc_type],
                "target_surface": target_surface,
            }
        )
    deduped: dict[str, dict[str, Any]] = {}
    for guide in guides:
        deduped.setdefault(str(guide["guide_id"]), guide)
    ordered = sorted(
        deduped.values(),
        key=lambda item: ({"high": 0, "medium": 1, "low": 2}.get(str(item.get("priority") or "low"), 9), str(item.get("label") or "")),
    )
    return ordered[:6]


def build_news_clusters(
    *,
    truth: InstrumentTruth,
    sleeve_key: str | None = None,
    limit: int = 8,
    surface_name: str = "evidence_workspace",
) -> list[dict[str, Any]]:
    try:
        items = list(get_news_adapter().fetch(limit=limit, surface_name=surface_name) or [])
    except Exception:
        items = []
    if not items:
        return []
    interest_tokens = set(_issuer_tokens(truth))
    channels = _relevance_channels(truth, sleeve_key=sleeve_key)
    grouped: dict[str, dict[str, Any]] = {}
    for row in items:
        headline = _text(dict(row).get("headline"))
        if not headline:
            continue
        lowered = headline.lower()
        cluster_id = _classify_news_cluster(headline)
        score = 0
        if any(token in lowered for token in interest_tokens):
            score += 3
        if cluster_id in channels:
            score += 2
        if truth.symbol.lower() in lowered:
            score += 4
        if score <= 0:
            continue
        group = grouped.setdefault(
            cluster_id,
            {"cluster_id": cluster_id, "headlines": [], "tones": [], "score": 0},
        )
        group["score"] += score
        group["headlines"].append(headline)
        group["tones"].append(_tone_from_headline(headline))
    if not grouped:
        return []
    rows: list[dict[str, Any]] = []
    for cluster_id, group in grouped.items():
        tones = list(group["tones"])
        if tones and len(set(tones)) == 1:
            tone = tones[0]
        elif "worsening" in tones and "improving" in tones:
            tone = "mixed"
        elif "worsening" in tones:
            tone = "worsening"
        elif "improving" in tones:
            tone = "improving"
        else:
            tone = "neutral"
        headlines = _unique(list(group["headlines"]))[:3]
        label = _cluster_label(cluster_id)
        rows.append(
            {
                "cluster_id": cluster_id,
                "label": label,
                "summary": f"{label} is part of the current market context with {len(headlines)} headline{'s' if len(headlines) != 1 else ''} worth tracking.",
                "tone": tone,
                "headline_count": len(group["headlines"]),
                "headlines": headlines,
                "score": int(group["score"]),
            }
        )
    rows.sort(key=lambda item: (-int(item.get("score") or 0), str(item.get("label") or "")))
    for item in rows:
        item.pop("score", None)
    return rows[:3]


def build_sentiment_annotation(news_clusters: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    clusters = list(news_clusters or [])
    if not clusters:
        return None
    tones = [_text(item.get("tone")).lower() for item in clusters]
    if "worsening" in tones and "improving" in tones:
        label, tone = "Mixed narrative tone", "warn"
        summary = "Headline tone is mixed. Keep it as a secondary annotation, not as a decision signal."
    elif "worsening" in tones:
        label, tone = "Narrative tone worsening", "warn"
        summary = "Headline tone is leaning more negative, but it still remains a secondary evidence modifier."
    elif "improving" in tones:
        label, tone = "Narrative tone improving", "good"
        summary = "Headline tone is improving, but it still remains secondary to direct market and issuer evidence."
    else:
        label, tone = "Narrative tone neutral", "neutral"
        summary = "Headline tone is balanced and should stay in the background."
    return {"label": label, "tone": tone, "summary": summary}


def build_thesis_drift(
    *,
    surface_id: str,
    object_id: str,
    current_state: dict[str, Any],
) -> dict[str, Any]:
    prior_snapshot = latest_surface_snapshot(surface_id=surface_id, object_id=object_id)
    if prior_snapshot is None:
        return {
            "state": "thesis_unchanged",
            "summary": "No prior saved view exists yet, so this remains the starting point for future drift checks.",
            "evidence_delta": "A prior saved view is still needed before evidence change can be measured.",
            "consequence_delta": "Treat the current recommendation view as the initial baseline.",
            "confidence_delta": "No prior confidence baseline is available yet.",
            "watchlist_priority_delta": "Keep the current blockers and support items on watch until a second saved view exists.",
            "prior_generated_at": None,
        }

    prior_contract = dict(prior_snapshot.get("contract") or {})
    prior_gate = dict(prior_contract.get("recommendation_gate") or {})
    prior_quality = dict(prior_contract.get("data_quality_summary") or {})
    prior_recon = dict(prior_contract.get("reconciliation_status") or {})

    def _gate_rank(value: Any) -> int:
        return {"admissible": 3, "review_only": 2, "blocked": 1}.get(_text(value).lower(), 0)

    def _confidence_rank(value: Any) -> int:
        return {"high": 3, "mixed": 2, "low": 1}.get(_text(value).lower(), 0)

    def _recon_rank(value: Any) -> int:
        return {"verified": 0, "soft_drift": 1, "hard_conflict": 2}.get(_text(value).lower(), 1)

    current_gate_state = _text(current_state.get("gate_state") or current_state.get("recommendation_gate"))
    current_confidence = _text(current_state.get("data_confidence"))
    current_reconciliation = _text(current_state.get("reconciliation_status"))
    current_blocked = int(current_state.get("blocked_reason_count") or 0)
    current_missing = int(current_state.get("critical_missing_count") or 0)

    prior_gate_state = _text(prior_gate.get("gate_state"))
    prior_confidence = _text(prior_quality.get("data_confidence") or prior_gate.get("data_confidence"))
    prior_reconciliation = _text(prior_recon.get("status"))
    prior_blocked = len(list(prior_gate.get("blocked_reasons") or []))
    prior_missing = len(list(prior_gate.get("critical_missing_fields") or []))

    current_score = (_gate_rank(current_gate_state) * 4) + (_confidence_rank(current_confidence) * 2) - (_recon_rank(current_reconciliation) * 2) - current_blocked - current_missing
    prior_score = (_gate_rank(prior_gate_state) * 4) + (_confidence_rank(prior_confidence) * 2) - (_recon_rank(prior_reconciliation) * 2) - prior_blocked - prior_missing
    delta = current_score - prior_score

    if current_gate_state == "blocked" and prior_gate_state in {"admissible", "review_only"}:
        state = "thesis_falsified"
        summary = "The candidate view is now blocked by a new critical dispute or missing support."
    elif delta >= 2:
        state = "thesis_strengthened"
        summary = "The candidate view strengthened because evidence quality or recommendation readiness improved."
    elif delta <= -2:
        state = "thesis_weakened"
        summary = "The candidate view weakened because blockers, drift, or lower-confidence evidence increased."
    else:
        state = "thesis_unchanged"
        summary = "The candidate view is broadly unchanged versus the last saved report."

    if current_missing < prior_missing or current_blocked < prior_blocked:
        evidence_delta = "Fewer critical blockers remain in the current evidence set."
    elif current_missing > prior_missing or current_blocked > prior_blocked:
        evidence_delta = "More critical gaps or blocked reasons are now visible in the evidence set."
    else:
        evidence_delta = "The critical evidence set is broadly unchanged."

    if _gate_rank(current_gate_state) > _gate_rank(prior_gate_state):
        consequence_delta = "This candidate moved closer to a clean sleeve-review decision."
    elif _gate_rank(current_gate_state) < _gate_rank(prior_gate_state):
        consequence_delta = "This candidate moved further away from a clean sleeve-review decision."
    else:
        consequence_delta = "The portfolio consequence is still broadly the same."

    if _confidence_rank(current_confidence) > _confidence_rank(prior_confidence):
        confidence_delta = f"Data confidence improved from {_text(prior_confidence).replace('_', ' ')} to {_text(current_confidence).replace('_', ' ')}."
    elif _confidence_rank(current_confidence) < _confidence_rank(prior_confidence):
        confidence_delta = f"Data confidence slipped from {_text(prior_confidence).replace('_', ' ')} to {_text(current_confidence).replace('_', ' ')}."
    else:
        confidence_delta = "Data confidence is broadly unchanged."

    if state == "thesis_strengthened":
        watchlist = "Keep the remaining blockers on active watch, but priority can start shifting from repair toward confirmation."
    elif state == "thesis_falsified":
        watchlist = "Escalate the disputed fields and keep this candidate in active repair mode before treating the prior thesis as live."
    elif state == "thesis_weakened":
        watchlist = "Keep the candidate on active watch until the new drift or blocker is resolved."
    else:
        watchlist = "Maintain the current watchlist. No material reprioritization is justified yet."

    return {
        "state": state,
        "summary": summary,
        "evidence_delta": evidence_delta,
        "consequence_delta": consequence_delta,
        "confidence_delta": confidence_delta,
        "watchlist_priority_delta": watchlist,
        "prior_generated_at": prior_snapshot.get("generated_at"),
    }


def build_logic_map(
    *,
    title: str,
    evidence_summary: str,
    market_context: dict[str, Any] | None,
    decision_line: str,
) -> dict[str, Any]:
    steps = [
        {"label": "Evidence", "detail": _text(evidence_summary) or "Current evidence remains the starting point."},
        {"label": "Market context", "detail": _text((market_context or {}).get("summary")) or "Market context is secondary and still bounded."},
        {"label": "Decision line", "detail": _text(decision_line) or "Current decision line remains bounded by recommendation readiness."},
    ]
    return {"title": title, "steps": steps}


def build_drafting_support(
    *,
    truth: InstrumentTruth,
    recommendation_gate: dict[str, Any] | None,
    data_quality_summary: dict[str, Any] | None,
    implementation_profile: dict[str, Any] | None,
    thesis_drift: dict[str, Any] | None,
    retrieval_guides: list[dict[str, Any]] | None,
    news_clusters: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    gate = dict(recommendation_gate or {})
    quality = dict(data_quality_summary or {})
    implementation = dict(implementation_profile or {})
    guides = list(retrieval_guides or [])
    clusters = list(news_clusters or [])
    title = f"{truth.symbol} research memo draft"
    summary_parts = [
        _text(gate.get("summary")),
        _text(quality.get("summary")),
        _text((thesis_drift or {}).get("summary")),
    ]
    summary = " ".join(part for part in summary_parts if part).strip() or f"{truth.name} remains under review with bounded evidence support."
    questions: list[str] = []
    for reason in list(gate.get("blocked_reasons") or [])[:2]:
        questions.append(f"What would resolve this blocker: {reason}?")
    if implementation.get("summary"):
        questions.append(f"Which implementation detail still matters most: {implementation.get('summary')}?")
    for cluster in clusters[:1]:
        questions.append(f"Does current news context in {cluster.get('label')} change the review priority?")
    next_steps = [f"{guide.get('label')}: {guide.get('query')}" for guide in guides[:3]]
    if not next_steps:
        next_steps = ["Keep the current candidate evidence map current and review the next saved report for real drift."]
    return {
        "suggested_title": title,
        "summary": summary,
        "key_questions": _unique(questions)[:4],
        "next_steps": _unique(next_steps)[:4],
    }


def build_research_support_summary(
    *,
    drift_surface_id: str | None,
    drift_object_id: str | None,
    drift_state: dict[str, Any] | None,
) -> str | None:
    if not drift_surface_id or not drift_object_id or drift_state is None:
        return None
    drift = build_thesis_drift(
        surface_id=drift_surface_id,
        object_id=drift_object_id,
        current_state=drift_state,
    )
    summary = _text(drift.get("summary"))
    return summary or None


def build_research_support_pack(
    *,
    truth: InstrumentTruth,
    target_surface: str,
    source_authority_fields: list[dict[str, Any]] | None = None,
    reconciliation_report: list[dict[str, Any]] | None = None,
    primary_document_manifest: list[dict[str, Any]] | None = None,
    recommendation_gate: dict[str, Any] | None = None,
    data_quality_summary: dict[str, Any] | None = None,
    implementation_profile: dict[str, Any] | None = None,
    market_context: dict[str, Any] | None = None,
    evidence_summary: str | None = None,
    decision_line: str | None = None,
    drift_surface_id: str | None = None,
    drift_object_id: str | None = None,
    drift_state: dict[str, Any] | None = None,
    sleeve_key: str | None = None,
) -> dict[str, Any]:
    retrieval_guides = build_retrieval_guides(
        truth=truth,
        source_authority_fields=source_authority_fields,
        reconciliation_report=reconciliation_report,
        primary_document_manifest=primary_document_manifest,
        target_surface=target_surface,
    )
    news_clusters = build_news_clusters(
        truth=truth,
        sleeve_key=sleeve_key,
        surface_name=target_surface,
    )
    sentiment = build_sentiment_annotation(news_clusters)
    thesis_drift = (
        build_thesis_drift(surface_id=drift_surface_id, object_id=drift_object_id, current_state=drift_state or {})
        if drift_surface_id and drift_object_id and drift_state is not None
        else None
    )
    stock_context = market_context or _market_context_summary(
        truth=truth,
        benchmark_label=_text(truth.metrics.get("benchmark_label") or truth.benchmark_id),
    )
    logic_map = build_logic_map(
        title=f"{truth.symbol} decision chain",
        evidence_summary=evidence_summary or _text((recommendation_gate or {}).get("summary")) or "Current evidence remains bounded.",
        market_context=stock_context,
        decision_line=decision_line or _text((recommendation_gate or {}).get("summary")) or "Current decision line remains bounded.",
    )
    drafting_support = build_drafting_support(
        truth=truth,
        recommendation_gate=recommendation_gate,
        data_quality_summary=data_quality_summary,
        implementation_profile=implementation_profile,
        thesis_drift=thesis_drift,
        retrieval_guides=retrieval_guides,
        news_clusters=news_clusters,
    )
    return {
        "retrieval_guides": retrieval_guides,
        "news_clusters": news_clusters,
        "market_context": stock_context,
        "thesis_drift": thesis_drift,
        "drafting_support": drafting_support,
        "logic_map": logic_map,
        "sentiment_annotation": sentiment,
    }
