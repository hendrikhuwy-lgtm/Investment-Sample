from __future__ import annotations

import json
import re
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from app.models.types import Citation, InsightRecord, PortfolioSignal, SourceRecord
from app.services.ingest_web import important_source_note
from app.services.language_safety import PERSISTENT_DISCLAIMER
from app.services.normalize import validate_report_sections, validate_section_citations
from app.services.render_utils import (
    emphasize_numbers_html,
    emphasize_numbers_markdown,
    escape_html,
    first_sentence,
    range_bar_percent,
    split_item_and_why,
    split_sentences,
    yes_no,
)


OUTBOX_DIR = Path(__file__).resolve().parents[3] / "outbox"
TEMPLATES_DIR = Path(__file__).resolve().parent / "templates"
HTML_TEMPLATE_PATH = TEMPLATES_DIR / "email_brief.html"
CSS_TEMPLATE_PATH = TEMPLATES_DIR / "email_brief.css"


class ReportBuildError(RuntimeError):
    pass


def build_dashboard_payload(
    allocation: dict[str, float],
    signals: list[PortfolioSignal],
    insights: list[InsightRecord],
    convex_metrics: dict[str, float],
) -> dict:
    validate_report_sections(insights)
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "allocation": allocation,
        "signals": [signal.model_dump(mode="json") for signal in signals],
        "insights": [insight.model_dump(mode="json") for insight in insights],
        "convex": convex_metrics,
        "graphs": {
            "allocation_vs_target": {
                "type": "bar",
                "series": allocation,
            },
            "signal_states": {
                "type": "status-grid",
                "series": {signal.metric: signal.state for signal in signals},
            },
            "guru_view": {
                "type": "panel",
                "series": {
                    "howard_marks": "cycle-aware, risk-first",
                    "taleb": "fragility-aware, convexity-required",
                },
            },
            "convex_kpi": {
                "type": "bar",
                "series": convex_metrics,
            },
        },
    }


def write_reports(payload: dict) -> tuple[Path, Path]:
    OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    json_path = OUTBOX_DIR / f"dashboard_{stamp}.json"
    md_path = OUTBOX_DIR / f"daily_report_{stamp}.md"

    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        f"# Investment Agent Daily Report ({payload['generated_at']})",
        "",
        "## Allocation",
    ]
    for sleeve, weight in payload["allocation"].items():
        lines.append(f"- {sleeve}: {weight:.2%}")

    lines.extend(["", "## Signals"])
    for signal in payload["signals"]:
        lines.append(
            f"- `{signal['metric']}`: {signal['state']} (value={signal['value']}, threshold={signal['threshold']})"
        )

    lines.extend(["", "## Insights (Cited)"])
    for insight in payload["insights"]:
        lines.append(f"- {insight['summary']}")
        for citation in insight["citations"]:
            note = important_source_note(citation["source_id"])
            lines.append(
                f"  - Source: {citation['url']} | retrieved_at: {citation['retrieved_at']} | importance: {note}"
            )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return json_path, md_path


def _pdf_escape(text: str) -> str:
    return text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _wrap_line(text: str, width: int = 104) -> list[str]:
    if len(text) <= width:
        return [text]
    out: list[str] = []
    remaining = text
    while len(remaining) > width:
        idx = remaining.rfind(" ", 0, width)
        if idx <= 0:
            idx = width
        out.append(remaining[:idx].rstrip())
        remaining = remaining[idx:].lstrip()
    if remaining:
        out.append(remaining)
    return out


def _write_simple_pdf_from_lines(lines: list[str], path: Path) -> None:
    wrapped: list[str] = []
    for line in lines:
        wrapped.extend(_wrap_line(line))

    lines_per_page = 52
    pages = [wrapped[i : i + lines_per_page] for i in range(0, len(wrapped), lines_per_page)]
    if not pages:
        pages = [["(empty)"]]

    objects: dict[int, bytes] = {}
    page_start = 3
    page_ids: list[int] = []

    for page_index, page_lines in enumerate(pages):
        page_obj = page_start + page_index * 2
        content_obj = page_obj + 1
        page_ids.append(page_obj)

        content_lines = ["BT", "/F1 10 Tf", "13 TL", "40 780 Td"]
        for line in page_lines:
            content_lines.append(f"({_pdf_escape(line)}) Tj")
            content_lines.append("T*")
        content_lines.append("ET")
        stream = "\n".join(content_lines).encode("utf-8")

        objects[content_obj] = (
            f"<< /Length {len(stream)} >>\nstream\n".encode("utf-8") + stream + b"\nendstream"
        )
        objects[page_obj] = (
            b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
            b"/Resources << /Font << /F1 "
            + f"{page_start + len(pages) * 2} 0 R".encode("utf-8")
            + b" >> >> "
            + f"/Contents {content_obj} 0 R >>".encode("utf-8")
        )

    font_obj = page_start + len(pages) * 2
    objects[font_obj] = b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>"
    objects[2] = (
        b"<< /Type /Pages /Kids [ "
        + " ".join(f"{page_id} 0 R" for page_id in page_ids).encode("utf-8")
        + b" ] "
        + f"/Count {len(page_ids)} >>".encode("utf-8")
    )
    objects[1] = b"<< /Type /Catalog /Pages 2 0 R >>"

    max_obj = max(objects.keys())
    blob = bytearray(b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n")
    offsets = [0] * (max_obj + 1)

    for obj_id in range(1, max_obj + 1):
        offsets[obj_id] = len(blob)
        blob.extend(f"{obj_id} 0 obj\n".encode("utf-8"))
        blob.extend(objects[obj_id])
        blob.extend(b"\nendobj\n")

    xref = len(blob)
    blob.extend(f"xref\n0 {max_obj + 1}\n".encode("utf-8"))
    blob.extend(b"0000000000 65535 f \n")
    for obj_id in range(1, max_obj + 1):
        blob.extend(f"{offsets[obj_id]:010d} 00000 n \n".encode("utf-8"))

    blob.extend(
        f"trailer\n<< /Size {max_obj + 1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode(
            "utf-8"
        )
    )

    path.write_bytes(bytes(blob))


def _build_markdown_table(headers: list[str], rows: list[list[str]]) -> list[str]:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    lines.extend("| " + " | ".join(row) + " |" for row in rows)
    return lines


def _minute_stamp(value: datetime) -> str:
    return value.astimezone(UTC).replace(second=0, microsecond=0).isoformat()


def _citation_key(citation: Citation) -> str:
    return f"{citation.url}|{_minute_stamp(citation.retrieved_at)}|{citation.source_id}"


def _infer_publisher_from_citation(citation: Citation) -> str:
    source_id = citation.source_id.lower()
    if source_id.startswith("fred_"):
        return "FRED"
    if source_id.startswith("irs_"):
        return "IRS"
    if source_id.startswith("iras_"):
        return "IRAS"
    if source_id.startswith("sec_"):
        return "SEC"
    if source_id.startswith("fed_"):
        return "Federal Reserve"
    parsed = urlparse(str(citation.url))
    host = parsed.hostname or ""
    if host.startswith("www."):
        host = host[4:]
    return host or "Unknown publisher"


@dataclass
class CitationEntry:
    index: int
    citation: Citation
    source: SourceRecord | None
    title: str


class CitationRegistry:
    def __init__(self, source_appendix: list[SourceRecord]) -> None:
        self._map: dict[str, CitationEntry] = {}
        self._ordered: list[CitationEntry] = []
        self._source_map: dict[str, SourceRecord] = {source.source_id: source for source in source_appendix}

    def _register(self, citation: Citation) -> int:
        key = _citation_key(citation)
        if key in self._map:
            return self._map[key].index
        source = self._source_map.get(citation.source_id)
        title = citation.importance.split(";")[0].strip() if citation.importance else "Source citation"
        entry = CitationEntry(index=len(self._ordered) + 1, citation=citation, source=source, title=title)
        self._map[key] = entry
        self._ordered.append(entry)
        return entry.index

    def indices(self, citations: list[Citation]) -> list[int]:
        ordered: list[int] = []
        seen: set[int] = set()
        for citation in citations:
            idx = self._register(citation)
            if idx in seen:
                continue
            seen.add(idx)
            ordered.append(idx)
        return ordered

    def markers_md(self, citations: list[Citation]) -> str:
        idxs = self.indices(citations)
        if not idxs:
            return ""
        return " " + " ".join(f"[{idx}]" for idx in idxs)

    def markers_html(self, citations: list[Citation]) -> str:
        idxs = self.indices(citations)
        if not idxs:
            return ""
        return "".join(
            f"<sup class='cite'><a href='#cite-{idx}' aria-label='source {idx}'>{idx}</a></sup>"
            for idx in idxs
        )

    def markdown_footnotes(self) -> list[str]:
        lines: list[str] = []
        for entry in self._ordered:
            source = entry.source
            publisher = source.publisher if source is not None else _infer_publisher_from_citation(entry.citation)
            source_type = source.source_type if source is not None else "citation"
            observed_note = ""
            if entry.citation.observed_at:
                observed_note = f", Data as of {entry.citation.observed_at}"
                if entry.citation.lag_days is not None:
                    observed_note += f", lag {entry.citation.lag_days} day(s)"
            lines.append(
                f"{entry.index}. {publisher}, {entry.title}, Retrieved {_minute_stamp(entry.citation.retrieved_at)}, "
                f"Type: {source_type}{observed_note}, [Link]({entry.citation.url})"
            )
        return lines

    def html_footnotes(self) -> list[str]:
        lines: list[str] = []
        for entry in self._ordered:
            source = entry.source
            publisher = escape_html(
                source.publisher if source is not None else _infer_publisher_from_citation(entry.citation)
            )
            source_type = escape_html(source.source_type if source is not None else "citation")
            observed_note = ""
            if entry.citation.observed_at:
                observed_note = f", Data as of {escape_html(entry.citation.observed_at)}"
                if entry.citation.lag_days is not None:
                    observed_note += f", lag {entry.citation.lag_days} day(s)"
            lines.append(
                f"<li id='cite-{entry.index}'><span class='cite-num'>[{entry.index}]</span> "
                f"{publisher}, {escape_html(entry.title)}, Retrieved {escape_html(_minute_stamp(entry.citation.retrieved_at))}, "
                f"Type: {source_type}{observed_note}, <a href='{entry.citation.url}'>Link</a></li>"
            )
        return lines

    @property
    def citation_count(self) -> int:
        return len(self._ordered)


def _load_template(path: Path, fallback: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return fallback


def _group_sources(source_appendix: list[SourceRecord]) -> dict[str, list[SourceRecord]]:
    order = ["primary", "secondary", "tertiary"]
    dedup: dict[str, SourceRecord] = {}
    for source in source_appendix:
        dedup[source.source_id] = source

    groups: dict[str, list[SourceRecord]] = {tier: [] for tier in order}
    for source in dedup.values():
        groups.setdefault(source.credibility_tier, []).append(source)
    for tier in groups:
        groups[tier].sort(key=lambda source: source.retrieved_at, reverse=True)
    return groups


def _extract_exec_freshness(citations: list[Citation]) -> tuple[int, int]:
    cached = sum(1 for citation in citations if "retrieval=cached" in citation.importance.lower())
    live = len(citations) - cached
    return cached, live


def _default_view_change_triggers(title: str) -> list[str]:
    lower = title.lower()
    if "tax" in lower:
        return [
            "Withholding or treaty treatment assumptions change for SG-resident investors.",
            "Vehicle-level costs or liquidity materially diverge from current assumptions.",
        ]
    if "taleb" in lower:
        return [
            "Volatility and credit stress proxies normalize or re-price sharply in the opposite direction.",
            "Tail-risk hedging costs rise enough to impair convex sleeve efficiency.",
        ]
    if "marks" in lower:
        return [
            "Cycle-sensitive proxies show persistent disinflation with lower rates volatility.",
            "Credit and liquidity conditions move materially away from current regime signals.",
        ]
    return [
        "Joint movement in rates, volatility, and credit proxies breaks the current regime pattern.",
        "Material new policy or liquidity information arrives from primary sources.",
    ]


def _render_range_display(row: dict[str, Any]) -> str:
    bar = str(row.get("range_bar", ""))
    pct = range_bar_percent(bar)
    if pct is None:
        return bar
    return f"{bar} {pct}%"


def _format_optional_float(value: Any, precision: int = 2) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{precision}f}"
    except Exception:
        return "n/a"


def _format_optional_pct(value: Any, precision: int = 1) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{precision}f}%"
    except Exception:
        return "n/a"


def _policy_citations(section: dict[str, Any]) -> list[Citation]:
    citations = list(section.get("citations") or [])
    for item in list(section.get("items") or []):
        citations.extend(list(item.get("citations") or []))
    for item in list(section.get("components") or []):
        citations.extend(list(item.get("citations") or []))
    for item in list(section.get("scenarios") or []):
        citations.extend(list(item.get("citations") or []))
    return citations


def _policy_banner_md(section: dict[str, Any], citation_registry: CitationRegistry | None = None) -> list[str]:
    truth_state = str(section.get("policy_truth_state") or "blocked")
    labels = ", ".join(list(section.get("policy_labels") or []))
    methodology = str(section.get("methodology_note") or "")
    markers = (
        citation_registry.markers_md(_policy_citations(section))
        if citation_registry is not None
        else ""
    )
    lines = [f"- Policy truth state: {truth_state}.{markers}"]
    if labels:
        lines.append(f"- Policy status: {labels}.")
    if methodology:
        lines.append(f"- Methodology note: {methodology}")
    return lines


def _policy_banner_html(section: dict[str, Any], citation_registry: CitationRegistry | None = None) -> list[str]:
    truth_state = str(section.get("policy_truth_state") or "blocked")
    labels = ", ".join(list(section.get("policy_labels") or []))
    methodology = str(section.get("methodology_note") or "")
    markers = (
        citation_registry.markers_html(_policy_citations(section))
        if citation_registry is not None
        else ""
    )
    lines = [f"<li><strong>Policy truth state:</strong> {escape_html(truth_state)}{markers}</li>"]
    if labels:
        lines.append(f"<li><strong>Policy status:</strong> {escape_html(labels)}</li>")
    if methodology:
        lines.append(f"<li><strong>Methodology note:</strong> {escape_html(methodology)}</li>")
    return lines


def _enriched_item_md(item: dict[str, Any], citation_registry: CitationRegistry) -> str:
    sentence = first_sentence(str(item.get("text", "")))
    why = first_sentence(str(item.get("why_it_matters") or item.get("portfolio_implication") or ""))
    monitor = first_sentence(str(item.get("what_to_monitor_next") or ""))
    consider = first_sentence(str(item.get("what_to_consider") or ""))
    base = emphasize_numbers_markdown(sentence)
    if why:
        base += f" Why it matters: {why}"
    if monitor:
        base += f" Monitor next: {monitor}"
    if consider:
        base += f" What to consider: {consider}"
    return f"- {base}{citation_registry.markers_md(item.get('citations', []))}"


def _enriched_item_html(item: dict[str, Any], citation_registry: CitationRegistry) -> str:
    sentence = first_sentence(str(item.get("text", "")))
    why = first_sentence(str(item.get("why_it_matters") or item.get("portfolio_implication") or ""))
    monitor = first_sentence(str(item.get("what_to_monitor_next") or ""))
    consider = first_sentence(str(item.get("what_to_consider") or ""))
    base = emphasize_numbers_html(sentence)
    if why:
        base += f" <strong>Why it matters:</strong> {escape_html(why)}"
    if monitor:
        base += f" <strong>Monitor next:</strong> {escape_html(monitor)}"
    if consider:
        base += f" <strong>What to consider:</strong> {escape_html(consider)}"
    return f"<li>{base}{citation_registry.markers_html(item.get('citations', []))}</li>"


def _render_policy_pack_markdown(
    policy_pack: dict[str, Any],
    chart_payloads: list[dict[str, Any]],
    citation_registry: CitationRegistry | None = None,
) -> list[str]:
    if not policy_pack:
        return ["## Policy Layer", "- Policy layer was not available in this run.", ""]
    lines = ["## Policy Layer", ""]
    trust_banner = dict(policy_pack.get("trust_banner") or {})
    policy_citation_health = dict(policy_pack.get("policy_citation_health") or {})
    lines.extend(
        [
            "### Trust Boundary",
            f"- Brief trust level: {trust_banner.get('label', 'Market monitoring only')}.",
            (
                "- Guidance status: policy sections are clear for portfolio guidance."
                if bool(trust_banner.get("guidance_ready"))
                else "- Guidance status: market monitoring only until policy sections are sourced and healthy."
            ),
            f"- Policy citation health: {policy_citation_health.get('overall_status', 'unknown')}.",
            "",
        ]
    )
    expected_returns = dict(policy_pack.get("expected_returns") or {})
    lines.extend(
        [
            "### Expected Return Range",
            (
                f"Policy assumption date: {expected_returns.get('assumption_date', 'n/a')} | "
                f"CMA version: {expected_returns.get('version', 'n/a')}"
            ),
        ]
    )
    lines.extend(_policy_banner_md(expected_returns, citation_registry))
    for item in list(expected_returns.get("items") or []):
        lines.append(
            "- "
            + f"{item.get('sleeve_name', item.get('sleeve_key', 'sleeve'))}: "
            + f"{float(item.get('expected_return_min') or 0.0):.1%} to {float(item.get('expected_return_max') or 0.0):.1%}, "
            + f"confidence {item.get('confidence_label', 'n/a')}. "
            + f"{str(item.get('scenario_notes') or '').strip()} "
            + f"Methodology: {str(item.get('methodology_note') or 'n/a')}"
            + (citation_registry.markers_md(item.get("citations", [])) if citation_registry else "")
        )
    if expected_returns.get("caveat"):
        lines.append(f"- Caveat: {expected_returns.get('caveat')}")

    benchmark = dict(policy_pack.get("benchmark") or {})
    lines.extend(["", "### Composite Benchmark"])
    lines.append(
        f"- {benchmark.get('benchmark_name', 'Composite benchmark')} | version {benchmark.get('version', 'n/a')} | assumption date {benchmark.get('assumption_date', 'n/a')}"
    )
    lines.extend(_policy_banner_md(benchmark, citation_registry))
    for item in list(benchmark.get("components") or []):
        lines.append(
            "- "
            + f"{item.get('component_name', item.get('component_key', 'component'))}: "
            + f"{float(item.get('weight') or 0.0):.1%}. {item.get('rationale', '')} "
            + f"Methodology: {str(item.get('methodology_note') or 'n/a')}"
            + (citation_registry.markers_md(item.get("citations", [])) if citation_registry else "")
        )
    if benchmark.get("expected_return_min") is not None:
        lines.append(
            "- "
            + f"Implied benchmark return context: {float(benchmark.get('expected_return_min') or 0.0):.1%} "
            + f"to {float(benchmark.get('expected_return_max') or 0.0):.1%}."
        )

    drawdown = dict(policy_pack.get("aggregate_drawdown") or {})
    lines.extend(["", "### Aggregate Drawdown"])
    lines.extend(_policy_banner_md(drawdown, citation_registry))
    lines.append(
        "- "
        + f"Expected worst-year loss range: {float(drawdown.get('expected_worst_year_loss_min') or 0.0):.1%} "
        + f"to {float(drawdown.get('expected_worst_year_loss_max') or 0.0):.1%}."
    )
    for item in list(drawdown.get("historical_analogs") or [])[:4]:
        lines.append(
            "- "
            + f"{item.get('label', 'Scenario')}: {float(item.get('estimated_impact_pct') or 0.0):+.1f}% "
            + f"estimated impact, confidence {item.get('confidence_rating', 'n/a')}. "
            + f"Methodology source: {item.get('methodology_source_name', 'n/a')}."
        )
    if drawdown.get("caveat"):
        lines.append(f"- Caveat: {drawdown.get('caveat')}")

    rebalancing = dict(policy_pack.get("rebalancing_policy") or {})
    lines.extend(["", "### Rebalancing Policy"])
    lines.append(f"- Calendar review cadence: {rebalancing.get('calendar_review_cadence', 'n/a')}.")
    lines.append(f"- Small trade threshold note: {rebalancing.get('small_trade_threshold', 'n/a')}")
    lines.append(f"- Tax-aware note: {rebalancing.get('tax_aware_note', 'n/a')}")
    for item in list(rebalancing.get("rules") or [])[:8]:
        lines.append(
            "- "
            + f"{str(item.get('sleeve', 'sleeve')).replace('_', ' ').title()}: "
            + f"target {float(item.get('target_weight') or 0.0):.1%}, "
            + f"band {float(item.get('min_band') or 0.0):.1%} to {float(item.get('max_band') or 0.0):.1%}, "
            + f"{item.get('calendar_rebalance_frequency', 'n/a')} review, "
            + f"priority {item.get('rebalance_priority', 'n/a')}."
        )

    ips_snapshot = dict(policy_pack.get("ips_snapshot") or {})
    lines.extend(["", "### IPS Snapshot"])
    lines.append(f"- Objectives: {ips_snapshot.get('objectives', 'n/a')}")
    lines.append(f"- Risk tolerance: {ips_snapshot.get('risk_tolerance', 'n/a')}")
    lines.append(f"- Time horizon: {ips_snapshot.get('time_horizon_years', 'n/a')} years")
    lines.append(f"- Liquidity needs: {ips_snapshot.get('liquidity_needs', 'n/a')}")
    lines.append(f"- Tax context: {ips_snapshot.get('tax_context', 'n/a')}")
    lines.append(f"- Benchmark: {(ips_snapshot.get('benchmark') or {}).get('benchmark_name', 'n/a')}")
    if ips_snapshot.get("caveat"):
        lines.append(f"- Caveat: {ips_snapshot.get('caveat')}")

    core_satellite = list(policy_pack.get("core_satellite_summary") or [])
    if core_satellite:
        lines.extend(["", "### Core and Satellite Summary"])
        for item in core_satellite:
            lines.append(
                "- "
                + f"{item.get('sleeve_name', item.get('sleeve_key', 'sleeve'))}: "
                + f"{item.get('classification', 'n/a')}, target {float(item.get('target_weight') or 0.0):.1%}, "
                + f"band {float(item.get('min_weight') or 0.0):.1%} to {float(item.get('max_weight') or 0.0):.1%}."
            )

    if chart_payloads:
        lines.extend(["", "### Charts"])
        for chart in chart_payloads:
            lines.append(
                "- "
                + f"{chart.get('title', chart.get('chart_key', 'chart'))}: "
                + f"source date {chart.get('source_as_of', 'n/a')}, freshness note {chart.get('freshness_note', 'n/a')}."
            )
    scenario_registry = list(policy_pack.get("scenario_registry") or [])
    scenario_compare = dict(policy_pack.get("scenario_compare") or {})
    if scenario_registry:
        lines.extend(["", "### Scenario Governance"])
        for item in scenario_registry[:6]:
            lines.append(
                "- "
                + f"{item.get('name', item.get('scenario_id', 'scenario'))}: "
                + f"probability {float(item.get('scenario_probability_weight') or 0.0):.0%}, "
                + f"confidence {item.get('confidence_rating', 'n/a')}, "
                + f"last reviewed {item.get('last_reviewed_at', 'n/a')}."
            )
    relevance = list(policy_pack.get("portfolio_relevance") or [])
    if relevance:
        lines.extend(["", "### Portfolio Relevance Bridge"])
        for item in relevance[:10]:
            lines.append(
                "- "
                + f"{item.get('sleeve_tag', 'sleeve')}: {item.get('relevance', 'low relevance')}; "
                + f"affects {item.get('affects', 'benchmark watch only')}; "
                + f"holding weight {_format_optional_pct(float(item.get('holding_weight') or 0.0) * 100.0, precision=1)}."
            )
    for item in list(scenario_compare.get("current_vs_prior_portfolio") or [])[:6]:
        lines.append(
            "- "
            + f"Scenario delta {item.get('name', item.get('scenario_id', 'scenario'))}: "
            + f"current {float(item.get('current_impact_pct') or 0.0):+.1f}%, "
            + f"prior {float(item.get('prior_portfolio_impact_pct') or 0.0):+.1f}%, "
            + f"change {float(item.get('portfolio_drift_impact_pct') or 0.0):+.1f}%."
        )
    lines.append("")
    return lines


def _render_execution_layer_markdown(policy_pack: dict[str, Any]) -> list[str]:
    if not policy_pack:
        return ["## Execution Layer", "- Execution layer was not available in this run.", ""]
    guidance_ready = bool(dict(policy_pack.get("trust_banner") or {}).get("guidance_ready"))
    lines = ["## Execution Layer", ""]
    if not guidance_ready:
        lines.extend(
            [
                "- Execution layer status: reference only.",
                "- Allocation-facing workflow items remain blocked until policy sections are sourced and healthy.",
                "",
            ]
        )
    lines.append("### What To Review Next")
    for item in list(policy_pack.get("review_queue") or []):
        lines.append(f"- [{item.get('action_tag', 'monitor')}] {item.get('item', '')}")

    dca = dict(policy_pack.get("dca_guidance") or {})
    lines.extend(["", "### DCA Module"])
    if not guidance_ready:
        lines.append("- DCA module is displayed as developer-seed reference only, not for allocation decisions.")
    lines.append(
        f"- Cadence {dca.get('cadence', 'n/a')} | routing mode {dca.get('routing_mode', 'n/a')}."
    )
    lines.append(
        "- Neutral conditions: "
        + ", ".join(
            f"{item.get('sleeve_key', 'sleeve')} {float(item.get('weight') or 0.0):.0%}"
            for item in list(dca.get("neutral_conditions") or [])
        )
    )
    lines.append(
        "- Drift conditions: "
        + ", ".join(
            f"{item.get('sleeve_key', 'sleeve')} {float(item.get('weight') or 0.0):.0%}"
            for item in list(dca.get("drift_conditions") or [])
        )
    )
    lines.append(
        "- Stress conditions: "
        + ", ".join(
            f"{item.get('sleeve_key', 'sleeve')} {float(item.get('weight') or 0.0):.0%}"
            for item in list(dca.get("stress_conditions") or [])
        )
    )
    lines.append(f"- Distribution logic: {dca.get('distribution_logic', 'n/a')}")

    fund_selection = list(policy_pack.get("fund_selection") or [])
    lines.extend(["", "### Fund Selection Table"])
    if fund_selection:
        fund_rows = []
        for item in fund_selection[:12]:
            fund_rows.append(
                [
                    str(item.get("ticker", "")),
                    str(item.get("sleeve_name", item.get("sleeve_key", ""))),
                    _format_optional_pct(float(item.get("ter") or 0.0) * 100.0, precision=2),
                    str(item.get("aum", "not available")),
                    str(item.get("liquidity_proxy", "not available")),
                    _format_optional_float(item.get("singapore_tax_efficiency_score"), precision=2),
                    first_sentence(str(item.get("rationale", ""))),
                ]
            )
        lines.extend(
            _build_markdown_table(
                ["Ticker", "Sleeve", "TER", "AUM", "Liquidity", "SG Tax Score", "Rationale"],
                fund_rows,
            )
        )
    else:
        lines.append("- No fund selection rows were available.")

    sub_sleeves = list(policy_pack.get("sub_sleeve_breakdown") or [])
    if sub_sleeves:
        lines.extend(["", "### Sub Sleeve Allocation Breakdown"])
        for item in sub_sleeves[:12]:
            constraints = "; ".join(list(item.get("constraints") or [])[:2])
            lines.append(
                "- "
                + f"{item.get('sleeve_name', item.get('sleeve_key', 'sleeve'))}: {item.get('purpose', '')}"
                + (f" Constraints: {constraints}" if constraints else "")
            )

    tax_guidance = list(policy_pack.get("tax_location_guidance") or [])
    lines.extend(["", "### Asset Location and Tax Guidance"])
    for item in tax_guidance[:12]:
        lines.append(
            "- "
            + f"{item.get('sleeve_name', item.get('sleeve_key', 'sleeve'))}: "
            + f"{item.get('guidance', '')} "
            + f"(preferred account type: {item.get('preferred_account_type', 'n/a')})."
        )
    lines.append("")
    return lines


def _render_policy_pack_html(
    policy_pack: dict[str, Any],
    chart_payloads: list[dict[str, Any]],
    citation_registry: CitationRegistry | None = None,
) -> list[str]:
    if not policy_pack:
        return ["<section id='policy-layer' class='section'><h2>Policy Layer</h2><p>Policy layer was not available in this run.</p></section>"]
    html: list[str] = ["<section id='policy-layer' class='section page-break'>", "<h2>Policy Layer</h2>"]
    trust_banner = dict(policy_pack.get("trust_banner") or {})
    policy_citation_health = dict(policy_pack.get("policy_citation_health") or {})
    html.extend(
        [
            "<h3>Trust Boundary</h3>",
            "<ul class='compact-list'>",
            f"<li><strong>Brief trust level:</strong> {escape_html(str(trust_banner.get('label', 'Market monitoring only')))}</li>",
            (
                "<li><strong>Guidance status:</strong> policy sections are clear for portfolio guidance.</li>"
                if bool(trust_banner.get("guidance_ready"))
                else "<li><strong>Guidance status:</strong> market monitoring only until policy sections are sourced and healthy.</li>"
            ),
            f"<li><strong>Policy citation health:</strong> {escape_html(str(policy_citation_health.get('overall_status', 'unknown')))}</li>",
            "</ul>",
        ]
    )
    expected_returns = dict(policy_pack.get("expected_returns") or {})
    html.extend(
        [
            "<h3>Expected Return Range</h3>",
            (
                "<p class='muted'>"
                + escape_html(
                    f"Policy assumption date: {expected_returns.get('assumption_date', 'n/a')} | "
                    f"CMA version: {expected_returns.get('version', 'n/a')}"
                )
                + "</p>"
            ),
            "<ul class='compact-list'>",
        ]
    )
    html.extend(_policy_banner_html(expected_returns, citation_registry))
    for item in list(expected_returns.get("items") or []):
        html.append(
            "<li>"
            + escape_html(
                f"{item.get('sleeve_name', item.get('sleeve_key', 'sleeve'))}: "
                f"{float(item.get('expected_return_min') or 0.0):.1%} to {float(item.get('expected_return_max') or 0.0):.1%}, "
                f"confidence {item.get('confidence_label', 'n/a')}. {str(item.get('scenario_notes') or '').strip()} "
                f"Methodology: {str(item.get('methodology_note') or 'n/a')}"
            )
            + (citation_registry.markers_html(item.get("citations", [])) if citation_registry else "")
            + "</li>"
        )
    if expected_returns.get("caveat"):
        html.append(f"<li>{escape_html('Caveat: ' + str(expected_returns.get('caveat')))}</li>")
    html.append("</ul>")

    benchmark = dict(policy_pack.get("benchmark") or {})
    html.extend(["<h3>Composite Benchmark</h3>", "<ul class='compact-list'>"])
    html.extend(_policy_banner_html(benchmark, citation_registry))
    html.append(
        "<li>"
        + escape_html(
            f"{benchmark.get('benchmark_name', 'Composite benchmark')} | version {benchmark.get('version', 'n/a')} | assumption date {benchmark.get('assumption_date', 'n/a')}"
        )
        + "</li>"
    )
    for item in list(benchmark.get("components") or []):
        html.append(
            "<li>"
            + escape_html(
                f"{item.get('component_name', item.get('component_key', 'component'))}: "
                f"{float(item.get('weight') or 0.0):.1%}. {item.get('rationale', '')} "
                f"Methodology: {str(item.get('methodology_note') or 'n/a')}"
            )
            + (citation_registry.markers_html(item.get("citations", [])) if citation_registry else "")
            + "</li>"
        )
    if benchmark.get("expected_return_min") is not None:
        html.append(
            "<li>"
            + escape_html(
                f"Implied benchmark return context: {float(benchmark.get('expected_return_min') or 0.0):.1%} "
                f"to {float(benchmark.get('expected_return_max') or 0.0):.1%}."
            )
            + "</li>"
        )
    html.append("</ul>")

    drawdown = dict(policy_pack.get("aggregate_drawdown") or {})
    html.extend(["<h3>Aggregate Drawdown</h3>", "<ul class='compact-list'>"])
    html.extend(_policy_banner_html(drawdown, citation_registry))
    html.append(
        "<li>"
        + escape_html(
            f"Expected worst-year loss range: {float(drawdown.get('expected_worst_year_loss_min') or 0.0):.1%} "
            f"to {float(drawdown.get('expected_worst_year_loss_max') or 0.0):.1%}."
        )
        + "</li>"
    )
    for item in list(drawdown.get("historical_analogs") or [])[:4]:
        html.append(
            "<li>"
            + escape_html(
                f"{item.get('label', 'Scenario')}: {float(item.get('estimated_impact_pct') or 0.0):+.1f}% "
                f"estimated impact, confidence {item.get('confidence_rating', 'n/a')}. "
                f"Methodology source: {item.get('methodology_source_name', 'n/a')}."
            )
            + "</li>"
        )
    if drawdown.get("caveat"):
        html.append(f"<li>{escape_html('Caveat: ' + str(drawdown.get('caveat')))}</li>")
    html.append("</ul>")

    rebalancing = dict(policy_pack.get("rebalancing_policy") or {})
    html.extend(["<h3>Rebalancing Policy</h3>", "<ul class='compact-list'>"])
    html.append(f"<li>{escape_html('Calendar review cadence: ' + str(rebalancing.get('calendar_review_cadence', 'n/a')))}</li>")
    html.append(f"<li>{escape_html('Small trade threshold note: ' + str(rebalancing.get('small_trade_threshold', 'n/a')))}</li>")
    html.append(f"<li>{escape_html('Tax-aware note: ' + str(rebalancing.get('tax_aware_note', 'n/a')))}</li>")
    for item in list(rebalancing.get("rules") or [])[:8]:
        html.append(
            "<li>"
            + escape_html(
                f"{str(item.get('sleeve', 'sleeve')).replace('_', ' ').title()}: "
                f"target {float(item.get('target_weight') or 0.0):.1%}, "
                f"band {float(item.get('min_band') or 0.0):.1%} to {float(item.get('max_band') or 0.0):.1%}, "
                f"{item.get('calendar_rebalance_frequency', 'n/a')} review, "
                f"priority {item.get('rebalance_priority', 'n/a')}."
            )
            + "</li>"
        )
    html.append("</ul>")

    ips_snapshot = dict(policy_pack.get("ips_snapshot") or {})
    html.extend(["<h3>IPS Snapshot</h3>", "<ul class='compact-list'>"])
    for label, value in [
        ("Objectives", ips_snapshot.get("objectives", "n/a")),
        ("Risk tolerance", ips_snapshot.get("risk_tolerance", "n/a")),
        ("Time horizon", f"{ips_snapshot.get('time_horizon_years', 'n/a')} years"),
        ("Liquidity needs", ips_snapshot.get("liquidity_needs", "n/a")),
        ("Tax context", ips_snapshot.get("tax_context", "n/a")),
        ("Benchmark", (ips_snapshot.get("benchmark") or {}).get("benchmark_name", "n/a")),
    ]:
        html.append(f"<li><strong>{escape_html(str(label))}:</strong> {escape_html(str(value))}</li>")
    if ips_snapshot.get("caveat"):
        html.append(f"<li>{escape_html('Caveat: ' + str(ips_snapshot.get('caveat')))}</li>")
    html.append("</ul>")

    core_satellite = list(policy_pack.get("core_satellite_summary") or [])
    if core_satellite:
        html.extend(["<h3>Core and Satellite Summary</h3>", "<table class='data-table compact'><thead><tr><th>Sleeve</th><th>Class</th><th class='num'>Min</th><th class='num'>Target</th><th class='num'>Max</th></tr></thead><tbody>"])
        for item in core_satellite:
            html.append(
                "<tr>"
                f"<td>{escape_html(str(item.get('sleeve_name', item.get('sleeve_key', 'sleeve'))))}</td>"
                f"<td>{escape_html(str(item.get('classification', 'n/a')))}</td>"
                f"<td class='num'>{float(item.get('min_weight') or 0.0):.1%}</td>"
                f"<td class='num'>{float(item.get('target_weight') or 0.0):.1%}</td>"
                f"<td class='num'>{float(item.get('max_weight') or 0.0):.1%}</td>"
                "</tr>"
            )
        html.extend(["</tbody></table>"])

    if chart_payloads:
        html.extend(["<h3>Charts</h3>"])
        for chart in chart_payloads:
            html.extend(
                [
                    "<article class='chart-card'>",
                    f"<h4>{escape_html(str(chart.get('title', chart.get('chart_key', 'chart'))))}</h4>",
                    f"<p class='muted'>Source date: {escape_html(str(chart.get('source_as_of', 'n/a')))} | {escape_html(str(chart.get('freshness_note', 'n/a')))}</p>",
                    str(chart.get("svg") or ""),
                    "</article>",
                ]
            )
    scenario_registry = list(policy_pack.get("scenario_registry") or [])
    scenario_compare = dict(policy_pack.get("scenario_compare") or {})
    if scenario_registry:
        html.extend(["<h3>Scenario Governance</h3>", "<ul class='compact-list'>"])
        for item in scenario_registry[:6]:
            html.append(
                "<li>"
                + escape_html(
                    f"{item.get('name', item.get('scenario_id', 'scenario'))}: "
                    f"probability {float(item.get('scenario_probability_weight') or 0.0):.0%}, "
                    f"confidence {item.get('confidence_rating', 'n/a')}, "
                    f"last reviewed {item.get('last_reviewed_at', 'n/a')}."
                )
                + "</li>"
            )
        for item in list(scenario_compare.get("current_vs_prior_portfolio") or [])[:6]:
            html.append(
                "<li>"
                + escape_html(
                    f"Scenario delta {item.get('name', item.get('scenario_id', 'scenario'))}: "
                    f"current {float(item.get('current_impact_pct') or 0.0):+.1f}%, "
                    f"prior {float(item.get('prior_portfolio_impact_pct') or 0.0):+.1f}%, "
                    f"change {float(item.get('portfolio_drift_impact_pct') or 0.0):+.1f}%."
                )
                + "</li>"
            )
        html.append("</ul>")
    relevance = list(policy_pack.get("portfolio_relevance") or [])
    if relevance:
        html.extend(["<h3>Portfolio Relevance Bridge</h3>", "<ul class='compact-list'>"])
        for item in relevance[:10]:
            html.append(
                "<li>"
                + escape_html(
                    f"{item.get('sleeve_tag', 'sleeve')}: {item.get('relevance', 'low relevance')}; "
                    f"affects {item.get('affects', 'benchmark watch only')}; "
                    f"holding weight {_format_optional_pct(float(item.get('holding_weight') or 0.0) * 100.0, precision=1)}."
                )
                + "</li>"
            )
        html.append("</ul>")
    html.extend(["</section>"])
    return html


def _render_execution_layer_html(policy_pack: dict[str, Any]) -> list[str]:
    if not policy_pack:
        return ["<section id='execution-layer' class='section'><h2>Execution Layer</h2><p>Execution layer was not available in this run.</p></section>"]
    guidance_ready = bool(dict(policy_pack.get("trust_banner") or {}).get("guidance_ready"))
    html = ["<section id='execution-layer' class='section page-break'>", "<h2>Execution Layer</h2>"]
    if not guidance_ready:
        html.extend(
            [
                "<p class='muted'>Execution layer is reference only until policy sections are sourced and healthy.</p>",
            ]
        )
    html.extend(["<h3>What To Review Next</h3>", "<ul class='compact-list'>"])
    for item in list(policy_pack.get("review_queue") or []):
        html.append(
            "<li>"
            + escape_html(f"[{item.get('action_tag', 'monitor')}] {item.get('item', '')}")
            + "</li>"
        )
    html.append("</ul>")

    dca = dict(policy_pack.get("dca_guidance") or {})
    html.extend(["<h3>DCA Module</h3>", "<ul class='compact-list'>"])
    if not guidance_ready:
        html.append("<li>DCA module is displayed as developer-seed reference only, not for allocation decisions.</li>")
    cadence_line = f"Cadence {dca.get('cadence', 'n/a')} | routing mode {dca.get('routing_mode', 'n/a')}."
    html.append(f"<li>{escape_html(cadence_line)}</li>")
    html.append(
        "<li>"
        + escape_html(
            "Neutral conditions: "
            + ", ".join(
                f"{item.get('sleeve_key', 'sleeve')} {float(item.get('weight') or 0.0):.0%}"
                for item in list(dca.get("neutral_conditions") or [])
            )
        )
        + "</li>"
    )
    html.append(
        "<li>"
        + escape_html(
            "Drift conditions: "
            + ", ".join(
                f"{item.get('sleeve_key', 'sleeve')} {float(item.get('weight') or 0.0):.0%}"
                for item in list(dca.get("drift_conditions") or [])
            )
        )
        + "</li>"
    )
    html.append(
        "<li>"
        + escape_html(
            "Stress conditions: "
            + ", ".join(
                f"{item.get('sleeve_key', 'sleeve')} {float(item.get('weight') or 0.0):.0%}"
                for item in list(dca.get("stress_conditions") or [])
            )
        )
        + "</li>"
    )
    html.append(f"<li>{escape_html('Distribution logic: ' + str(dca.get('distribution_logic', 'n/a')))}</li>")
    html.append("</ul>")

    fund_selection = list(policy_pack.get("fund_selection") or [])
    html.extend(["<h3>Fund Selection Table</h3>"])
    if fund_selection:
        html.extend(
            [
                "<table class='data-table compact'><thead><tr><th>Ticker</th><th>Sleeve</th><th class='num'>TER</th><th>AUM</th><th>Liquidity</th><th class='num'>SG Tax Score</th><th>Rationale</th></tr></thead><tbody>",
            ]
        )
        for item in fund_selection[:12]:
            html.append(
                "<tr>"
                f"<td>{escape_html(str(item.get('ticker', '')))}</td>"
                f"<td>{escape_html(str(item.get('sleeve_name', item.get('sleeve_key', ''))))}</td>"
                f"<td class='num'>{_format_optional_pct(float(item.get('ter') or 0.0) * 100.0, precision=2)}</td>"
                f"<td>{escape_html(str(item.get('aum', 'not available')))}</td>"
                f"<td>{escape_html(str(item.get('liquidity_proxy', 'not available')))}</td>"
                f"<td class='num'>{_format_optional_float(item.get('singapore_tax_efficiency_score'), precision=2)}</td>"
                f"<td>{escape_html(first_sentence(str(item.get('rationale', ''))))}</td>"
                "</tr>"
            )
        html.extend(["</tbody></table>"])
    else:
        html.append("<p>No fund selection rows were available.</p>")

    sub_sleeves = list(policy_pack.get("sub_sleeve_breakdown") or [])
    if sub_sleeves:
        html.extend(["<h3>Sub Sleeve Allocation Breakdown</h3>", "<ul class='compact-list'>"])
        for item in sub_sleeves[:12]:
            constraints = "; ".join(list(item.get("constraints") or [])[:2])
            message = (
                f"{item.get('sleeve_name', item.get('sleeve_key', 'sleeve'))}: {item.get('purpose', '')}"
                + (f" Constraints: {constraints}" if constraints else "")
            )
            html.append(f"<li>{escape_html(message)}</li>")
        html.append("</ul>")

    tax_guidance = list(policy_pack.get("tax_location_guidance") or [])
    html.extend(["<h3>Asset Location and Tax Guidance</h3>", "<ul class='compact-list'>"])
    for item in tax_guidance[:12]:
        html.append(
            "<li>"
            + escape_html(
                f"{item.get('sleeve_name', item.get('sleeve_key', 'sleeve'))}: {item.get('guidance', '')} "
                f"(preferred account type: {item.get('preferred_account_type', 'n/a')})."
            )
            + "</li>"
        )
    html.extend(["</ul>", "</section>"])
    return html


def _normalize_brief_mode(value: str | None) -> str:
    normalized = str(value or "daily").strip().lower()
    return normalized if normalized in {"daily", "weekly", "monthly"} else "daily"


def _normalize_audience_preset(value: str | None) -> str:
    normalized = str(value or "pm").strip().lower()
    if normalized in {"client", "client_friendly"}:
        return "client_friendly"
    if normalized in {"internal", "internal_diagnostic"}:
        return "internal_diagnostic"
    return "pm"


def _validate_rendered_report(markdown: str, html: str, citation_count: int) -> None:
    if citation_count <= 0:
        raise ReportBuildError("Citation list is empty after rendering")

    required_html_sections = [
        "id='top-sheet'",
        "id='executive-snapshot'",
        "id='what-changed'",
        "id='policy-layer'",
        "id='execution-layer'",
        "id='evidence-layer'",
        "id='data-graphs'",
        "id='long-horizon-context'",
        "id='multi-perspective'",
        "id='alerts-timeline'",
        "id='opportunity-observations'",
        "id='big-players'",
        "id='portfolio-mapping'",
        "id='implementation-layer'",
        "id='source-appendix'",
        "id='sources'",
    ]
    for marker in required_html_sections:
        if marker not in html:
            raise ReportBuildError(f"HTML output missing required section marker: {marker}")

    start = html.find("id='executive-snapshot'")
    if start >= 0:
        end = html.find("</section>", start)
        snippet = html[start:end if end > start else len(html)]
        snippet = re.sub(r"href='#cite-\d+'", "", snippet)
        if re.search(r"https?://", snippet):
            raise ReportBuildError("Raw URL detected inside executive snapshot section")

    md_start = markdown.find("## Executive Snapshot")
    md_end = markdown.find("## What Changed Since Last Report", md_start)
    if md_start >= 0 and md_end > md_start:
        section = markdown[md_start:md_end]
        if re.search(r"https?://", section):
            raise ReportBuildError("Raw URL detected inside markdown executive snapshot section")


def build_narrated_email_brief(
    subject: str,
    generated_at_sgt: str,
    executive_snapshot: list[dict[str, Any]],
    graph_rows: list[dict[str, Any]],
    graph_quality_audit: dict[str, Any] | None,
    lenses: list[dict[str, Any]],
    big_players: list[dict[str, Any]],
    portfolio_mapping: list[dict[str, Any]],
    convex_report: dict[str, Any],
    source_appendix: list[SourceRecord],
    allocation: dict[str, float] | None = None,
    long_state: str | None = None,
    short_state: str | None = None,
    executive_policy_context: list[dict[str, Any]] | None = None,
    executive_monitoring_now: list[dict[str, Any]] | None = None,
    what_changed: list[dict[str, Any]] | None = None,
    long_horizon_context: list[dict[str, Any]] | None = None,
    alerts_timeline: list[dict[str, Any]] | None = None,
    opportunities: list[dict[str, Any]] | None = None,
    implementation_mapping: dict[str, Any] | None = None,
    data_recency_summary: dict[str, Any] | None = None,
    mcp_updates: dict[str, Any] | None = None,
    source_data_asof: dict[str, str] | None = None,
    policy_pack: dict[str, Any] | None = None,
    chart_payloads: list[dict[str, Any]] | None = None,
    approval_record: dict[str, Any] | None = None,
    brief_mode: str | None = None,
    audience_preset: str | None = None,
) -> dict[str, str]:
    policy_items = executive_policy_context or executive_snapshot[: max(1, len(executive_snapshot) // 2)]
    monitoring_items = executive_monitoring_now or executive_snapshot[max(1, len(executive_snapshot) // 2) :]
    if not monitoring_items:
        monitoring_items = executive_snapshot[:]
    what_changed_items = what_changed or []
    long_context_items = long_horizon_context or []
    alert_items = alerts_timeline or []
    opportunity_items = opportunities or []
    implementation_payload = implementation_mapping or {}
    recency_summary = data_recency_summary or {}
    mcp_updates_payload = mcp_updates or {}
    source_data_asof_map = source_data_asof or {}
    policy_pack_payload = policy_pack or {}
    chart_payload_list = chart_payloads or []
    approval_payload = approval_record or {}
    brief_mode_value = _normalize_brief_mode(brief_mode)
    audience_value = _normalize_audience_preset(audience_preset)
    include_mcp_details = audience_value != "client_friendly"
    include_internal_diagnostics = audience_value == "internal_diagnostic"
    include_full_implementation_layer = audience_value != "client_friendly"
    changed_heading = {
        "daily": "## What Changed Since Last Report",
        "weekly": "## What Changed This Week",
        "monthly": "## What Changed This Month",
    }[brief_mode_value]

    for item in [*policy_items, *monitoring_items]:
        validate_section_citations("Executive Snapshot", item.get("citations", []), actionable=True)
    for row in graph_rows:
        validate_section_citations("Data Graphs", [row.get("citation")] if row.get("citation") else [], actionable=False)
    if graph_quality_audit:
        validate_section_citations(
            "Graph Quality Evaluation",
            graph_quality_audit.get("citations", []),
            actionable=False,
        )
    for item in what_changed_items:
        validate_section_citations("What Changed Since Last Report", item.get("citations", []), actionable=False)
    for item in long_context_items:
        validate_section_citations("Long Horizon Context", item.get("citations", []), actionable=False)
    for lens in lenses:
        validate_section_citations("Multi Perspective Interpretation", lens.get("citations", []), actionable=True)
        for monitor in lens.get("near_term_monitor", [])[:2]:
            validate_section_citations("Multi Perspective Near Term Monitor", monitor.get("citations", []), actionable=False)
    for alert in alert_items:
        validate_section_citations("Alerts Timeline", alert.get("citations", []), actionable=False)
    for opportunity in opportunity_items:
        validate_section_citations("Opportunity Observations", opportunity.get("citations", []), actionable=False)
    for item in mcp_updates_payload.get("new_items", []):
        validate_section_citations("MCP Updates - New Items", item.get("citations", []), actionable=False)
    for item in mcp_updates_payload.get("changed_items", []):
        validate_section_citations("MCP Updates - Changed Items", item.get("citations", []), actionable=False)
    for player in big_players:
        validate_section_citations("Big Players Activity", player.get("citations", []), actionable=False)
    for mapping in portfolio_mapping:
        validate_section_citations("Portfolio Mapping", mapping.get("citations", []), actionable=True)
    for payload in implementation_payload.get("sleeves", {}).values():
        for candidate in payload.get("candidates", []):
            validate_section_citations(
                "Implementation Mapping – Candidate Instruments",
                candidate.citations,
                actionable=False,
            )
        for item in payload.get("sg_tax_observations", []):
            validate_section_citations(
                "Implementation Mapping – SG Tax Implementation Observations",
                item.get("citations", []),
                actionable=False,
            )
    for item in implementation_payload.get("watchlist_candidates", []):
        validate_section_citations(
            "Implementation Mapping – Watchlist Candidates",
            item.get("citations", []),
            actionable=False,
        )

    if not source_appendix:
        raise ReportBuildError("Source appendix is empty; aborting report generation")

    citation_registry = CitationRegistry(source_appendix)

    executive_items = [*policy_items[:5], *monitoring_items[:5]]
    executive_citations: list[Citation] = []
    for item in executive_items:
        executive_citations.extend(item.get("citations", []))
    cached_count, live_count = _extract_exec_freshness(executive_citations)

    md_lines = [
        "# Investment Agent: Institutional Daily Brief",
        "",
        f"**Subject:** {subject}",
        f"**Generated (SGT):** {generated_at_sgt}",
        f"**Signal states:** Long horizon = {long_state or 'n/a'} | Short horizon = {short_state or 'n/a'}",
        f"**Cadence / Audience:** {brief_mode_value} / {audience_value}",
        f"*{PERSISTENT_DISCLAIMER}*",
        "",
        "## Top Sheet",
        "",
        f"- Approval state: {approval_payload.get('approval_status', 'generated')}",
        f"- Brief mode: {brief_mode_value}",
        f"- Audience preset: {audience_value}",
        "",
        "## Executive Snapshot",
        "### Policy context (long horizon)",
    ]
    for item in policy_items[:5]:
        md_lines.append(_enriched_item_md(item, citation_registry))
    md_lines.append("")
    md_lines.append("### Monitoring and opportunities (short horizon)")
    for item in monitoring_items[:5]:
        md_lines.append(_enriched_item_md(item, citation_registry))

    freshness_line = (
        f"_Data freshness: {live_count} live and {cached_count} cached citations in this snapshot"
        f"{citation_registry.markers_md(executive_citations[:2])}._"
    )
    md_lines.extend(["", freshness_line, "", changed_heading])
    if what_changed_items:
        for item in what_changed_items:
            md_lines.append(_enriched_item_md(item, citation_registry))
    else:
        md_lines.append("- No prior report comparison was available in this run.")

    if include_mcp_details:
        md_lines.extend(
            [
                "",
                "## MCP Updates",
                (
                    "- MCP coverage summary: "
                    f"connectable {int(mcp_updates_payload.get('coverage', {}).get('connectable', 0))}, "
                    f"live successes {int(mcp_updates_payload.get('coverage', {}).get('live_successes', 0))}, "
                    f"success ratio {float(mcp_updates_payload.get('coverage', {}).get('success_ratio', 0.0)):.2f}"
                ),
            ]
        )
        new_mcp_items = list(mcp_updates_payload.get("new_items", []))
        changed_mcp_items = list(mcp_updates_payload.get("changed_items", []))
        if not new_mcp_items and not changed_mcp_items:
            md_lines.append("- No new MCP items since prior run")
        else:
            if new_mcp_items:
                md_lines.append("- New MCP items today:")
                for item in new_mcp_items[:10]:
                    published_at = str(item.get("published_at") or "publication date unavailable")
                    md_lines.append(
                        "- "
                        + f"{item.get('server_id', 'unknown_server')}: {first_sentence(str(item.get('title', 'untitled MCP item')))} "
                        + f"(published_at: {published_at}, retrieved_at: {item.get('retrieved_at', 'n/a')})"
                        + citation_registry.markers_md(item.get("citations", []))
                    )
            if changed_mcp_items:
                md_lines.append("- Changed MCP items today:")
                for item in changed_mcp_items[:5]:
                    published_at = str(item.get("published_at") or "publication date unavailable")
                    md_lines.append(
                        "- "
                        + f"{item.get('server_id', 'unknown_server')}: {first_sentence(str(item.get('title', 'untitled MCP item')))} "
                        + f"(published_at: {published_at}, retrieved_at: {item.get('retrieved_at', 'n/a')})"
                        + citation_registry.markers_md(item.get("citations", []))
                    )

    md_lines.extend(["", *_render_policy_pack_markdown(policy_pack_payload, chart_payload_list, citation_registry)])
    md_lines.extend(_render_execution_layer_markdown(policy_pack_payload))
    md_lines.extend(["## Evidence Layer", ""])
    if include_internal_diagnostics:
        md_lines.extend(
            [
                "### Internal Diagnostics",
                f"- Citation count: {citation_registry.citation_count}",
                f"- Cached citations in executive section: {cached_count}",
                "",
            ]
        )
    md_lines.extend(["## Data Graphs"])
    if recency_summary:
        counts = recency_summary.get("counts", {})
        recency_citations = [row.get("citation") for row in graph_rows if row.get("citation") is not None][:2]
        md_lines.extend(
            [
                "",
                "### Data Recency Summary",
                (
                    f"- fresh: {int(counts.get('fresh', 0))} | "
                    f"lagged: {int(counts.get('lagged', 0))} | "
                    f"stale: {int(counts.get('stale', 0))}"
                    f"{citation_registry.markers_md(recency_citations)}"
                ),
            ]
        )
        top_lagged = recency_summary.get("top_lagged_metrics", [])
        if top_lagged:
            md_lines.append("- Top lagged metrics:")
            for item in top_lagged[:5]:
                md_lines.append(
                    f"  - {item.get('metric', 'metric')} | observed_at {item.get('observed_at', 'observed_at unavailable')} | "
                    f"lag {item.get('lag_days', 'n/a')} day(s) | source {item.get('source_id', 'unknown_source')}"
                )
        non_compliant = recency_summary.get("non_compliant_metrics", [])
        for metric_name in non_compliant:
            md_lines.append(f"- {metric_name}: observed_at unavailable")

    graph_headers = [
        "Metric",
        "As of",
        "Lag",
        "Layer",
        "Latest",
        "1y Chg",
        "3y Trend (w)",
        "5y Vol",
        "5y pct",
        "10y pct",
        "10y Range",
        "Regime",
        "5obs",
        "20obs Mom",
        "60d Trend",
        "60d pct",
        "60d Range",
        "Dir Tag",
    ]
    graph_rows_md = []
    for row in graph_rows:
        citation = row.get("citation")
        marker = citation_registry.markers_md([citation]) if citation else ""
        if "long_horizon" in row and "short_horizon" in row:
            long = row.get("long_horizon", {})
            short = row.get("short_horizon", {})
            graph_rows_md.append(
                [
                    f"{row['metric']}{marker} ({row.get('daily_change_cue', '1d delta: n/a')})".strip(),
                    str(row.get("as_of", "observed_at unavailable")),
                    (
                        f"{row.get('lag_days')} day(s) [{row.get('lag_class')}]"
                        if row.get("lag_days") is not None
                        else "observed_at unavailable"
                    ),
                    "long",
                    _format_optional_float(long.get("latest")),
                    _format_optional_float(long.get("change_1y")),
                    str(long.get("sparkline_3y_weekly", "")),
                    _format_optional_pct(
                        float(long.get("rolling_vol_5y")) * 100.0 if long.get("rolling_vol_5y") is not None else None
                    ),
                    _format_optional_pct(long.get("percentile_5y")),
                    (
                        _format_optional_pct(long.get("percentile_10y"))
                        if long.get("percentile_10y") is not None
                        else str(long.get("ten_year_note", "n/a"))
                    ),
                    f"{long.get('range_bar_10y', '')} { _format_optional_pct(long.get('percentile_10y') or long.get('percentile_5y')) }",
                    str(long.get("regime_classification", "")),
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                ]
            )
            graph_rows_md.append(
                [
                    "",
                    "",
                    "",
                    "short",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "",
                    _format_optional_float(short.get("change_5obs")),
                    _format_optional_float(short.get("momentum_20obs")),
                    str(short.get("sparkline_60d", "")),
                    _format_optional_pct(short.get("percentile_60d")),
                    str(short.get("range_bar_60d", "")),
                    str(short.get("direction_tag", "")),
                ]
            )
        else:
            metric = f"{row['metric']}{marker}".strip()
            graph_rows_md.append(
                [
                    f"{metric} ({row.get('daily_change_cue', '1d delta: n/a')})",
                    str(row.get("as_of", "observed_at unavailable")),
                    (
                        f"{row.get('lag_days')} day(s) [{row.get('lag_class')}]"
                        if row.get("lag_days") is not None
                        else "observed_at unavailable"
                    ),
                    "legacy",
                    str(row.get("latest", "")),
                    "",
                    "",
                    "",
                    "",
                    "",
                    _render_range_display(row),
                    str(row.get("trajectory", "")),
                    str(row.get("delta_5", "")),
                    "",
                    str(row.get("sparkline", "")),
                    "",
                    str(row.get("range_bar", "")),
                    str(row.get("pattern", "")),
                ]
            )
    md_lines.extend(_build_markdown_table(graph_headers, graph_rows_md))
    md_lines.extend(
        [
            "",
            "_Legend: long horizon uses 1y/3y/5y/10y context for policy monitoring; short horizon uses 5obs/20obs/60d diagnostics for monitoring and opportunities._",
            "",
            "### Graph Quality",
        ]
    )
    if graph_quality_audit:
        md_lines.extend(
            [
                f"- Up to date: {graph_quality_audit.get('up_to_date')}",
                f"- Retrieval fresh (<=48h): {graph_quality_audit.get('retrieval_fresh')}",
                f"- Data fresh (<=7d): {graph_quality_audit.get('data_fresh')}",
                f"- Cited: {graph_quality_audit.get('cited')}",
                f"- Easy to comprehend: {graph_quality_audit.get('easy_to_comprehend')}",
                f"- Note: {graph_quality_audit.get('note', '')}{citation_registry.markers_md(graph_quality_audit.get('citations', []))}",
            ]
        )

    md_lines.extend(["", "## Long Horizon Context"])
    if long_context_items:
        for item in long_context_items:
            sentence = first_sentence(str(item.get("text", "")))
            md_lines.append(
                f"- {emphasize_numbers_markdown(sentence)}{citation_registry.markers_md(item.get('citations', []))}"
            )
    else:
        md_lines.append("- Long-horizon explanatory context was not provided in this run.")

    md_lines.extend(["", "## Multi Perspective Interpretation"])
    for lens in lenses:
        citations = lens.get("citations", [])
        body = str(lens.get("body", "")).strip()
        emphasis = str(lens.get("emphasis", "")) or first_sentence(body)
        takeaways = list(lens.get("key_takeaways", [])) or split_sentences(body)[:3]
        changes = list(lens.get("what_changes_view", [])) or _default_view_change_triggers(str(lens.get("title", "")))
        takeaways = [first_sentence(item) for item in takeaways[:3] if item]
        changes = [first_sentence(item) for item in changes[:2] if item]
        md_lines.extend(
            [
                "",
                f"### {lens['title']}{citation_registry.markers_md(citations)}",
                f"Source: {lens.get('source_title', 'Source metadata not provided')}, "
                + (
                    str(lens.get("source_date"))
                    if lens.get("source_date_available", False) and lens.get("source_date")
                    else "Publication date not available in source metadata."
                ),
            ]
        )
        quote = lens.get("quote")
        if quote:
            md_lines.append("")
            md_lines.append("Quoted line:")
            md_lines.append(f"> \"{quote}\"")
        md_lines.extend(
            [
                "",
                "Interpretation:",
                f"*What this lens emphasizes:* {emphasize_numbers_markdown(emphasis)}",
                "Key takeaways:",
            ]
        )
        md_lines.extend(f"- {emphasize_numbers_markdown(item)}" for item in takeaways)
        md_lines.append("What would change this view:")
        md_lines.extend(f"- {emphasize_numbers_markdown(item)}" for item in changes)
        near_term = lens.get("near_term_monitor", [])[:2]
        if near_term:
            md_lines.append("Near term monitor:")
            for item in near_term:
                sentence = first_sentence(str(item.get("text", "")))
                md_lines.append(
                    f"- {emphasize_numbers_markdown(sentence)}{citation_registry.markers_md(item.get('citations', []))}"
                )

    md_lines.extend(["", "## Alerts Timeline"])
    if not alert_items:
        md_lines.append("- No alert events were generated in this run.")
    else:
        alert_rows = []
        for item in alert_items:
            alert_rows.append(
                [
                    str(item.get("severity", "info")).upper(),
                    first_sentence(str(item.get("trigger_reason", "")))
                    + citation_registry.markers_md(item.get("citations", [])),
                    first_sentence(str(item.get("what_moved", ""))),
                    first_sentence(str(item.get("why_it_matters", ""))),
                    first_sentence(str(item.get("what_would_neutralize", ""))),
                    first_sentence(str(item.get("what_to_monitor_next", ""))),
                    first_sentence(str(item.get("what_to_consider", item.get("action_tag", "")))),
                ]
            )
        md_lines.extend(
            _build_markdown_table(
                ["Severity", "Trigger reason", "What moved", "Why it matters", "What would neutralize it", "Monitor next", "What to consider"],
                alert_rows,
            )
        )

    md_lines.extend(["", "## Opportunity Observations"])
    if not opportunity_items:
        md_lines.append("- No opportunity observations met citation and confidence gates in this run.")
    else:
        opp_rows = []
        for idx, item in enumerate(opportunity_items, start=1):
            opp_rows.append(
                [
                    str(idx),
                    first_sentence(str(item.get("condition_observed", "")))
                    + citation_registry.markers_md(item.get("citations", [])),
                    first_sentence(str(item.get("confirmation_data", ""))),
                    str(item.get("time_horizon", "")),
                    _format_optional_pct(float(item.get("confidence", 0.0)) * 100.0),
                    first_sentence(str(item.get("what_to_monitor_next", ""))),
                ]
            )
        md_lines.extend(
            _build_markdown_table(
                ["Rank", "Condition observed", "What data would confirm", "Horizon", "Observation confidence", "Monitor next"],
                opp_rows,
            )
        )

    md_lines.extend(["", "## Big Players Activity"])
    if not big_players:
        md_lines.append("- No reliably sourced large player items in this run.")
    else:
        player_rows = []
        for item in big_players:
            left, right = split_item_and_why(str(item.get("text", "")))
            player_rows.append(
                [
                    emphasize_numbers_markdown(left) + citation_registry.markers_md(item.get("citations", [])),
                    emphasize_numbers_markdown(right),
                ]
            )
        md_lines.extend(_build_markdown_table(["Item", "Why it matters"], player_rows))

    md_lines.extend(["", "## Portfolio Mapping to SGD 1M Policy"])
    policy_alloc = allocation or {}
    if policy_alloc:
        allocation_rows = [
            [sleeve.replace("_", " ").title(), f"{weight * 100:.1f}%"]
            for sleeve, weight in policy_alloc.items()
        ]
        md_lines.extend(_build_markdown_table(["Policy Sleeve", "Target Weight"], allocation_rows))
    md_lines.extend(
        [
            "",
            "> Holdings not provided, drift and rebalance bands not evaluated.",
            "",
        ]
    )
    for mapping in portfolio_mapping:
        md_lines.append(
            f"- {emphasize_numbers_markdown(first_sentence(mapping['text']))}{citation_registry.markers_md(mapping.get('citations', []))}"
        )

    status = "PASS" if convex_report.get("valid") else "FAIL"
    md_lines.append("")
    md_lines.append(f"Convex compliance status: **{status}**")

    target_breakdown = convex_report.get("target_breakdown", [])
    if target_breakdown:
        mini_rows = []
        for item in target_breakdown:
            check = "OK" if item.get("within_target", False) else "Review"
            mini_rows.append(
                [
                    str(item.get("component", "")),
                    f"{float(item.get('target', 0.0)) * 100:.1f}%",
                    f"{float(item.get('actual', 0.0)) * 100:.1f}%",
                    check,
                ]
            )
        md_lines.extend(_build_markdown_table(["Convex Component", "Target", "Actual", "Check"], mini_rows))

    md_lines.extend(
        [
            f"- Margin required anywhere in convex sleeve: {yes_no(convex_report.get('margin_required_any', False))}",
            f"- Max loss known across convex sleeve: {yes_no(convex_report.get('max_loss_known_all', False))}",
            f"- Convex notes: {'; '.join(convex_report.get('errors', [])) if convex_report.get('errors') else 'Policy compliant'}",
            "",
            "## Implementation Layer – Illustrative Products",
        ]
    )

    disclaimer = implementation_payload.get("disclaimer")
    if disclaimer:
        md_lines.append(f"*{disclaimer}*")
    if not include_full_implementation_layer:
        md_lines.append("- Client-friendly preset: full implementation tables are suppressed in favor of the execution-layer summary.")
    else:
        sleeve_order = ["global_equity", "ig_bonds", "real_assets", "alternatives", "convex"]
        for sleeve_key in sleeve_order:
            payload = implementation_payload.get("sleeves", {}).get(sleeve_key, {})
            candidates = payload.get("candidates", [])
            if not candidates:
                continue
            md_lines.extend(["", f"### {payload.get('title', sleeve_key.replace('_', ' ').title())}"])
            md_rows = []
            for candidate in candidates:
                md_rows.append(
                    [
                        candidate.symbol + citation_registry.markers_md(candidate.citations),
                        candidate.name,
                        candidate.domicile,
                        _format_optional_pct(candidate.expense_ratio * 100.0, precision=2),
                        _format_optional_float(candidate.tax_score, precision=2),
                        yes_no(candidate.us_situs_risk_flag),
                        _format_optional_float(candidate.liquidity_score, precision=2),
                        _format_optional_pct(candidate.withholding_rate * 100.0, precision=1),
                        _format_optional_float(candidate.yield_proxy, precision=2),
                        _format_optional_float(candidate.duration_years, precision=2),
                        _minute_stamp(candidate.retrieved_at),
                    ]
                )
            md_lines.extend(
                _build_markdown_table(
                    [
                        "Symbol",
                        "Instrument",
                        "Domicile",
                        "Expense",
                        "SG tax score",
                        "US situs risk",
                        "Liquidity",
                        "Withholding",
                        "Yield proxy",
                        "Duration",
                        "Retrieved",
                    ],
                    md_rows,
                )
            )

            if sleeve_key == "global_equity":
                md_lines.append("")
                md_lines.append("SG Tax Implementation Observations:")
                for item in payload.get("sg_tax_observations", []):
                    md_lines.append(
                        f"- {first_sentence(str(item.get('text', '')))}{citation_registry.markers_md(item.get('citations', []))}"
                    )

            if sleeve_key == "convex":
                md_lines.append("")
                md_lines.append("Convex option implementation constraints:")
                for candidate in candidates:
                    if candidate.option_position is None:
                        continue
                    md_lines.append(
                        "- "
                        + f"{candidate.symbol}: option_position={candidate.option_position}, strike={_format_optional_float(candidate.strike, precision=2)}, "
                        + f"expiry={candidate.expiry or 'n/a'}, premium_pct_nav={_format_optional_pct((candidate.premium_paid_pct_nav or 0.0) * 100.0, precision=2)}, "
                        + f"annualized_carry={_format_optional_pct((candidate.annualized_carry_estimate or 0.0) * 100.0, precision=2)}, "
                        + f"margin_required={yes_no(candidate.margin_required)}, max_loss_known={yes_no(candidate.max_loss_known)}"
                        + citation_registry.markers_md(candidate.citations)
                    )

        watchlist_candidates = implementation_payload.get("watchlist_candidates", [])
        if watchlist_candidates:
            md_lines.extend(["", "### Implementation Watchlist Candidates"])
            watch_rows = []
            for item in watchlist_candidates:
                watch_rows.append(
                    [
                        str(item.get("symbol", "")) + citation_registry.markers_md(item.get("citations", [])),
                        first_sentence(str(item.get("condition", ""))),
                        str(item.get("time_horizon", "")),
                        _format_optional_float(item.get("tax_score"), precision=2),
                        _format_optional_float(item.get("liquidity_score"), precision=2),
                    ]
                )
            md_lines.extend(
                _build_markdown_table(
                    ["Symbol", "Condition observed", "Horizon", "SG tax score", "Liquidity score"],
                    watch_rows,
                )
            )

    md_lines.extend(
        [
            "",
            "## Source Appendix",
        ]
    )

    grouped_sources = _group_sources(source_appendix)
    for tier in ["primary", "secondary", "tertiary"]:
        rows = grouped_sources.get(tier, [])
        if not rows:
            continue
        md_lines.extend(["", f"### {tier.title()} Sources"])
        table_rows: list[list[str]] = []
        for source in rows:
            note = important_source_note(source.source_id)
            data_as_of = source_data_asof_map.get(source.source_id, "")
            if not data_as_of and source.published_at:
                data_as_of = source.published_at.date().isoformat()
            table_rows.append(
                [
                    source.publisher,
                    _minute_stamp(source.retrieved_at),
                    data_as_of or "",
                    note,
                    f"[Link]({source.url})",
                ]
            )
        md_lines.extend(
            _build_markdown_table(
                ["Publisher", "Retrieved At (UTC)", "Data as of", "Importance note", "Link"],
                table_rows,
            )
        )

    md_lines.extend(["", "## Sources"])
    md_lines.extend(citation_registry.markdown_footnotes())
    markdown = "\n".join(md_lines) + "\n"

    css = _load_template(
        CSS_TEMPLATE_PATH,
        """
        body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 20px; color: #17212b; }
        .container { max-width: 860px; margin: 0 auto; }
        table { width: 100%; border-collapse: collapse; }
        th, td { border: 1px solid #d9e3ec; padding: 6px; vertical-align: top; }
        """,
    )
    html_template = _load_template(
        HTML_TEMPLATE_PATH,
        "<!doctype html><html><head><meta charset='utf-8'><style>{{styles}}</style></head>"
        "<body><main class='container'>{{body}}</main></body></html>",
    )

    html_body: list[str] = [
        "<header class='report-header'>",
        "<h1>Investment Agent: Institutional Daily Brief</h1>",
        f"<p class='subject'><strong>Subject:</strong> {escape_html(subject)}</p>",
        f"<p class='meta'><strong>Generated (SGT):</strong> {escape_html(generated_at_sgt)}</p>",
        f"<p class='meta'><strong>Signal states:</strong> Long horizon = {escape_html(long_state or 'n/a')} | Short horizon = {escape_html(short_state or 'n/a')}</p>",
        f"<p class='meta'><strong>Cadence / Audience:</strong> {escape_html(brief_mode_value)} / {escape_html(audience_value)}</p>",
        f"<p class='meta'><em>{escape_html(PERSISTENT_DISCLAIMER)}</em></p>",
        "</header>",
        "<hr class='divider' />",
        "<section id='top-sheet' class='section'>",
        "<h2>Top Sheet</h2>",
        f"<p class='muted'>Approval state: {escape_html(str(approval_payload.get('approval_status', 'generated')))}</p>",
        f"<p class='muted'>Brief mode: {escape_html(brief_mode_value)} | Audience preset: {escape_html(audience_value)}</p>",
        "</section>",
        "<section id='executive-snapshot' class='section'>",
        "<h2>Executive Snapshot</h2>",
        "<h3>Policy context (long horizon)</h3>",
        "<ul class='compact-list'>",
    ]
    for item in policy_items[:5]:
        html_body.append(_enriched_item_html(item, citation_registry))
    html_body.extend(["</ul>", "<h3>Monitoring and opportunities (short horizon)</h3>", "<ul class='compact-list'>"])
    for item in monitoring_items[:5]:
        html_body.append(_enriched_item_html(item, citation_registry))
    html_body.extend(
        [
            "</ul>",
            (
                "<p class='muted'>Data freshness: "
                f"{live_count} live and {cached_count} cached citations in this snapshot"
                f"{citation_registry.markers_html(executive_citations[:2])}.</p>"
            ),
            "</section>",
            "<section id='what-changed' class='section'>",
            f"<h2>{escape_html(changed_heading.replace('## ', ''))}</h2>",
            "<ul class='compact-list'>",
        ]
    )
    if what_changed_items:
        for item in what_changed_items:
            html_body.append(_enriched_item_html(item, citation_registry))
    else:
        html_body.append("<li>No prior report comparison was available in this run.</li>")
    html_body.extend(["</ul>", "</section>"])
    if include_mcp_details:
        html_body.extend(
            [
                "<section id='mcp-updates' class='section'>",
                "<h2>MCP Updates</h2>",
                (
                    "<p class='muted'>MCP coverage summary: "
                    f"connectable {int(mcp_updates_payload.get('coverage', {}).get('connectable', 0))}, "
                    f"live successes {int(mcp_updates_payload.get('coverage', {}).get('live_successes', 0))}, "
                    f"success ratio {float(mcp_updates_payload.get('coverage', {}).get('success_ratio', 0.0)):.2f}.</p>"
                ),
            ]
        )
        new_mcp_items = list(mcp_updates_payload.get("new_items", []))
        changed_mcp_items = list(mcp_updates_payload.get("changed_items", []))
        if not new_mcp_items and not changed_mcp_items:
            html_body.append("<p>No new MCP items since prior run.</p>")
        else:
            if new_mcp_items:
                html_body.extend(["<h3>New MCP items today</h3>", "<ul class='compact-list'>"])
                for item in new_mcp_items[:10]:
                    published_at = str(item.get("published_at") or "publication date unavailable")
                    html_body.append(
                        "<li>"
                        + f"<strong>{escape_html(str(item.get('server_id', 'unknown_server')))}</strong>: "
                        + f"{escape_html(first_sentence(str(item.get('title', 'untitled MCP item'))))} "
                        + f"(published_at: {escape_html(published_at)}, retrieved_at: {escape_html(str(item.get('retrieved_at', 'n/a')))})."
                        + citation_registry.markers_html(item.get("citations", []))
                        + "</li>"
                    )
                html_body.append("</ul>")
            if changed_mcp_items:
                html_body.extend(["<h3>Changed MCP items today</h3>", "<ul class='compact-list'>"])
                for item in changed_mcp_items[:5]:
                    published_at = str(item.get("published_at") or "publication date unavailable")
                    html_body.append(
                        "<li>"
                        + f"<strong>{escape_html(str(item.get('server_id', 'unknown_server')))}</strong>: "
                        + f"{escape_html(first_sentence(str(item.get('title', 'untitled MCP item'))))} "
                        + f"(published_at: {escape_html(published_at)}, retrieved_at: {escape_html(str(item.get('retrieved_at', 'n/a')))})."
                        + citation_registry.markers_html(item.get("citations", []))
                        + "</li>"
                    )
                html_body.append("</ul>")
        html_body.append("</section>")
    html_body.extend([*_render_policy_pack_html(policy_pack_payload, chart_payload_list, citation_registry), *_render_execution_layer_html(policy_pack_payload)])
    html_body.extend(["<section id='evidence-layer' class='section page-break'>", "<h2>Evidence Layer</h2>"])
    if include_internal_diagnostics:
        html_body.extend(
            [
                "<div class='quality-box'>",
                "<h3>Internal Diagnostics</h3>",
                "<ul class='compact-list'>",
                f"<li>Citation count: {citation_registry.citation_count}</li>",
                f"<li>Cached citations in executive section: {cached_count}</li>",
                "</ul>",
                "</div>",
            ]
        )
    html_body.extend(["<section id='data-graphs' class='section page-break'>", "<h2>Data Graphs</h2>"])
    if recency_summary:
        counts = recency_summary.get("counts", {})
        recency_citations = [row.get("citation") for row in graph_rows if row.get("citation") is not None][:2]
        html_body.append(
            "<div class='quality-box'>"
            + "<h3>Data Recency Summary</h3>"
            + f"<p>fresh: {int(counts.get('fresh', 0))} | lagged: {int(counts.get('lagged', 0))} | stale: {int(counts.get('stale', 0))}{citation_registry.markers_html(recency_citations)}</p>"
        )
        top_lagged = recency_summary.get("top_lagged_metrics", [])
        if top_lagged:
            html_body.append("<ul class='compact-list'>")
            for item in top_lagged[:5]:
                html_body.append(
                    "<li>"
                    + f"{escape_html(str(item.get('metric', 'metric')))} | observed_at {escape_html(str(item.get('observed_at', 'observed_at unavailable')))} | "
                    + f"lag {escape_html(str(item.get('lag_days', 'n/a')))} day(s) | source {escape_html(str(item.get('source_id', 'unknown_source')))}"
                    + "</li>"
                )
            html_body.append("</ul>")
        non_compliant = recency_summary.get("non_compliant_metrics", [])
        if non_compliant:
            html_body.append("<ul class='compact-list'>")
            for metric_name in non_compliant:
                html_body.append(f"<li>{escape_html(str(metric_name))}: observed_at unavailable</li>")
            html_body.append("</ul>")
        html_body.append("</div>")
    html_body.extend(
        [
            "<table class='data-table'>",
            "<thead><tr><th>Metric</th><th>As of</th><th>Lag</th><th>Layer</th><th class='num'>Latest</th><th class='num'>1y Chg</th><th>3y Trend (w)</th>"
            "<th class='num'>5y Vol</th><th class='num'>5y pct</th><th class='num'>10y pct</th><th>10y Range</th><th>Regime</th>"
            "<th class='num'>5obs</th><th class='num'>20obs Mom</th><th>60d Trend</th><th class='num'>60d pct</th><th>60d Range</th><th>Dir Tag</th></tr></thead><tbody>",
        ]
    )
    for row in graph_rows:
        citation = row.get("citation")
        marker = citation_registry.markers_html([citation]) if citation else ""
        lag_label = (
            f"{row.get('lag_days')} day(s) [{row.get('lag_class')}]"
            if row.get("lag_days") is not None
            else "observed_at unavailable"
        )
        if "long_horizon" in row and "short_horizon" in row:
            long = row.get("long_horizon", {})
            short = row.get("short_horizon", {})
            pct_10 = long.get("percentile_10y")
            pct_anchor = pct_10 if pct_10 is not None else long.get("percentile_5y")
            pct_label = _format_optional_pct(pct_anchor)
            pct_value = float(pct_anchor) if pct_anchor is not None else None
            width = f"{pct_value:.1f}%" if pct_value is not None else "0%"
            html_body.append(
                "<tr class='row-long'>"
                f"<td>{escape_html(str(row['metric']))}<div class='muted'>{escape_html(str(row.get('daily_change_cue', '1d delta: n/a')))}</div>{marker}</td>"
                f"<td>{escape_html(str(row.get('as_of', 'observed_at unavailable')))}</td>"
                f"<td>{escape_html(lag_label)}</td>"
                "<td>long</td>"
                f"<td class='num'>{_format_optional_float(long.get('latest'))}</td>"
                f"<td class='num'>{_format_optional_float(long.get('change_1y'))}</td>"
                f"<td><code class='sparkline'>{escape_html(str(long.get('sparkline_3y_weekly', '')))}</code></td>"
                f"<td class='num'>{_format_optional_pct(float(long.get('rolling_vol_5y')) * 100.0 if long.get('rolling_vol_5y') is not None else None)}</td>"
                f"<td class='num'>{_format_optional_pct(long.get('percentile_5y'))}</td>"
                f"<td class='num'>{_format_optional_pct(long.get('percentile_10y')) if long.get('percentile_10y') is not None else escape_html(str(long.get('ten_year_note', 'n/a')))}</td>"
                "<td><div class='range-wrap'>"
                f"<span class='range-track'><span class='range-fill' style='width:{width}'></span></span>"
                f"<span class='range-pct'>{pct_label}</span></div></td>"
                f"<td>{escape_html(str(long.get('regime_classification', '')))}</td>"
                "<td class='num'></td><td class='num'></td><td></td><td class='num'></td><td></td><td></td>"
                "</tr>"
            )
            pct_60 = short.get("percentile_60d")
            width_60 = f"{float(pct_60):.1f}%" if pct_60 is not None else "0%"
            html_body.append(
                "<tr class='row-short'>"
                "<td></td><td></td><td></td><td>short</td><td class='num'></td><td class='num'></td><td></td><td class='num'></td><td class='num'></td><td class='num'></td><td></td><td></td>"
                f"<td class='num'>{_format_optional_float(short.get('change_5obs'))}</td>"
                f"<td class='num'>{_format_optional_float(short.get('momentum_20obs'))}</td>"
                f"<td><code class='sparkline'>{escape_html(str(short.get('sparkline_60d', '')))}</code></td>"
                f"<td class='num'>{_format_optional_pct(short.get('percentile_60d'))}</td>"
                "<td><div class='range-wrap'>"
                f"<span class='range-track'><span class='range-fill' style='width:{width_60}'></span></span>"
                f"<span class='range-pct'>{_format_optional_pct(short.get('percentile_60d'))}</span></div></td>"
                f"<td>{escape_html(str(short.get('direction_tag', '')))}</td>"
                "</tr>"
            )
        else:
            pct = range_bar_percent(str(row.get("range_bar", "")))
            pct_label = f"{pct}%" if pct is not None else "n/a"
            width = f"{pct}%" if pct is not None else "0%"
            html_body.append(
                "<tr>"
                f"<td>{escape_html(str(row['metric']))}<div class='muted'>{escape_html(str(row.get('daily_change_cue', '1d delta: n/a')))}</div>{marker}</td>"
                f"<td>{escape_html(str(row.get('as_of', 'observed_at unavailable')))}</td>"
                f"<td>{escape_html(lag_label)}</td>"
                "<td>legacy</td>"
                f"<td class='num'>{escape_html(str(row.get('latest', '')))}</td>"
                "<td class='num'></td>"
                "<td></td>"
                "<td class='num'></td>"
                "<td class='num'></td>"
                "<td class='num'></td>"
                "<td><div class='range-wrap'>"
                f"<span class='range-track'><span class='range-fill' style='width:{width}'></span></span>"
                f"<span class='range-pct'>{pct_label}</span></div></td>"
                f"<td>{escape_html(str(row.get('trajectory', 'n/a')))}</td>"
                f"<td class='num'>{escape_html(str(row.get('delta_5', '')))}</td>"
                "<td class='num'></td>"
                f"<td><code class='sparkline'>{escape_html(str(row.get('sparkline', '')))}</code></td>"
                "<td class='num'></td>"
                f"<td><code>{escape_html(str(row.get('range_bar', '')))}</code></td>"
                f"<td>{escape_html(str(row.get('pattern', 'n/a')))}</td>"
                "</tr>"
            )
    html_body.extend(
        [
            "</tbody></table>",
            "<p class='muted'>Legend: long horizon shows regime context (1y/3y/5y/10y); short horizon captures monitoring drift (5obs/20obs/60d).</p>",
            "<div class='quality-box'>",
            "<h3>Graph Quality</h3>",
        ]
    )
    if graph_quality_audit:
        html_body.extend(
            [
                "<ul class='compact-list'>",
                f"<li>Up to date: {escape_html(str(graph_quality_audit.get('up_to_date')))}</li>",
                f"<li>Retrieval fresh (&lt;=48h): {escape_html(str(graph_quality_audit.get('retrieval_fresh')))}</li>",
                f"<li>Data fresh (&lt;=7d): {escape_html(str(graph_quality_audit.get('data_fresh')))}</li>",
                f"<li>Cited: {escape_html(str(graph_quality_audit.get('cited')))}</li>",
                f"<li>Easy to comprehend: {escape_html(str(graph_quality_audit.get('easy_to_comprehend')))}</li>",
                (
                    "<li>Note: "
                    f"{escape_html(str(graph_quality_audit.get('note', '')))}"
                    f"{citation_registry.markers_html(graph_quality_audit.get('citations', []))}</li>"
                ),
                "</ul>",
            ]
        )
    html_body.extend(["</div>", "</section>"])

    html_body.extend(["<section id='long-horizon-context' class='section'>", "<h2>Long Horizon Context</h2>", "<ul class='compact-list'>"])
    if long_context_items:
        for item in long_context_items:
            sentence = first_sentence(str(item.get("text", "")))
            html_body.append(
                f"<li>{emphasize_numbers_html(sentence)}{citation_registry.markers_html(item.get('citations', []))}</li>"
            )
    else:
        html_body.append("<li>Long-horizon explanatory context was not provided in this run.</li>")
    html_body.extend(["</ul>", "</section>"])

    html_body.extend(["<section id='multi-perspective' class='section page-break'>", "<h2>Multi Perspective Interpretation</h2>"])
    for lens in lenses:
        citations = lens.get("citations", [])
        body = str(lens.get("body", "")).strip()
        emphasis = str(lens.get("emphasis", "")) or first_sentence(body)
        takeaways = list(lens.get("key_takeaways", [])) or split_sentences(body)[:3]
        changes = list(lens.get("what_changes_view", [])) or _default_view_change_triggers(str(lens.get("title", "")))
        takeaways = [first_sentence(item) for item in takeaways[:3] if item]
        changes = [first_sentence(item) for item in changes[:2] if item]
        html_body.extend(
            [
                "<article class='lens'>",
                f"<h3>{escape_html(str(lens['title']))}{citation_registry.markers_html(citations)}</h3>",
                (
                    "<p class='source-meta'><strong>Source:</strong> "
                    f"{escape_html(str(lens.get('source_title', 'Source metadata not provided')))}, "
                    + (
                        escape_html(str(lens.get("source_date")))
                        if lens.get("source_date_available", False) and lens.get("source_date")
                        else "Publication date not available in source metadata."
                    )
                    + "</p>"
                ),
            ]
        )
        quote = lens.get("quote")
        if quote:
            html_body.append(f"<blockquote class='quote'>\"{escape_html(str(quote))}\"</blockquote>")
        html_body.extend(
            [
                f"<p class='lens-emphasis'><em>What this lens emphasizes:</em> {emphasize_numbers_html(emphasis)}</p>",
                "<h4>Key takeaways</h4>",
                "<ul class='compact-list'>",
            ]
        )
        html_body.extend(f"<li>{emphasize_numbers_html(item)}</li>" for item in takeaways)
        html_body.extend(["</ul>", "<h4>What would change this view</h4>", "<ul class='compact-list'>"])
        html_body.extend(f"<li>{emphasize_numbers_html(item)}</li>" for item in changes)
        near_term = lens.get("near_term_monitor", [])[:2]
        if near_term:
            html_body.extend(["</ul>", "<h4>Near term monitor</h4>", "<ul class='compact-list'>"])
            for item in near_term:
                sentence = first_sentence(str(item.get("text", "")))
                html_body.append(
                    f"<li>{emphasize_numbers_html(sentence)}{citation_registry.markers_html(item.get('citations', []))}</li>"
                )
        html_body.extend(["</ul>", "</article>"])
    html_body.append("</section>")

    html_body.extend(["<section id='alerts-timeline' class='section page-break'>", "<h2>Alerts Timeline</h2>"])
    if not alert_items:
        html_body.append("<p>No alert events were generated in this run.</p>")
    else:
        html_body.extend(
            [
                "<table class='data-table compact'><thead><tr><th>Severity</th><th>Trigger reason</th><th>What moved</th><th>Why it matters</th><th>What would neutralize it</th><th>Monitor next</th><th>What to consider</th></tr></thead><tbody>",
            ]
        )
        for item in alert_items:
            severity = str(item.get("severity", "info")).lower()
            html_body.append(
                "<tr>"
                f"<td><span class='pill {escape_html(severity)}'>{escape_html(severity.upper())}</span></td>"
                f"<td>{emphasize_numbers_html(first_sentence(str(item.get('trigger_reason', ''))))}{citation_registry.markers_html(item.get('citations', []))}</td>"
                f"<td>{emphasize_numbers_html(first_sentence(str(item.get('what_moved', ''))))}</td>"
                f"<td>{emphasize_numbers_html(first_sentence(str(item.get('why_it_matters', ''))))}</td>"
                f"<td>{emphasize_numbers_html(first_sentence(str(item.get('what_would_neutralize', ''))))}</td>"
                f"<td>{emphasize_numbers_html(first_sentence(str(item.get('what_to_monitor_next', ''))))}</td>"
                f"<td>{emphasize_numbers_html(first_sentence(str(item.get('what_to_consider', item.get('action_tag', '')))) )}</td>"
                "</tr>"
            )
        html_body.extend(["</tbody></table>"])
    html_body.append("</section>")

    html_body.extend(["<section id='opportunity-observations' class='section'>", "<h2>Opportunity Observations</h2>"])
    if not opportunity_items:
        html_body.append("<p>No opportunity observations met citation and confidence gates in this run.</p>")
    else:
        html_body.extend(
            [
                "<table class='data-table compact'><thead><tr><th>Rank</th><th>Condition observed</th><th>What data would confirm</th><th>Horizon</th><th>Observation confidence</th><th>Monitor next</th></tr></thead><tbody>",
            ]
        )
        for idx, item in enumerate(opportunity_items, start=1):
            html_body.append(
                "<tr>"
                f"<td class='num'>{idx}</td>"
                f"<td>{emphasize_numbers_html(first_sentence(str(item.get('condition_observed', ''))))}{citation_registry.markers_html(item.get('citations', []))}</td>"
                f"<td>{emphasize_numbers_html(first_sentence(str(item.get('confirmation_data', ''))))}</td>"
                f"<td>{escape_html(str(item.get('time_horizon', '')))}</td>"
                f"<td class='num'>{_format_optional_pct(float(item.get('confidence', 0.0)) * 100.0)}</td>"
                f"<td>{emphasize_numbers_html(first_sentence(str(item.get('what_to_monitor_next', ''))))}</td>"
                "</tr>"
            )
        html_body.extend(["</tbody></table>"])
    html_body.append("</section>")

    html_body.extend(["<section id='big-players' class='section page-break'>", "<h2>Big Players Activity</h2>"])
    if not big_players:
        html_body.append("<p>No reliably sourced large player items in this run.</p>")
    else:
        html_body.extend(
            [
                "<table class='data-table'><thead><tr><th>Item</th><th>Why it matters</th></tr></thead><tbody>",
            ]
        )
        for item in big_players:
            left, right = split_item_and_why(str(item.get("text", "")))
            html_body.append(
                "<tr>"
                f"<td>{emphasize_numbers_html(left)}{citation_registry.markers_html(item.get('citations', []))}</td>"
                f"<td>{emphasize_numbers_html(right)}</td>"
                "</tr>"
            )
        html_body.extend(["</tbody></table>"])
    html_body.append("</section>")

    html_body.extend(["<section id='portfolio-mapping' class='section page-break'>", "<h2>Portfolio Mapping to SGD 1M Policy</h2>"])
    if policy_alloc:
        html_body.extend(["<table class='data-table compact'><thead><tr><th>Policy Sleeve</th><th class='num'>Target Weight</th></tr></thead><tbody>"])
        for sleeve, weight in policy_alloc.items():
            html_body.append(
                f"<tr><td>{escape_html(sleeve.replace('_', ' ').title())}</td><td class='num'>{weight * 100:.1f}%</td></tr>"
            )
        html_body.extend(["</tbody></table>"])

    html_body.extend(
        [
            "<div class='placeholder-card'>Holdings not provided, drift and rebalance bands not evaluated.</div>",
            "<ul class='compact-list'>",
        ]
    )
    for mapping in portfolio_mapping:
        html_body.append(
            f"<li>{emphasize_numbers_html(first_sentence(mapping['text']))}{citation_registry.markers_html(mapping.get('citations', []))}</li>"
        )
    html_body.append("</ul>")

    status_class = "pass" if convex_report.get("valid") else "fail"
    status_text = "PASS" if convex_report.get("valid") else "FAIL"
    html_body.append(f"<p>Convex compliance status: <span class='pill {status_class}'>{status_text}</span></p>")

    if target_breakdown:
        html_body.extend(
            [
                "<table class='data-table compact'><thead><tr><th>Convex Component</th><th class='num'>Target</th>"
                "<th class='num'>Actual</th><th>Check</th></tr></thead><tbody>",
            ]
        )
        for item in target_breakdown:
            check = "OK" if item.get("within_target", False) else "Review"
            html_body.append(
                "<tr>"
                f"<td>{escape_html(str(item.get('component', '')))}</td>"
                f"<td class='num'>{float(item.get('target', 0.0)) * 100:.1f}%</td>"
                f"<td class='num'>{float(item.get('actual', 0.0)) * 100:.1f}%</td>"
                f"<td>{check}</td>"
                "</tr>"
            )
        html_body.extend(["</tbody></table>"])

    html_body.extend(
        [
            "<table class='data-table compact'><thead><tr><th>Flag</th><th>Value</th></tr></thead><tbody>",
            f"<tr><td>Margin required anywhere in convex sleeve</td><td>{yes_no(convex_report.get('margin_required_any', False))}</td></tr>",
            f"<tr><td>Max loss known across convex sleeve</td><td>{yes_no(convex_report.get('max_loss_known_all', False))}</td></tr>",
            f"<tr><td>Notes</td><td>{escape_html('; '.join(convex_report.get('errors', [])) if convex_report.get('errors') else 'Policy compliant')}</td></tr>",
            "</tbody></table>",
            "</section>",
        ]
    )

    html_body.extend(["<section id='implementation-layer' class='section page-break'>", "<h2>Implementation Layer – Illustrative Products</h2>"])
    if disclaimer:
        html_body.append(f"<p class='muted'><em>{escape_html(str(disclaimer))}</em></p>")
    if not include_full_implementation_layer:
        html_body.append("<p>Client-friendly preset: full implementation tables are suppressed in favor of the execution-layer summary.</p>")
    else:
        for sleeve_key in sleeve_order:
            payload = implementation_payload.get("sleeves", {}).get(sleeve_key, {})
            candidates = payload.get("candidates", [])
            if not candidates:
                continue
            html_body.extend(
                [
                    f"<h3>{escape_html(str(payload.get('title', sleeve_key.replace('_', ' ').title())))}</h3>",
                    "<table class='data-table compact'><thead><tr><th>Symbol</th><th>Instrument</th><th>Domicile</th><th class='num'>Expense</th>"
                    "<th class='num'>SG tax score</th><th>US situs risk</th><th class='num'>Liquidity</th><th class='num'>Withholding</th>"
                    "<th class='num'>Yield proxy</th><th class='num'>Duration</th><th>Retrieved (UTC)</th></tr></thead><tbody>",
                ]
            )
            for candidate in candidates:
                html_body.append(
                    "<tr>"
                    f"<td>{escape_html(candidate.symbol)}{citation_registry.markers_html(candidate.citations)}</td>"
                    f"<td>{escape_html(candidate.name)}</td>"
                    f"<td>{escape_html(candidate.domicile)}</td>"
                    f"<td class='num'>{_format_optional_pct(candidate.expense_ratio * 100.0, precision=2)}</td>"
                    f"<td class='num'>{_format_optional_float(candidate.tax_score, precision=2)}</td>"
                    f"<td>{yes_no(candidate.us_situs_risk_flag)}</td>"
                    f"<td class='num'>{_format_optional_float(candidate.liquidity_score, precision=2)}</td>"
                    f"<td class='num'>{_format_optional_pct(candidate.withholding_rate * 100.0, precision=1)}</td>"
                    f"<td class='num'>{_format_optional_float(candidate.yield_proxy, precision=2)}</td>"
                    f"<td class='num'>{_format_optional_float(candidate.duration_years, precision=2)}</td>"
                    f"<td>{escape_html(_minute_stamp(candidate.retrieved_at))}</td>"
                    "</tr>"
                )
            html_body.extend(["</tbody></table>"])

            if sleeve_key == "global_equity":
                html_body.extend(["<h4>SG Tax Implementation Observations</h4>", "<ul class='compact-list'>"])
                for item in payload.get("sg_tax_observations", []):
                    html_body.append(
                        f"<li>{emphasize_numbers_html(first_sentence(str(item.get('text', ''))))}{citation_registry.markers_html(item.get('citations', []))}</li>"
                    )
                html_body.append("</ul>")

            if sleeve_key == "convex":
                html_body.extend(["<h4>Convex Option Implementation Constraints</h4>", "<ul class='compact-list'>"])
                for candidate in candidates:
                    if candidate.option_position is None:
                        continue
                    html_body.append(
                        "<li>"
                        + f"{escape_html(candidate.symbol)}: option_position={escape_html(candidate.option_position)}, "
                        + f"strike={_format_optional_float(candidate.strike, precision=2)}, expiry={escape_html(candidate.expiry or 'n/a')}, "
                        + f"premium_pct_nav={_format_optional_pct((candidate.premium_paid_pct_nav or 0.0) * 100.0, precision=2)}, "
                        + f"annualized_carry={_format_optional_pct((candidate.annualized_carry_estimate or 0.0) * 100.0, precision=2)}, "
                        + f"margin_required={yes_no(candidate.margin_required)}, max_loss_known={yes_no(candidate.max_loss_known)}"
                        + citation_registry.markers_html(candidate.citations)
                        + "</li>"
                    )
                html_body.append("</ul>")

        if watchlist_candidates:
            html_body.extend(
                [
                    "<h3>Implementation Watchlist Candidates</h3>",
                    "<table class='data-table compact'><thead><tr><th>Symbol</th><th>Condition observed</th><th>Horizon</th><th class='num'>SG tax score</th><th class='num'>Liquidity score</th></tr></thead><tbody>",
                ]
            )
            for item in watchlist_candidates:
                html_body.append(
                    "<tr>"
                    f"<td>{escape_html(str(item.get('symbol', '')))}{citation_registry.markers_html(item.get('citations', []))}</td>"
                    f"<td>{emphasize_numbers_html(first_sentence(str(item.get('condition', ''))))}</td>"
                    f"<td>{escape_html(str(item.get('time_horizon', '')))}</td>"
                    f"<td class='num'>{_format_optional_float(item.get('tax_score'), precision=2)}</td>"
                    f"<td class='num'>{_format_optional_float(item.get('liquidity_score'), precision=2)}</td>"
                    "</tr>"
                )
            html_body.extend(["</tbody></table>"])
    html_body.append("</section>")

    html_body.extend(["<section id='source-appendix' class='section page-break'>", "<h2>Source Appendix</h2>"])
    for tier in ["primary", "secondary", "tertiary"]:
        rows = grouped_sources.get(tier, [])
        if not rows:
            continue
        html_body.extend(
            [
                f"<h3>{tier.title()} Sources</h3>",
                "<table class='data-table compact'><thead><tr><th>Publisher</th><th>Retrieved At (UTC)</th><th>Data as of</th><th>Importance note</th><th>Link</th></tr></thead><tbody>",
            ]
        )
        for source in rows:
            data_as_of = source_data_asof_map.get(source.source_id, "")
            if not data_as_of and source.published_at is not None:
                data_as_of = source.published_at.date().isoformat()
            html_body.append(
                "<tr>"
                f"<td>{escape_html(source.publisher)}</td>"
                f"<td>{escape_html(_minute_stamp(source.retrieved_at))}</td>"
                f"<td>{escape_html(data_as_of)}</td>"
                f"<td>{escape_html(important_source_note(source.source_id))}</td>"
                f"<td><a href='{source.url}'>Link</a></td>"
                "</tr>"
            )
        html_body.extend(["</tbody></table>"])
    html_body.append("</section>")

    html_body.extend(["<section id='sources' class='section page-break'>", "<h2>Sources</h2>", "<ol class='citations'>"])
    html_body.extend(citation_registry.html_footnotes())
    html_body.extend(["</ol>", "</section>", "</section>"])

    html = html_template.replace("{{styles}}", css).replace("{{body}}", "\n".join(html_body))

    _validate_rendered_report(markdown=markdown, html=html, citation_count=citation_registry.citation_count)
    return {"markdown": markdown, "html": html}


def write_narrated_email_files(prefix: str, markdown: str, html: str) -> dict[str, str]:
    OUTBOX_DIR.mkdir(parents=True, exist_ok=True)
    md_path = OUTBOX_DIR / f"{prefix}.md"
    html_path = OUTBOX_DIR / f"{prefix}.html"
    pdf_path = OUTBOX_DIR / f"{prefix}.pdf"

    md_path.write_text(markdown, encoding="utf-8")
    html_path.write_text(html, encoding="utf-8")

    pdf_ok = False
    try:
        with pdf_path.open("wb") as out:
            # cupsfilter can hang in headless/local test environments even after
            # the rest of the brief generation work has finished. Bound the call
            # and fall back to the deterministic PDF writer so pytest can exit
            # cleanly without leaving a blocked subprocess behind.
            proc = subprocess.run(
                ["/usr/sbin/cupsfilter", "-m", "application/pdf", str(html_path)],
                stdout=out,
                stderr=subprocess.PIPE,
                check=False,
                timeout=15,
            )
        pdf_ok = proc.returncode == 0 and pdf_path.exists() and pdf_path.stat().st_size > 0
    except Exception:
        pdf_ok = False

    if not pdf_ok:
        _write_simple_pdf_from_lines(markdown.splitlines(), pdf_path)

    if not md_path.exists() or not html_path.exists() or not pdf_path.exists():
        raise ReportBuildError("Report output validation failed: one or more files were not created")
    if pdf_path.stat().st_size <= 0:
        raise ReportBuildError("Report output validation failed: PDF file is empty")

    return {
        "md_path": str(md_path),
        "html_path": str(html_path),
        "pdf_path": str(pdf_path),
    }
