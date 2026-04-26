from __future__ import annotations


def classify_action_tag(text: str) -> str:
    lowered = text.lower()
    if "tax" in lowered or "withholding" in lowered:
        return "tax_review"
    if "benchmark" in lowered:
        return "benchmark_watch"
    if "scenario" in lowered or "tail" in lowered or "drawdown" in lowered:
        return "scenario_watch"
    if "rebalance" in lowered or "band" in lowered or "drift" in lowered:
        return "rebalance_candidate"
    if "review" in lowered:
        return "review"
    return "monitor"


def _affected_sleeves(text: str) -> list[str]:
    lowered = text.lower()
    tags = []
    mapping = {
        "global_equity_core": ["equity", "sp500", "global equity"],
        "emerging_markets": ["em ", "emerging market"],
        "china_satellite": ["china"],
        "IG_bonds": ["bond", "yield", "duration", "credit spread", "treasury"],
        "cash_bills": ["cash", "bill", "front-end rate"],
        "real_assets": ["inflation", "commodity", "oil", "real asset"],
        "alternatives": ["alternative", "spread dislocation"],
        "convex": ["volatility", "vix", "tail", "convex"],
    }
    for sleeve_tag, keywords in mapping.items():
        if any(keyword in lowered for keyword in keywords):
            tags.append(sleeve_tag)
    return tags or ["benchmark_watch_only"]


def _output_type(action_tag: str) -> str:
    if action_tag in {"benchmark_watch"}:
        return "implication_for_blueprint_review"
    if action_tag in {"scenario_watch"}:
        return "implication_for_dashboard_alerting"
    if action_tag in {"rebalance_candidate", "review"}:
        return "implication_for_monitoring"
    return "observation_only"


def _monitor_next(action_tag: str, sleeves: list[str]) -> str:
    if action_tag == "benchmark_watch":
        return f"Review benchmark drift and sleeve assumptions for {', '.join(sleeves[:2])}."
    if action_tag == "scenario_watch":
        return f"Watch for persistence or reversal in the drivers affecting {', '.join(sleeves[:2])}."
    if action_tag == "rebalance_candidate":
        return f"Check Blueprint bands and implementation drift for {', '.join(sleeves[:2])}."
    if action_tag == "tax_review":
        return "Watch withholding, domicile, and wrapper assumptions."
    return f"Monitor whether the signal broadens beyond {', '.join(sleeves[:2])}."


def _conservative_guidance(action_tag: str) -> str:
    mapping = {
        "benchmark_watch": "review Blueprint sleeve",
        "scenario_watch": "escalate to Dashboard",
        "rebalance_candidate": "review current holdings",
        "tax_review": "monitor",
        "review": "prepare committee memo only",
        "monitor": "monitor",
    }
    return mapping.get(action_tag, "no action")


def _portfolio_implication(sleeves: list[str]) -> str:
    if not sleeves or sleeves == ["benchmark_watch_only"]:
        return "Benchmark watch only; no direct current-holdings implication established."
    return f"Most relevant to {', '.join(sleeves[:3])}; confirm against live holdings and Blueprint before any policy review."


def tag_items(items: list[dict]) -> list[dict]:
    tagged: list[dict] = []
    for item in items:
        text = str(item.get("text") or item.get("summary") or "")
        enriched = dict(item)
        action_tag = str(item.get("action_tag") or classify_action_tag(text))
        sleeves = _affected_sleeves(text)
        enriched["action_tag"] = action_tag
        enriched["affected_sleeves"] = sleeves
        enriched["output_type"] = _output_type(action_tag)
        enriched["portfolio_implication"] = str(item.get("portfolio_implication") or _portfolio_implication(sleeves))
        enriched["what_to_monitor_next"] = str(item.get("what_to_monitor_next") or _monitor_next(action_tag, sleeves))
        enriched["what_to_consider"] = str(item.get("what_to_consider") or _conservative_guidance(action_tag))
        tagged.append(enriched)
    return tagged
