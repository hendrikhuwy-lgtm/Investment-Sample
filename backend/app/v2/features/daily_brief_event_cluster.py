from __future__ import annotations

import hashlib
from typing import Any


_STOPWORDS = {
    "about",
    "after",
    "again",
    "against",
    "amid",
    "with",
    "from",
    "into",
    "over",
    "said",
    "says",
    "reported",
    "report",
    "markets",
    "market",
    "risk",
    "risks",
    "global",
    "news",
    "latest",
}


def build_event_cluster(
    *,
    label: str,
    source_class: str,
    bucket: str,
    seed_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return compact event metadata for news/policy signals.

    The Daily Brief still renders the existing card contract. This metadata is
    used for backend selection and dedupe so one story cluster cannot appear as
    both active news and regime context.
    """
    metadata = dict(seed_metadata or {})
    normalized_source_class = str(source_class or "").strip().lower()
    expected_family = "policy" if normalized_source_class == "policy_event" else "geopolitics"
    existing_cluster_id = str(metadata.get("event_cluster_id") or "").strip()
    existing_family = str(metadata.get("event_family") or "").strip()
    if existing_cluster_id and existing_family == expected_family:
        return _normalize_existing_metadata(metadata)

    if normalized_source_class not in {"geopolitical_news", "policy_event"}:
        return {}

    text = _clean_text(" ".join([str(label or ""), str(metadata.get("summary") or "")]))
    subtype = _event_subtype(text=text, source_class=normalized_source_class, bucket=bucket)
    family = expected_family
    region = _event_region(text=text, subtype=subtype)
    entities = _event_entities(text=text, subtype=subtype)
    market_channels, confirmation_assets, trigger_summary = _market_channels_for(subtype=subtype)
    anchor_entities = _cluster_anchor_entities(subtype=subtype, entities=entities)
    cluster_anchor = "|".join([family, subtype, region, ",".join(anchor_entities[:3])])
    if not any(part for part in [subtype, region, *entities]):
        cluster_anchor = f"{family}|{_fingerprint(text)}"
    cluster_id = f"event:{_slug(cluster_anchor)}"
    return {
        "event_cluster_id": cluster_id,
        "event_family": family,
        "event_subtype": subtype,
        "event_region": region,
        "event_entities": entities,
        "market_channels": market_channels,
        "confirmation_assets": confirmation_assets,
        "event_trigger_summary": trigger_summary,
        "event_title": _event_title(subtype),
        "event_fingerprint": _fingerprint(text),
    }


def _normalize_existing_metadata(metadata: dict[str, Any]) -> dict[str, Any]:
    subtype = str(metadata.get("event_subtype") or "general_event").strip().lower()
    _, fallback_assets, fallback_trigger = _market_channels_for(subtype=subtype)
    channels = [str(item).strip() for item in list(metadata.get("market_channels") or []) if str(item).strip()]
    assets = [str(item).strip() for item in list(metadata.get("confirmation_assets") or []) if str(item).strip()]
    return {
        "event_cluster_id": str(metadata.get("event_cluster_id") or "").strip(),
        "event_family": str(metadata.get("event_family") or "").strip(),
        "event_subtype": subtype,
        "event_region": str(metadata.get("event_region") or "").strip() or "global",
        "event_entities": [str(item).strip() for item in list(metadata.get("event_entities") or []) if str(item).strip()],
        "market_channels": channels,
        "confirmation_assets": assets or fallback_assets,
        "event_trigger_summary": str(metadata.get("event_trigger_summary") or "").strip() or fallback_trigger,
        "event_title": str(metadata.get("event_title") or "").strip() or _event_title(subtype),
        "event_fingerprint": str(metadata.get("event_fingerprint") or "").strip(),
    }


def _clean_text(text: str) -> str:
    return " ".join(str(text or "").lower().replace("-", " ").split())


def _event_subtype(*, text: str, source_class: str, bucket: str) -> str:
    if source_class == "policy_event":
        if any(term in text for term in {"tariff", "export control", "trade war", "import duty"}):
            return "trade_tariff_policy"
        if any(term in text for term in {"fed", "ecb", "boj", "pboc", "central bank", "rate decision"}):
            return "central_bank_policy"
        if any(term in text for term in {"election", "government", "budget", "fiscal", "treasury"}):
            return "fiscal_political_policy"
        if any(term in text for term in {"sanction", "sanctions"}):
            return "sanctions_policy"
        return "policy_headline"

    if any(term in text for term in {"iran", "israel", "hezbollah", "hamas", "gaza", "lebanon", "hormuz", "ceasefire"}):
        return "middle_east_security"
    if any(term in text for term in {"red sea", "shipping", "tanker", "freight", "oil route"}):
        return "shipping_energy_supply"
    if any(term in text for term in {"russia", "ukraine", "nato", "black sea", "europe gas"}):
        return "russia_ukraine_energy"
    if any(term in text for term in {"china", "taiwan", "south china sea"}):
        return "china_taiwan_security"
    if any(term in text for term in {"sanction", "missile", "strike", "war", "conflict"}):
        return "global_security_risk"
    if bucket == "energy":
        return "energy_supply_risk"
    return "global_headline_risk"


def _event_region(*, text: str, subtype: str) -> str:
    if subtype == "middle_east_security":
        return "middle_east"
    if subtype == "russia_ukraine_energy":
        return "europe_russia_ukraine"
    if subtype == "china_taiwan_security":
        return "china_taiwan"
    if subtype in {"trade_tariff_policy", "sanctions_policy"} and "china" in text:
        return "us_china"
    if "us" in text or "u.s" in text or "america" in text:
        return "united_states"
    if "europe" in text or "euro" in text:
        return "europe"
    return "global"


def _event_entities(*, text: str, subtype: str) -> list[str]:
    candidates = [
        "iran",
        "israel",
        "hezbollah",
        "hamas",
        "hormuz",
        "china",
        "taiwan",
        "russia",
        "ukraine",
        "fed",
        "ecb",
        "boj",
        "pboc",
        "tariff",
        "sanctions",
        "shipping",
    ]
    found = [term for term in candidates if term in text]
    if found:
        return found
    return [subtype]


def _cluster_anchor_entities(*, subtype: str, entities: list[str]) -> list[str]:
    if subtype in {
        "middle_east_security",
        "shipping_energy_supply",
        "russia_ukraine_energy",
        "china_taiwan_security",
        "global_security_risk",
        "energy_supply_risk",
    }:
        return [subtype]
    if subtype == "central_bank_policy":
        central_banks = [entity for entity in entities if entity in {"fed", "ecb", "boj", "pboc"}]
        return central_banks[:1] or [subtype]
    if subtype in {"trade_tariff_policy", "sanctions_policy"}:
        return [entity for entity in entities if entity in {"china", "tariff", "sanctions"}][:2] or [subtype]
    return entities or [subtype]


def _market_channels_for(*, subtype: str) -> tuple[list[str], list[str], str]:
    if subtype == "middle_east_security":
        assets = ["Brent/WTI", "gold", "VIX", "S&P futures", "DXY"]
        return ["oil", "safe_haven_gold", "volatility", "dollar"], assets, "Escalates only if oil, gold, volatility, or dollar pressure confirms the headline."
    if subtype == "shipping_energy_supply":
        assets = ["Brent/WTI", "shipping rates", "breakevens", "VIX"]
        return ["oil", "shipping", "inflation", "volatility"], assets, "Escalates if oil, freight, or inflation hedges confirm supply stress."
    if subtype == "russia_ukraine_energy":
        assets = ["European gas", "Brent", "EUR", "defense equities"]
        return ["energy", "europe_fx", "defense"], assets, "Escalates if European gas, oil, or EUR stress confirms the geopolitical path."
    if subtype == "china_taiwan_security":
        assets = ["China/HK equities", "semiconductors", "CNH", "EM FX"]
        return ["china_equity", "semiconductors", "em_fx"], assets, "Escalates if China, semis, CNH, or EM FX confirm risk transmission."
    if subtype == "trade_tariff_policy":
        assets = ["China/HK equities", "semiconductors", "DXY", "EM FX"]
        return ["trade", "china_equity", "semiconductors", "dollar"], assets, "Escalates if trade-sensitive equities, semis, dollar, or EM FX confirm the policy shock."
    if subtype == "central_bank_policy":
        assets = ["2Y yields", "10Y yields", "DXY", "equity futures"]
        return ["rates", "dollar", "equity_risk"], assets, "Escalates if front-end rates, the dollar, or equity futures reprice the policy path."
    if subtype == "fiscal_political_policy":
        assets = ["10Y yields", "DXY", "equity futures", "credit spreads"]
        return ["rates", "dollar", "credit", "equity_risk"], assets, "Escalates if rates, dollar, credit, or equity risk confirm a policy shock."
    assets = ["VIX", "S&P futures", "DXY", "gold"]
    return ["volatility", "equity_risk", "dollar", "safe_haven_gold"], assets, "Escalates only if cross-asset confirmation broadens."


def _event_title(subtype: str) -> str:
    return {
        "middle_east_security": "Middle East security risk",
        "shipping_energy_supply": "Shipping and energy supply risk",
        "russia_ukraine_energy": "Russia/Ukraine energy risk",
        "china_taiwan_security": "China/Taiwan security risk",
        "trade_tariff_policy": "Trade and tariff policy risk",
        "central_bank_policy": "Central bank policy risk",
        "fiscal_political_policy": "Fiscal and political policy risk",
        "sanctions_policy": "Sanctions policy risk",
        "energy_supply_risk": "Energy supply risk",
        "global_security_risk": "Global security risk",
        "policy_headline": "Policy headline risk",
    }.get(subtype, "Global headline risk")


def _fingerprint(text: str) -> str:
    tokens = [token for token in _clean_text(text).split() if len(token) > 2 and token not in _STOPWORDS]
    basis = " ".join(tokens[:12]) or _clean_text(text)
    return hashlib.sha1(basis.encode("utf-8")).hexdigest()[:12]


def _slug(text: str) -> str:
    cleaned = "".join(char.lower() if char.isalnum() else "_" for char in str(text or ""))
    while "__" in cleaned:
        cleaned = cleaned.replace("__", "_")
    return cleaned.strip("_")[:96] or "unknown"
