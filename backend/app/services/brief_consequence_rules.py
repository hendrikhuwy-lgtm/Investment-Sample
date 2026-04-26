from __future__ import annotations

from typing import Any


def metric_signal_registry() -> dict[str, dict[str, Any]]:
    return {
        "DGS10": {
            "primary_sleeves": ["IG_bonds"],
            "secondary_sleeves": ["cash_bills"],
            "benchmark_context": "Duration-sensitive benchmark assumptions and defensive ballast remain exposed to rate repricing.",
            "policy_relevance": "high relevance to target sleeves",
            "follow_up": "review Blueprint",
            "category": "Rates and inflation",
            "interpretation": "Higher long yields keep duration pressure elevated and raise the hurdle for bond sleeves to provide clean ballast.",
            "mechanism": "Long-duration discounting and carry assumptions deteriorate when the term premium or real-rate pressure rises.",
        },
        "T10Y2Y": {
            "primary_sleeves": [],
            "secondary_sleeves": ["IG_bonds", "cash_bills"],
            "benchmark_context": "Curve shape matters for duration carry and benchmark duration framing.",
            "policy_relevance": "benchmark watch only",
            "follow_up": "watch benchmark drift",
            "category": "Rates and inflation",
            "interpretation": "Curve shape is changing the carry and policy backdrop even if outright yields are not delivering the whole story.",
            "mechanism": "Curve shifts influence roll-down, carry, and the practical value of extending duration.",
        },
        "T10YIE": {
            "primary_sleeves": ["real_assets"],
            "secondary_sleeves": ["IG_bonds"],
            "benchmark_context": "Inflation-sensitive sleeves and real-yield assumptions deserve watch.",
            "policy_relevance": "high relevance to target sleeves",
            "follow_up": "review Blueprint",
            "category": "Rates and inflation",
            "interpretation": "Firm inflation compensation keeps real-asset ballast relevant and limits how cleanly falling growth fears can help duration.",
            "mechanism": "Higher breakevens alter the balance between inflation protection and nominal-duration relief.",
        },
        "BAMLH0A0HYM2": {
            "primary_sleeves": ["IG_bonds"],
            "secondary_sleeves": ["cash_bills", "alternatives"],
            "benchmark_context": "Credit-spread dislocation changes defensive ballast and liquidity watch context.",
            "policy_relevance": "high relevance to target sleeves",
            "follow_up": "monitor",
            "category": "Credit and liquidity",
            "interpretation": "Wider credit spreads point to tighter conditions and a less forgiving backdrop for risk assets that rely on orderly financing.",
            "mechanism": "Credit spreads are one of the clearest cross-market signals of funding stress and weakening risk tolerance.",
        },
        "VIXCLS": {
            "primary_sleeves": ["convex"],
            "secondary_sleeves": ["global_equity_core"],
            "benchmark_context": "Volatility regime shifts raise benchmark-watch urgency but do not justify allocation changes by themselves.",
            "policy_relevance": "high relevance to target sleeves",
            "follow_up": "escalate to Dashboard",
            "category": "Volatility and stress",
            "interpretation": "Rising volatility materially improves the relevance of convex protection and raises the standard for treating weakness as noise.",
            "mechanism": "Higher implied volatility usually reflects wider expected trading ranges, more fragile liquidity, and faster repricing risk.",
        },
        "SP500": {
            "primary_sleeves": ["global_equity_core"],
            "secondary_sleeves": [],
            "benchmark_context": "Broad equity risk appetite affects core-equity watch context first.",
            "policy_relevance": "high relevance to target sleeves",
            "follow_up": "monitor",
            "category": "Equity breadth and risk appetite",
            "interpretation": "Broad equity direction helps separate orderly risk appetite from a more brittle repricing phase.",
            "mechanism": "Index-level equity moves frame whether macro pressure is broadening into equity beta or being absorbed calmly.",
        },
        "DTWEXBGS": {
            "primary_sleeves": ["emerging_markets", "china_satellite"],
            "secondary_sleeves": [],
            "benchmark_context": "A stronger broad USD often tightens EM financial conditions and matters for benchmark-relative EM posture.",
            "policy_relevance": "high relevance to target sleeves",
            "follow_up": "review Blueprint",
            "category": "FX and Singapore context",
            "interpretation": "A stronger USD tightens the external backdrop for EM assets and changes return translation for a non-USD allocator.",
            "mechanism": "USD strength usually increases funding pressure, weighs on EM risk sentiment, and changes local-currency realized returns.",
        },
        "DEXSIUS": {
            "primary_sleeves": ["cash_bills"],
            "secondary_sleeves": ["global_equity_core"],
            "benchmark_context": "USD asset translation into SGD changes the practical experience of a Singapore-based allocator.",
            "policy_relevance": "high relevance to current holdings",
            "follow_up": "review current portfolio",
            "category": "FX and Singapore context",
            "interpretation": "USD-SGD changes the realized experience of global assets for a Singapore-based investor even when underlying fundamentals are stable.",
            "mechanism": "FX translation affects local purchasing power, cash hurdle comparisons, and the local framing of USD asset gains or losses.",
        },
        "IRLTLT01SGM156N": {
            "primary_sleeves": ["cash_bills"],
            "secondary_sleeves": ["IG_bonds"],
            "benchmark_context": "Local Singapore rate context matters for cash reserve posture and local opportunity cost.",
            "policy_relevance": "high relevance to current holdings",
            "follow_up": "review current portfolio",
            "category": "FX and Singapore context",
            "interpretation": "Singapore rates alter local hurdle rates and change how compelling cash-like ballast looks versus extending duration.",
            "mechanism": "Local sovereign yields shape the opportunity cost of risk taking for an SGD-based allocator.",
        },
        "IRLTLT01EZM156N": {
            "primary_sleeves": ["developed_ex_us_optional"],
            "secondary_sleeves": ["IG_bonds"],
            "benchmark_context": "Euro area rates provide a non-US benchmark lens for developed ex-US discount-rate pressure and bond framing.",
            "policy_relevance": "medium relevance",
            "follow_up": "review benchmark assumptions",
            "category": "Rates and inflation",
            "interpretation": "Euro area rates help distinguish whether duration and discount-rate pressure are global or still concentrated in the US context.",
            "mechanism": "A Europe-aware rate signal improves developed ex-US and global benchmark interpretation when US rates no longer tell the whole story.",
        },
        "STI_PROXY": {
            "primary_sleeves": ["global_equity_core"],
            "secondary_sleeves": ["cash_bills"],
            "benchmark_context": "Local Singapore equity context keeps the brief relevant to an SGD allocator rather than relying on US equity alone.",
            "policy_relevance": "medium relevance",
            "follow_up": "monitor",
            "category": "FX and Singapore context",
            "interpretation": "Local Singapore equity direction tests whether the broader risk message is actually showing up in the investor's home market context.",
            "mechanism": "Local-equity confirmation or divergence sharpens judgment on whether global risk appetite is broad or narrowly US-led.",
        },
        "VEA_PROXY": {
            "primary_sleeves": ["developed_ex_us_optional"],
            "secondary_sleeves": ["global_equity_core"],
            "benchmark_context": "Developed ex-US equity participation helps determine whether global equity breadth is truly broad or still US-centric.",
            "policy_relevance": "medium relevance",
            "follow_up": "review benchmark assumptions",
            "category": "Equity breadth and risk appetite",
            "interpretation": "Developed ex-US equity direction helps test whether broad risk appetite is global enough to trust for benchmark framing.",
            "mechanism": "When developed ex-US equities diverge from US leadership, benchmark-relative global equity interpretation becomes less clean.",
        },
        "VXEEMCLS": {
            "primary_sleeves": ["emerging_markets", "china_satellite"],
            "secondary_sleeves": ["convex"],
            "benchmark_context": "EM volatility helps distinguish local stress from broader global-vol moves.",
            "policy_relevance": "high relevance to target sleeves",
            "follow_up": "review Blueprint",
            "category": "EM and China context",
            "interpretation": "Higher EM volatility raises the bar for risk-taking in EM and China sleeves because stress transmission is usually fast and nonlinear.",
            "mechanism": "EM volatility is often an early sign that cross-border funding and sentiment conditions are deteriorating.",
        },
        "DCOILWTICO": {
            "primary_sleeves": ["real_assets"],
            "secondary_sleeves": ["global_equity_core"],
            "benchmark_context": "Oil moves broaden inflation and input-cost context rather than acting as a direct allocation signal.",
            "policy_relevance": "medium relevance",
            "follow_up": "monitor",
            "category": "Cross asset summary",
            "interpretation": "Oil matters when it reinforces or challenges the inflation and growth message already coming from rates and credit.",
            "mechanism": "Energy moves influence inflation expectations, input-cost pressure, and the market's growth-versus-inflation debate.",
        },
    }


def signal_family_relevance_registry() -> dict[str, dict[str, Any]]:
    return {
        "rates and inflation": {
            "primary_allowed": {"IG_bonds", "cash_bills", "real_assets", "developed_ex_us_optional"},
            "secondary_allowed": {"cash_bills", "real_assets", "global_equity_core", "developed_ex_us_optional"},
            "default_policy_relevance": "benchmark watch only",
            "default_benchmark_context": "Rates should only become a sleeve issue when duration, inflation ballast, or reserve hurdles are genuinely moving.",
            "default_interpretation": "Rates matter when they change benchmark duration framing, reserve hurdles, or the case for real-asset ballast.",
            "default_mechanism": "The rates family transmits through duration sensitivity, hurdle-rate shifts, and inflation-versus-growth framing.",
        },
        "credit and liquidity": {
            "primary_allowed": {"IG_bonds", "alternatives", "cash_bills"},
            "secondary_allowed": {"global_equity_core", "convex", "emerging_markets"},
            "default_policy_relevance": "high relevance to target sleeves",
            "default_benchmark_context": "Credit only deserves elevation when funding stress or liquidity deterioration can plausibly transmit into sleeve implementation or pacing.",
            "default_interpretation": "Credit widening matters mainly through financing stress, liquidity tolerance, and defensive ballast quality.",
            "default_mechanism": "Spread widening transmits into execution quality, risk tolerance, and the cleanliness of benchmark-relative readings.",
        },
        "volatility and stress": {
            "primary_allowed": {"convex", "global_equity_core", "emerging_markets", "china_satellite", "cash_bills"},
            "secondary_allowed": {"global_equity_core", "emerging_markets", "china_satellite", "cash_bills", "convex"},
            "default_policy_relevance": "high relevance to target sleeves",
            "default_benchmark_context": "Volatility only becomes benchmark-relevant when it changes whether benchmark weakness is broad stress, protection relevance, or background noise.",
            "default_interpretation": "Stress signals matter through protection relevance and pacing discipline before they become structural thesis changes.",
            "default_mechanism": "Volatility and stress transmit through tighter tolerance for risk, higher protection value, and noisier implementation conditions.",
        },
        "equity breadth and risk appetite": {
            "primary_allowed": {"global_equity_core", "developed_ex_us_optional", "emerging_markets"},
            "secondary_allowed": {"developed_ex_us_optional", "global_equity_core", "emerging_markets"},
            "default_policy_relevance": "medium relevance",
            "default_benchmark_context": "Equity breadth matters when it changes whether benchmark-relative readings reflect broad participation or narrow leadership.",
            "default_interpretation": "Equity moves matter when they improve or weaken the quality of broad participation, not simply because the market is up or down.",
            "default_mechanism": "Breadth and risk appetite change whether sleeve conclusions should be treated as broad confirmation, fragile leadership, or background noise.",
        },
        "fx and singapore context": {
            "primary_allowed": {"cash_bills", "emerging_markets", "china_satellite", "global_equity_core", "IG_bonds"},
            "secondary_allowed": {"cash_bills", "global_equity_core", "IG_bonds", "emerging_markets", "china_satellite"},
            "default_policy_relevance": "medium relevance",
            "default_benchmark_context": "FX is mainly a local allocator framing issue unless it is clearly transmitting into EM funding pressure or reserve-hurdle comparisons.",
            "default_interpretation": "FX and Singapore context matter through translation, local hurdle comparisons, and EM funding sensitivity.",
            "default_mechanism": "Currency and local-rate changes affect realized SGD experience, reserve comparisons, and the cleanliness of global benchmark interpretation.",
        },
        "em and china context": {
            "primary_allowed": {"emerging_markets", "china_satellite", "convex", "cash_bills"},
            "secondary_allowed": {"convex", "cash_bills", "global_equity_core"},
            "default_policy_relevance": "high relevance to target sleeves",
            "default_benchmark_context": "EM and China context matters when stress transmission changes pacing, implementation quality, or concentration pressure without automatically breaking the structural sleeve role.",
            "default_interpretation": "EM and China signals should be read first as pacing and stress-transmission inputs, not immediate long-horizon thesis failure.",
            "default_mechanism": "EM and China stress usually reaches the portfolio through funding pressure, concentration risk, and sequencing of deployment rather than immediate strategic invalidation.",
        },
        "cross asset summary": {
            "primary_allowed": {"real_assets", "cash_bills", "convex", "IG_bonds", "global_equity_core"},
            "secondary_allowed": {"real_assets", "cash_bills", "convex", "IG_bonds", "global_equity_core", "emerging_markets", "china_satellite"},
            "default_policy_relevance": "background relevance only",
            "default_benchmark_context": "Cross-asset synthesis should only elevate when multiple families are jointly pointing to the same investor consequence.",
            "default_interpretation": "Cross-asset summaries matter only when they synthesize several active families into one consequence.",
            "default_mechanism": "Synthesis is useful when rates, stress, credit, FX, or breadth are confirming a broader investor consequence together.",
        },
    }


def cross_asset_benchmark_cases() -> dict[frozenset[str], str]:
    return {
        frozenset({"rates and inflation", "credit and liquidity"}): (
            "Benchmark effect (moderate): rates and credit are jointly tightening the benchmark lens, so duration and ballast comparison should be read more as financing pressure than as a clean attribution signal."
        ),
        frozenset({"rates and inflation", "fx and singapore context"}): (
            "Benchmark effect (moderate): rates and local-currency context are jointly shifting the allocator hurdle, so benchmark-relative returns matter more for reserve versus deployment framing than for short-term judgment."
        ),
        frozenset({"rates and inflation", "credit and liquidity", "fx and singapore context"}): (
            "Benchmark effect (strong): rates, credit, and FX are jointly distorting the benchmark lens, so reserve-versus-duration framing and EM benchmark comparison are both less clean than any single signal would imply."
        ),
        frozenset({"rates and inflation", "credit and liquidity", "volatility and stress"}): (
            "Benchmark effect (strong): rates, spreads, and volatility are all reinforcing the same stress regime, so benchmark-relative moves should be read mainly through defensive fit and implementation resilience rather than near-term attribution."
        ),
        frozenset({"volatility and stress", "equity breadth and risk appetite", "em and china context"}): (
            "Benchmark effect (strong): volatility, breadth, and EM stress are confirming the same fragility, so benchmark-relative weakness is more useful for sequencing risk and protection review than for attribution alone."
        ),
        frozenset({"volatility and stress", "em and china context"}): (
            "Benchmark effect (moderate): volatility and EM stress are reinforcing each other, so benchmark-relative EM weakness is more useful for pacing and protection review than for immediate candidate judgment."
        ),
        frozenset({"rates and inflation", "volatility and stress"}): (
            "Benchmark effect (moderate): rate pressure and volatility are moving together, so benchmark-relative weakness should be read more as implementation stress than as a clean attribution signal."
        ),
        frozenset({"credit and liquidity", "volatility and stress"}): (
            "Benchmark effect (strong): spreads and volatility are reinforcing the same stress regime, so benchmark comparison is more useful for defensive fit and funding tolerance than for near-term winner-loser attribution."
        ),
        frozenset({"equity breadth and risk appetite", "fx and singapore context"}): (
            "Benchmark effect (moderate): global breadth and local-currency context are diverging enough that benchmark-relative returns should be read with more emphasis on allocator experience and less on headline equity attribution."
        ),
        frozenset({"equity breadth and risk appetite", "em and china context"}): (
            "Benchmark effect (moderate): breadth and EM stress are diverging enough that benchmark-relative EM and global-equity comparison should emphasize stress transmission and participation quality rather than one broad risk-on or risk-off story."
        ),
        frozenset({"rates and inflation", "equity breadth and risk appetite", "fx and singapore context"}): (
            "Benchmark effect (strong): rates, equity breadth, and local-currency context are jointly shifting the hurdle for global deployment, so benchmark framing is more about sleeve pacing and allocator fit than simple performance comparison."
        ),
    }


def signal_family_transmission_registry() -> dict[str, dict[str, Any]]:
    return {
        "rates and inflation": {
            "economic_driver": "discount-rate repricing, inflation hurdle shifts, and reserve-versus-duration trade-offs",
            "primary_relevance_rule": "raise primary relevance when duration, reserve hurdles, or inflation ballast framing are directly moving",
            "secondary_relevance_rule": "allow secondary relevance when rates pressure broadens into non-US discount-rate context or real-asset ballast framing",
            "background_rule": "keep as background when curve or inflation moves stay isolated and benchmark-only",
        },
        "credit and liquidity": {
            "economic_driver": "funding stress, spread dislocation, and implementation resilience",
            "primary_relevance_rule": "raise primary relevance when spread widening or liquidity stress directly changes ballast quality or execution tolerance",
            "secondary_relevance_rule": "allow secondary relevance when equity or protection sleeves inherit the same financing pressure",
            "background_rule": "keep as background when spread moves are small and unconfirmed by volatility or dollar tightening",
        },
        "volatility and stress": {
            "economic_driver": "risk tolerance compression, protection value, and deployment pacing discipline",
            "primary_relevance_rule": "raise primary relevance when volatility changes protection relevance or directly slows deployment pacing",
            "secondary_relevance_rule": "allow secondary relevance when stress only changes benchmark cleanliness or broad monitoring posture",
            "background_rule": "keep as background when volatility stays elevated but unconfirmed by breadth, credit, or EM stress",
        },
        "equity breadth and risk appetite": {
            "economic_driver": "quality of broad participation versus fragile leadership",
            "primary_relevance_rule": "raise primary relevance when breadth changes the trustworthiness of core-equity or developed ex-US confirmation",
            "secondary_relevance_rule": "allow secondary relevance when breadth only shifts benchmark framing or pacing for higher-beta sleeves",
            "background_rule": "keep as background when leadership remains narrow but no corroborating stress signal exists",
        },
        "fx and singapore context": {
            "economic_driver": "local-currency translation, SGD hurdle comparisons, and EM funding transmission",
            "primary_relevance_rule": "raise primary relevance when FX changes reserve posture, local hurdle comparison, or EM funding conditions",
            "secondary_relevance_rule": "allow secondary relevance when translation mainly affects allocator experience rather than asset fundamentals",
            "background_rule": "keep as background when FX moves are present but not changing pacing, reserve, or EM stress interpretation",
        },
        "em and china context": {
            "economic_driver": "stress transmission, concentration pressure, and deployment sequencing",
            "primary_relevance_rule": "raise primary relevance when EM or China stress changes pacing, concentration tolerance, or implementation fit",
            "secondary_relevance_rule": "allow secondary relevance when the same stress only raises benchmark watch or protection awareness",
            "background_rule": "keep as background when EM or China weakness is isolated and not corroborated by FX, volatility, or credit",
        },
        "cross asset summary": {
            "economic_driver": "multi-family confirmation of one broader investor consequence",
            "primary_relevance_rule": "raise primary relevance only when several families confirm the same sleeve consequence",
            "secondary_relevance_rule": "allow secondary relevance when synthesis improves framing but not direct review urgency",
            "background_rule": "keep as background when synthesis only restates one nearby family without broader confirmation",
        },
    }


def benchmark_effect_family_registry() -> dict[str, dict[str, Any]]:
    return {
        "duration benchmark pressure": {
            "issue_type": "sleeve framing",
            "driver": "rates",
        },
        "broad equity benchmark breadth pressure": {
            "issue_type": "attribution",
            "driver": "breadth",
        },
        "em benchmark stress transmission": {
            "issue_type": "pacing",
            "driver": "EM stress",
        },
        "china benchmark concentration pressure": {
            "issue_type": "implementation comparison",
            "driver": "China concentration",
        },
        "cross asset benchmark fragility": {
            "issue_type": "sleeve framing",
            "driver": "cross-asset confirmation",
        },
        "local currency benchmark translation relevance": {
            "issue_type": "pacing",
            "driver": "FX",
        },
        "credit benchmark liquidity pressure": {
            "issue_type": "implementation comparison",
            "driver": "credit and liquidity",
        },
        "protection benchmark stress framing": {
            "issue_type": "sleeve framing",
            "driver": "volatility",
        },
    }


def select_benchmark_effect_family(family: str, code: str, category: str) -> str:
    normalized_family = str(family or "").strip().lower()
    normalized_code = str(code or "").strip().upper()
    normalized_category = str(category or "").strip().lower()
    if normalized_family == "rates and inflation":
        return "duration benchmark pressure"
    if normalized_family == "credit and liquidity":
        return "credit benchmark liquidity pressure"
    if normalized_family == "volatility and stress":
        return "protection benchmark stress framing"
    if normalized_family == "equity breadth and risk appetite":
        return "broad equity benchmark breadth pressure"
    if normalized_family == "fx and singapore context":
        return "local currency benchmark translation relevance"
    if normalized_family == "em and china context":
        if normalized_code == "CHINA_STRESS" or normalized_code.startswith("CHINA_"):
            return "china benchmark concentration pressure"
        return "em benchmark stress transmission"
    return "cross asset benchmark fragility"


def holdings_proxy_basis(holdings_state: str) -> str:
    normalized = str(holdings_state or "").strip().lower()
    if normalized == "partial holdings mapped":
        return "partial current holdings mapping plus target-sleeve design"
    if normalized == "benchmark watch proxy only":
        return "benchmark-watch context with no direct realized holdings confirmation, so the signal should be treated as framing pressure rather than a confirmed portfolio effect"
    if normalized == "holdings unavailable, target sleeve proxy only":
        return "Blueprint target sleeve design and benchmark context instead of a live positions read, so the consequence is about intended portfolio structure rather than confirmed realized exposure"
    if normalized == "target sleeve relevance only":
        return "target sleeve design first, with benchmark context used only as supporting evidence for likely implementation relevance"
    if normalized == "no meaningful holdings consequence inferred":
        return "no direct holdings consequence can be justified, so only background sleeve or benchmark context should be used"
    return "conservative target-sleeve relevance only"


def holdings_state_registry() -> dict[str, dict[str, str]]:
    return {
        "direct holdings benchmark pressure": {
            "confidence": "high",
            "reason_template": "Current holdings are directly exposed through {names}, and benchmark framing is part of the consequence.",
            "fallback_reason": "",
        },
        "direct holdings implementation pressure": {
            "confidence": "high",
            "reason_template": "Current holdings are directly exposed through {names}, and implementation quality matters now.",
            "fallback_reason": "",
        },
        "direct holdings exposure pressure": {
            "confidence": "high",
            "reason_template": "Current holdings are directly exposed through {names}.",
            "fallback_reason": "",
        },
        "partial holdings mapped": {
            "confidence": "medium",
            "reason_template": "Some current positions map into the relevant sleeves, but portfolio coverage is incomplete for a clean holdings-level conclusion.",
            "fallback_reason": "Using partial sleeve mapping plus benchmark context rather than a full realized holdings read.",
        },
        "no meaningful holdings consequence inferred": {
            "confidence": "low",
            "reason_template": "A direct holdings consequence is not observable from the current mapping.",
            "fallback_reason": f"Proxy basis: {holdings_proxy_basis('no meaningful holdings consequence inferred')}; no stronger holdings-level inference is justified.",
        },
        "target sleeve relevance only": {
            "confidence": "medium",
            "reason_template": "No direct holdings exposure is currently available, so the consequence is being inferred from target sleeve design rather than confirmed against positions.",
            "fallback_reason": f"Proxy basis: {holdings_proxy_basis('target sleeve relevance only')}.",
        },
        "holdings unavailable, target sleeve proxy only": {
            "confidence": "low",
            "reason_template": "Current holdings are not available in the active snapshot, so there is no direct realized-exposure check for this consequence.",
            "fallback_reason": f"Proxy basis: {holdings_proxy_basis('holdings unavailable, target sleeve proxy only')}.",
        },
        "benchmark watch proxy only": {
            "confidence": "medium",
            "reason_template": "The strongest live effect is on benchmark framing rather than on a confirmed current holdings exposure.",
            "fallback_reason": f"Proxy basis: {holdings_proxy_basis('benchmark watch proxy only')}.",
        },
        "likely implementation relevance but unconfirmed at holdings level": {
            "confidence": "medium",
            "reason_template": "The signal is strong enough to suggest likely implementation relevance, but current holdings coverage is not sufficient to confirm realized exposure.",
            "fallback_reason": "Using target sleeve design plus benchmark framing as the best available implementation proxy.",
        },
    }


def signal_family_for_card(metric_code: str, category: str) -> str:
    metric = str(metric_code or "").upper()
    category_text = str(category or "").strip().lower()
    registry_category = str(metric_signal_registry().get(metric, {}).get("category") or "").strip()
    if registry_category:
        return registry_category.lower()
    if category_text:
        return category_text
    return "cross asset summary"


def signal_direction(row: dict[str, Any]) -> str:
    change_5obs = row.get("short_horizon", {}).get("change_5obs")
    if change_5obs is None:
        return "stable"
    if float(change_5obs) > 0.1:
        return "rising"
    if float(change_5obs) < -0.1:
        return "falling"
    return "stable"


def signal_pressure(row: dict[str, Any]) -> str:
    percentile = row.get("long_horizon", {}).get("percentile_10y")
    if percentile is None:
        return "normal"
    percentile = float(percentile)
    if percentile >= 90:
        return "extreme_high"
    if percentile >= 75:
        return "elevated"
    if percentile <= 10:
        return "extreme_low"
    if percentile <= 25:
        return "depressed"
    return "normal"


def corroborating_codes(code: str, row_map: dict[str, dict[str, Any]]) -> list[str]:
    family = signal_family_for_card(code, str(metric_signal_registry().get(code, {}).get("category") or ""))
    matches: list[str] = []
    for other_code, other_row in row_map.items():
        if str(other_code or "").upper() == str(code or "").upper():
            continue
        other_family = signal_family_for_card(str(other_code), str(metric_signal_registry().get(str(other_code).upper(), {}).get("category") or ""))
        if other_family != family:
            continue
        pressure = signal_pressure(other_row)
        direction = signal_direction(other_row)
        if pressure in {"elevated", "extreme_high", "depressed", "extreme_low"} or direction != "stable":
            matches.append(str(other_code).upper())
    return matches


def blueprint_present_sleeves(policy_pack: dict[str, Any]) -> set[str]:
    present: set[str] = set()
    for item in list(policy_pack.get("portfolio_relevance") or []):
        if bool(item.get("blueprint_present")):
            present.add(str(item.get("sleeve_tag") or ""))
    return present


def metric_relevance(code: str) -> dict[str, Any]:
    mapping = metric_signal_registry()
    return mapping.get(
        code,
        {
            "primary_sleeves": [],
            "secondary_sleeves": [],
            "benchmark_context": "Background market context only.",
            "policy_relevance": "low relevance background context",
            "follow_up": "no action",
            "category": "Cross asset summary",
            "interpretation": "Contextual observation without a strong direct sleeve implication.",
            "mechanism": "This signal currently adds background context rather than a decisive portfolio message.",
        },
    )


def derive_metric_relevance(
    code: str,
    row: dict[str, Any],
    row_map: dict[str, dict[str, Any]],
    policy_pack: dict[str, Any],
) -> dict[str, Any]:
    base = dict(metric_relevance(code))
    family = str(signal_family_for_card(code, str(base.get("category") or "")))
    family_key = family.lower()
    registry = dict(signal_family_relevance_registry().get(family_key) or {})
    primary: list[str] = []
    secondary: list[str] = []
    benchmark_context = str(registry.get("default_benchmark_context") or base.get("benchmark_context") or "")
    policy_relevance = str(registry.get("default_policy_relevance") or base.get("policy_relevance") or "background relevance only")
    interpretation = str(registry.get("default_interpretation") or base.get("interpretation") or "")
    mechanism = str(registry.get("default_mechanism") or base.get("mechanism") or "")
    direction = signal_direction(row)
    pressure = signal_pressure(row)
    corroborators = corroborating_codes(code, row_map)
    benchmark_strength = "contextual"
    transmission_registry = dict(signal_family_transmission_registry().get(family_key) or {})
    primary_allowed = {str(item) for item in registry.get("primary_allowed") or set()}
    secondary_allowed = {str(item) for item in registry.get("secondary_allowed") or set()}
    present_sleeves = blueprint_present_sleeves(policy_pack)

    def add_primary(*sleeves: str) -> None:
        for sleeve in sleeves:
            if primary_allowed and sleeve not in primary_allowed:
                continue
            if sleeve and sleeve not in primary:
                primary.append(sleeve)
            if sleeve in secondary:
                secondary.remove(sleeve)

    def add_secondary(*sleeves: str) -> None:
        for sleeve in sleeves:
            if secondary_allowed and sleeve not in secondary_allowed:
                continue
            if sleeve and sleeve not in primary and sleeve not in secondary:
                secondary.append(sleeve)

    if family_key == "rates and inflation":
        benchmark_strength = "strong" if pressure in {"elevated", "extreme_high", "depressed", "extreme_low"} or direction != "stable" else "moderate"
        if code == "DGS10":
            add_primary("IG_bonds")
            add_secondary("cash_bills")
            if "T10YIE" in corroborators:
                add_secondary("real_assets")
                benchmark_context = "Duration assumptions and inflation-sensitive benchmark framing are both under pressure, so rates are no longer a clean bond-only signal."
            elif pressure in {"extreme_high", "elevated"}:
                benchmark_context = "Duration-sensitive benchmark assumptions are under strong pressure, and reserve sleeves matter more because long-duration ballast is less clean."
            interpretation = "Higher long yields now matter mainly through bond benchmark pressure and duration implementation quality, not just as a generic rates warning."
            mechanism = "When long yields rise with corroborating inflation or funding signals, benchmark duration and reserve sleeve comparisons become meaningfully less clean."
        elif code == "T10YIE":
            add_primary("real_assets")
            add_secondary("IG_bonds")
            if "DCOILWTICO" in corroborators:
                add_secondary("cash_bills")
            benchmark_context = "Benchmark framing matters because inflation compensation can weaken nominal-duration comparisons and strengthen inflation ballast context."
            benchmark_strength = "moderate" if pressure in {"elevated", "extreme_high"} else "contextual"
            interpretation = "Inflation compensation matters here as a sleeve-framing input: it changes whether nominal duration or real-asset ballast deserves more attention."
        elif code == "T10Y2Y":
            if pressure in {"elevated", "extreme_high", "depressed", "extreme_low"}:
                add_primary("IG_bonds")
            add_secondary("cash_bills")
            benchmark_context = "Curve shape is mainly a benchmark framing and implementation question, not a direct sleeve-thesis change by itself."
            benchmark_strength = "moderate"
            policy_relevance = "benchmark watch only" if pressure == "normal" else policy_relevance
        elif code == "IRLTLT01EZM156N":
            add_primary("developed_ex_us_optional")
            add_secondary("IG_bonds", "global_equity_core")
            benchmark_strength = "moderate" if pressure in {"elevated", "extreme_high", "depressed", "extreme_low"} or direction != "stable" else "contextual"
            benchmark_context = "Euro-area duration context matters when developed ex-US benchmark comparisons need something more than a US-only rates lens."
            interpretation = "European long yields matter here as a developed ex-US benchmark and discount-rate context, not as a direct portfolio instruction by themselves."
            mechanism = "When euro-area yields move alongside US rates or non-US equity confirmation, the investor gets a cleaner read on whether pressure is global or still US-centric."
    elif family_key == "credit and liquidity":
        add_primary("IG_bonds")
        add_secondary("cash_bills", "alternatives")
        if "VIXCLS" in corroborators or "DTWEXBGS" in corroborators:
            add_secondary("global_equity_core", "convex")
            benchmark_context = "Credit stress is now a stronger cross-asset benchmark issue because spreads, volatility, or USD conditions are jointly pointing to tighter financing conditions."
            benchmark_strength = "strong"
        else:
            benchmark_context = "Credit spreads primarily affect defensive ballast quality and implementation resilience rather than broad strategic role changes."
            benchmark_strength = "moderate"
        interpretation = "Credit widening matters as funding-stress transmission, not just as a bond-market footnote."
    elif family_key == "volatility and stress":
        benchmark_strength = "moderate"
        if code == "VIXCLS":
            add_primary("convex")
            add_secondary("global_equity_core")
            if "VXEEMCLS" in corroborators:
                add_secondary("emerging_markets", "china_satellite")
            if "BAMLH0A0HYM2" in corroborators:
                add_secondary("cash_bills")
                benchmark_strength = "strong"
            interpretation = "Volatility matters here mostly through protection relevance and deployment pacing discipline rather than by itself invalidating core equity sleeves."
            benchmark_context = "Benchmark watch is meaningful because volatility changes how cleanly broad-equity weakness should be read, especially when credit also deteriorates."
        elif code == "VXEEMCLS":
            add_primary("emerging_markets", "china_satellite")
            add_secondary("convex")
            if "DTWEXBGS" in corroborators:
                add_secondary("cash_bills")
            benchmark_strength = "strong" if "DTWEXBGS" in corroborators else "moderate"
            benchmark_context = "EM volatility affects EM benchmark interpretation directly by changing whether underperformance looks like local stress, broad risk aversion, or funding pressure."
            interpretation = "EM volatility matters as pacing and stress-transmission evidence first; it does not automatically break the long-horizon EM role."
    elif family_key == "equity breadth and risk appetite":
        add_primary("global_equity_core")
        if pressure in {"elevated", "extreme_high", "depressed", "extreme_low"}:
            add_secondary("developed_ex_us_optional")
        if "VIXCLS" in corroborators or "BAMLH0A0HYM2" in corroborators:
            add_secondary("emerging_markets")
            benchmark_strength = "moderate"
        else:
            benchmark_strength = "contextual"
        benchmark_context = "Equity benchmark interpretation matters when breadth and risk appetite either confirm or undermine the idea that core equity weakness is broad enough to matter."
        interpretation = "Equity direction matters only when it changes the quality of broad risk appetite, not as a stand-alone instruction for the core sleeve."
        if code == "VEA_PROXY":
            add_primary("developed_ex_us_optional")
            add_secondary("global_equity_core")
            if "SP500" in corroborators:
                benchmark_strength = "strong"
            benchmark_context = "Developed ex-US participation matters because it shows whether broad equity confirmation is global or still dominated by US leadership."
            interpretation = "Non-US developed equity confirmation matters as a breadth-quality check for the global equity system, especially when it diverges from US leadership."
            mechanism = "If developed ex-US participation weakens while US breadth still holds, benchmark-relative global equity interpretation becomes less clean."
    elif family_key == "fx and singapore context":
        if code == "DTWEXBGS":
            add_primary("emerging_markets", "china_satellite")
            add_secondary("cash_bills")
            benchmark_strength = "strong"
            benchmark_context = "Broad USD pressure changes EM benchmark interpretation first and only then spills into pacing and local allocator comparisons."
            interpretation = "Broad USD moves matter because they tighten EM financial conditions and make benchmark-relative EM signals more demanding."
        elif code == "DEXSIUS":
            add_primary("cash_bills")
            add_secondary("global_equity_core", "IG_bonds")
            benchmark_strength = "moderate"
            benchmark_context = "FX translation is mainly a local allocator framing issue: it changes how global benchmark-relative returns are experienced in SGD rather than changing underlying asset truth."
            interpretation = "SGD translation matters through local hurdle comparison and realized allocator experience more than through asset-level structural change."
        elif code == "IRLTLT01SGM156N":
            add_primary("cash_bills")
            add_secondary("IG_bonds")
            benchmark_strength = "moderate"
            benchmark_context = "Singapore-rate context affects reserve and duration comparison logic by changing local opportunity cost."
            interpretation = "Local sovereign yields matter because they change the hurdle for holding cash-like reserves versus extending duration."
        elif code == "STI_PROXY":
            add_primary("global_equity_core")
            add_secondary("cash_bills")
            if "SP500" in corroborators:
                benchmark_strength = "moderate"
                benchmark_context = "Local equity confirmation helps show whether global risk appetite is broad enough to matter for an SGD-based allocator rather than remaining US-led."
            else:
                benchmark_context = "Local equity context is useful, but mostly as a confirmation check rather than a primary portfolio signal."
            interpretation = "Singapore context matters when local equity confirmation changes the confidence of broader equity-risk readings."
    elif family_key == "em and china context":
        add_primary("emerging_markets", "china_satellite")
        add_secondary("convex")
        if "DTWEXBGS" in corroborators:
            add_secondary("cash_bills")
            benchmark_strength = "strong"
        else:
            benchmark_strength = "moderate"
        benchmark_context = "EM and China benchmark interpretation matters because current stress can affect pacing and implementation quality without automatically becoming a structural thesis break."
        interpretation = "EM and China moves should be read first as stress transmission and pacing context, not as automatic long-horizon thesis rejection."
    else:
        benchmark_strength = "contextual"
        if code == "DCOILWTICO":
            add_primary("real_assets")
            if "T10YIE" in corroborators:
                add_secondary("cash_bills", "IG_bonds")
                benchmark_strength = "moderate"
            benchmark_context = "Cross-asset synthesis matters when energy is reinforcing inflation and rates signals rather than acting as an isolated commodity move."
            interpretation = "Oil only matters here when it changes the wider inflation-growth mix already affecting portfolio sleeves."

    primary = [sleeve for sleeve in primary if sleeve in present_sleeves] or [sleeve for sleeve in list(base.get("primary_sleeves") or []) if sleeve in present_sleeves]
    secondary = [sleeve for sleeve in secondary if sleeve in present_sleeves and sleeve not in primary]
    if not primary and not secondary and str(base.get("policy_relevance") or "").startswith("benchmark watch"):
        policy_relevance = "benchmark watch only"
    elif primary:
        policy_relevance = "high relevance to target sleeves"
    elif secondary:
        policy_relevance = "medium relevance"
    elif benchmark_context:
        policy_relevance = "benchmark watch only"
    else:
        policy_relevance = "background relevance only"

    transmission_path = str(transmission_registry.get("economic_driver") or mechanism or interpretation)
    sleeve_relevance_class = (
        "primary target sleeve" if primary else ("secondary target sleeve" if secondary else ("benchmark watch only" if benchmark_context else "background relevance only"))
    )
    benchmark_relevance_class = (
        "strong"
        if benchmark_strength == "strong"
        else ("moderate" if benchmark_strength == "moderate" else ("contextual" if benchmark_context else "none"))
    )
    review_relevance = "background only"
    if policy_relevance == "high relevance to target sleeves":
        review_relevance = "review now"
    elif policy_relevance == "medium relevance":
        review_relevance = "review if persistence builds"
    elif policy_relevance == "benchmark watch only":
        review_relevance = "benchmark watch"

    return {
        **base,
        "category": family.title(),
        "primary_sleeves": primary,
        "secondary_sleeves": secondary,
        "benchmark_context": benchmark_context,
        "benchmark_strength": benchmark_strength,
        "policy_relevance": policy_relevance,
        "interpretation": interpretation,
        "mechanism": mechanism,
        "signal_direction": direction,
        "signal_pressure": pressure,
        "corroborating_codes": corroborators,
        "transmission_path": transmission_path,
        "sleeve_relevance_class": sleeve_relevance_class,
        "benchmark_relevance_class": benchmark_relevance_class,
        "review_relevance": review_relevance,
        "relevance_why": {
            "economic_driver": transmission_path,
            "primary_relevance_rule": str(transmission_registry.get("primary_relevance_rule") or ""),
            "secondary_relevance_rule": str(transmission_registry.get("secondary_relevance_rule") or ""),
            "background_rule": str(transmission_registry.get("background_rule") or ""),
        },
    }
