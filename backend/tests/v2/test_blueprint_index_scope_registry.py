from app.v2.surfaces.blueprint.index_scope_registry import resolve_index_scope_explainer


CURRENT_BLUEPRINT_SYMBOLS = [
    "CSPX",
    "SSAC",
    "IWDA",
    "VWRA",
    "VWRL",
    "VEVE",
    "EIMI",
    "VFEA",
    "HMCH",
    "XCHA",
    "A35",
    "AGGU",
    "VAGU",
    "BIL",
    "BILS",
    "IB01",
    "SGOV",
    "CMOD",
    "IWDP",
    "SGLN",
    "DBMF",
    "KMLM",
    "TAIL",
    "CAOS",
]


def test_current_blueprint_symbols_resolve_index_scope_explainer() -> None:
    for symbol in CURRENT_BLUEPRINT_SYMBOLS:
        scope = resolve_index_scope_explainer(
            symbol=symbol,
            sleeve_key="global_equity_core",
            exposure_label="Candidate exposure",
        )
        assert scope
        assert scope["display_title"]
        assert scope["summary"]
        assert scope["source_basis"] == "candidate_registry"


def test_tail_risk_candidates_do_not_collapse_to_plain_index_scope() -> None:
    tail = resolve_index_scope_explainer(
        symbol="TAIL",
        benchmark_full_name="Bloomberg Barclays Short Treasury Index",
        exposure_label="Tail risk",
    )
    caos = resolve_index_scope_explainer(
        symbol="CAOS",
        benchmark_full_name="S&P 500 Index",
        exposure_label="Tail risk",
    )

    assert tail["scope_type"] == "tail_risk_strategy"
    assert tail["label"] == "Exposure scope"
    assert caos["scope_type"] == "tail_risk_strategy"
    assert caos["label"] == "Exposure scope"


def test_unknown_candidate_falls_back_to_asset_class_scope() -> None:
    scope = resolve_index_scope_explainer(
        symbol="UNKNOWN",
        sleeve_key="ig_bonds",
        exposure_label="Fixed income",
    )

    assert scope["source_basis"] == "sleeve_asset_class_fallback"
    assert scope["scope_type"] == "fallback_asset_class"
    assert scope["summary"]
