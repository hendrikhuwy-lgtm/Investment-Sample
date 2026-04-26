from __future__ import annotations

from app.services.portfolio_blueprint import build_portfolio_blueprint_payload


def _sleeve(payload: dict, key: str) -> dict:
    return next(item for item in payload["sleeves"] if item["sleeve_key"] == key)


def test_cash_bills_supports_mixed_instrument_types_with_fallbacks() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = _sleeve(payload, "cash_bills")
    candidates = list(sleeve.get("candidates") or [])
    placeholders = list(sleeve.get("policy_placeholders") or [])
    live_types = {str(item.get("instrument_type")) for item in candidates}
    placeholder_types = {str(item.get("instrument_type")) for item in placeholders}

    assert "etf_ucits" in live_types
    assert {"t_bill_sg", "money_market_fund_sg", "cash_account_sg"}.issubset(placeholder_types)

    assert any(str(item.get("verification_status")) in {"verified", "partially_verified"} for item in candidates)
    assert all(item.get("verification_status") is None for item in placeholders)


def test_convex_has_all_policy_buckets_and_long_put_placeholder() -> None:
    payload = build_portfolio_blueprint_payload()
    sleeve = _sleeve(payload, "convex")
    candidates = list(sleeve.get("candidates") or [])
    strategy_placeholders = list(sleeve.get("strategy_placeholders") or [])

    bucket_keys = {str(item.get("bucket")) for item in candidates}
    assert {"managed_futures", "tail_hedge"}.issubset(bucket_keys)
    assert any(str(item.get("bucket")) == "long_put" for item in strategy_placeholders)

    long_put = next(item for item in strategy_placeholders if str(item.get("bucket")) == "long_put")
    assert str(long_put.get("instrument_type")) == "long_put_overlay_strategy"
    assert long_put.get("margin_required") is False
    assert long_put.get("max_loss_known") is True
    assert long_put.get("short_options") is False
    assert str(long_put.get("fallback_routing") or "")


def test_non_etf_candidates_can_be_partially_verified_and_unscored() -> None:
    payload = build_portfolio_blueprint_payload()
    cash_bills = _sleeve(payload, "cash_bills")
    cash_placeholder = next(
        item for item in cash_bills.get("policy_placeholders", []) if str(item.get("instrument_type")) == "cash_account_sg"
    )

    assert cash_placeholder.get("verification_status") is None
    assert cash_placeholder.get("verification_missing") in (None, [])
    assert cash_placeholder.get("sg_lens") is None
