from app.models.types import ConvexSleevePosition
from app.services.convex_engine import validate_retail_safe_convex


def test_convex_policy_rejects_margin() -> None:
    positions = [
        ConvexSleevePosition(
            symbol="BAD",
            allocation_weight=0.03,
            retail_accessible=True,
            margin_required=True,
            max_loss_known=True,
            instrument_type="managed_futures_etf",
        )
    ]

    result = validate_retail_safe_convex(positions)
    assert not result.valid
    assert any("requires margin" in error for error in result.errors)


def test_convex_policy_accepts_target_mix() -> None:
    positions = [
        ConvexSleevePosition(
            symbol="DBMF",
            allocation_weight=0.02,
            retail_accessible=True,
            margin_required=False,
            max_loss_known=True,
            instrument_type="managed_futures_etf",
        ),
        ConvexSleevePosition(
            symbol="TAIL",
            allocation_weight=0.007,
            retail_accessible=True,
            margin_required=False,
            max_loss_known=True,
            instrument_type="tail_hedge_fund",
        ),
        ConvexSleevePosition(
            symbol="PUT",
            allocation_weight=0.003,
            retail_accessible=True,
            margin_required=False,
            max_loss_known=True,
            instrument_type="long_put_option",
        ),
    ]

    result = validate_retail_safe_convex(positions)
    assert result.valid
