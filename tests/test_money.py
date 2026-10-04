# tests/test_money.py
"""Тесты money.py: округления, минимумы, сплиты, формат API-строк."""
from decimal import Decimal

import pytest

from trading.money import (
    D,
    MoneyError,
    ceil_to_step,
    compute_entry_qty,
    floor_to_step,
    round_price_tick,
    to_api_str,
    validate_split,
)
from trading.types import RejectReason, Side, SymbolFilters


def _filters(
    min_qty: str = "0.1", step: str = "0.1", max_qty: str = "100000",
    tick: str = "0.0001", min_notional: str = "5",
) -> SymbolFilters:
    return SymbolFilters(
        symbol="TESTUSDT", status="TRADING",
        tick_size=Decimal(tick), step_size=Decimal(step),
        min_qty=Decimal(min_qty), max_qty=Decimal(max_qty),
        min_notional=Decimal(min_notional),
        price_precision=4, quantity_precision=1,
    )


class TestDecimalHelpers:
    def test_d_rejects_nan_inf(self) -> None:
        with pytest.raises(MoneyError):
            D("NaN")
        with pytest.raises(MoneyError):
            D("Infinity")

    def test_floor_to_step(self) -> None:
        assert floor_to_step(Decimal("61.847"), Decimal("0.1")) == Decimal("61.8")
        assert floor_to_step(Decimal("62.5"), Decimal("0.1")) == Decimal("62.5")
        assert floor_to_step(Decimal("0.93"), Decimal("1")) == Decimal("0")

    def test_ceil_to_step(self) -> None:
        assert ceil_to_step(Decimal("1.23456"), Decimal("0.0001")) == Decimal("1.2346")

    def test_round_price_tick_direction(self) -> None:
        p, tick = Decimal("1.23456"), Decimal("0.0001")
        # LONG -> floor (SL дальше / TP ближе); SHORT -> ceil
        assert round_price_tick(p, Side.LONG, tick) == Decimal("1.2345")
        assert round_price_tick(p, Side.SHORT, tick) == Decimal("1.2346")

    def test_to_api_str(self) -> None:
        assert to_api_str(Decimal("62.40")) == "62.4"
        assert to_api_str(Decimal("0.00000001")) == "0.00000001"


class TestComputeEntryQty:
    def test_ok(self) -> None:
        qty, reason = compute_entry_qty(Decimal("20"), Decimal("0.3202"), _filters())
        assert qty == Decimal("62.4")  # 20/0.3202 = 62.461... -> floor(0.1)
        assert reason is None

    def test_below_min_qty(self) -> None:
        qty, reason = compute_entry_qty(Decimal("20"), Decimal("0.3202"), _filters(min_qty="2"))
        assert qty is None
        assert reason is RejectReason.QTY_BELOW_MIN

    def test_notional_below_min(self) -> None:
        qty, reason = compute_entry_qty(
            Decimal("20"), Decimal("0.32"), _filters(min_notional="100")
        )
        assert qty is None
        assert reason is RejectReason.NOTIONAL_BELOW_MIN

    def test_above_max_qty(self) -> None:
        qty, reason = compute_entry_qty(Decimal("20"), Decimal("0.3202"), _filters(max_qty="50"))
        assert qty is None
        assert reason is RejectReason.QTY_ABOVE_MAX


class TestValidateSplit:
    def test_ok(self) -> None:
        assert validate_split(Decimal("10"), Decimal("5"), Decimal("2"), _filters()) is True

    def test_share_below_min_qty(self) -> None:
        assert validate_split(Decimal("10.1"), Decimal("0.2"), Decimal("2"), _filters()) is False

    def test_remainder_below_notional(self) -> None:
        # остаток 0.1 * цена 2 = 0.2 USDT < minNotional 5
        assert validate_split(Decimal("10.2"), Decimal("10.1"), Decimal("2"), _filters()) is False
