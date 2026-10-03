# tests/test_types.py
"""Тесты доменных моделей: инварианты OrderRequest, статусы, clientOrderId."""
from decimal import Decimal

import pytest
from pydantic import ValidationError

from trading.types import (
    OrderKind, OrderRequest, OrderSide, OrderState, make_client_id,
)


class TestOrderRequest:
    def test_market_ok(self) -> None:
        r = OrderRequest(
            client_order_id="wx1-in", symbol="RLCUSDT",
            side=OrderSide.BUY, kind=OrderKind.MARKET, qty=Decimal("62.4"),
        )
        assert r.qty == Decimal("62.4")

    def test_market_rejects_stop_price(self) -> None:
        with pytest.raises(ValidationError):
            OrderRequest(
                client_order_id="wx1-in", symbol="RLCUSDT",
                side=OrderSide.BUY, kind=OrderKind.MARKET,
                qty=Decimal("1"), stop_price=Decimal("1"),
            )

    def test_close_position_excludes_qty_and_reduce_only(self) -> None:
        with pytest.raises(ValidationError):
            OrderRequest(
                client_order_id="wx1-sl", symbol="RLCUSDT",
                side=OrderSide.SELL, kind=OrderKind.STOP_MARKET,
                stop_price=Decimal("0.31"), qty=Decimal("62.4"), close_position=True,
            )
        with pytest.raises(ValidationError):
            OrderRequest(
                client_order_id="wx1-sl", symbol="RLCUSDT",
                side=OrderSide.SELL, kind=OrderKind.STOP_MARKET,
                stop_price=Decimal("0.31"), close_position=True, reduce_only=True,
            )

    def test_conditional_with_qty_ok(self) -> None:
        r = OrderRequest(
            client_order_id="wx1-tp1", symbol="RLCUSDT",
            side=OrderSide.SELL, kind=OrderKind.TAKE_PROFIT_MARKET,
            stop_price=Decimal("0.33"), qty=Decimal("31.2"),
            reduce_only=True, price_protect=True,
        )
        assert r.reduce_only and r.price_protect

    def test_conditional_requires_qty_without_close_position(self) -> None:
        with pytest.raises(ValidationError):
            OrderRequest(
                client_order_id="wx1-tp1", symbol="RLCUSDT",
                side=OrderSide.SELL, kind=OrderKind.TAKE_PROFIT_MARKET,
                stop_price=Decimal("0.33"),
            )

    def test_bad_client_id_rejected(self) -> None:
        with pytest.raises(ValidationError):
            OrderRequest(
                client_order_id="плохой id!", symbol="RLCUSDT",
                side=OrderSide.BUY, kind=OrderKind.MARKET, qty=Decimal("1"),
            )


class TestMakeClientId:
    def test_ok(self) -> None:
        assert make_client_id(4212, "in") == "wx4212-in"
        assert make_client_id(4212, "sl", 1) == "wx4212-sl1"

    def test_too_long_raises(self) -> None:
        with pytest.raises(ValueError):
            make_client_id(10**34, "in")


class TestOrderStateMapping:
    def test_known(self) -> None:
        assert OrderState.from_exchange("NEW") is OrderState.NEW
        assert OrderState.from_exchange("FILLED") is OrderState.FILLED

    def test_expired_maps_to_canceled(self) -> None:
        assert OrderState.from_exchange("EXPIRED") is OrderState.CANCELED

    def test_unknown_returns_none(self) -> None:
        assert OrderState.from_exchange("SOME_FUTURE_STATUS") is None