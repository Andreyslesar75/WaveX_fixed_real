"""Тесты PaperVenue: исполнение, триггеры, комиссии, PnL, события."""
import asyncio
from decimal import Decimal

import pytest

from trading.binance.rest import OrderNotFoundError
from trading.paper.venue import PaperVenue
from trading.types import (
    OrderKind,
    OrderRequest,
    OrderSide,
    OrderState,
)
from trading.venue import (
    InsufficientFundsError,
    UnknownOrderError,
)

PRICES = {"RLCUSDT": Decimal("0.32")}
ENTRY = OrderRequest(
    client_order_id="wx1-in", symbol="RLCUSDT", side=OrderSide.BUY,
    kind=OrderKind.MARKET, qty=Decimal("62.4"),
)
SL_LONG = OrderRequest(
    client_order_id="wx1-sl", symbol="RLCUSDT", side=OrderSide.SELL,
    kind=OrderKind.STOP_MARKET, stop_price=Decimal("0.3139"),
    close_position=True,
)
TP1_LONG = OrderRequest(
    client_order_id="wx1-tp1", symbol="RLCUSDT", side=OrderSide.SELL,
    kind=OrderKind.TAKE_PROFIT_MARKET, stop_price=Decimal("0.3299"),
    qty=Decimal("31.2"), reduce_only=True,
)


def _venue() -> PaperVenue:
    return PaperVenue(
        starting_capital=Decimal("1000"),
        price_provider=lambda s: PRICES.get(s),
    )


class TestEntryAndStop:
    async def test_market_entry_fills_and_charges_fee(self) -> None:
        venue = _venue()
        ack = await venue.execute_order(ENTRY)
        assert ack.status is OrderState.FILLED
        # Decimal-сравнение по значению: арифметика даёт trailing zero ('0.320')
        assert ack.avg_price == Decimal("0.32")
        assert str(ack.executed_qty) == "62.4"
        fee = Decimal("62.4") * Decimal("0.32") * Decimal("0.0004")
        assert venue._balance == Decimal("1000") - fee

    async def test_sl_close_position_triggers(self) -> None:
        venue = _venue()
        await venue.execute_order(ENTRY)
        sl_ack = await venue.execute_order(SL_LONG)
        assert sl_ack.status is OrderState.NEW
        PRICES["RLCUSDT"] = Decimal("0.3139")
        venue.feed_price("RLCUSDT", Decimal("0.3139"))
        assert (await venue.positions()) == []  # позиция закрыта
        loss = (Decimal("0.3139") - Decimal("0.32")) * Decimal("62.4")
        fee_close = Decimal("62.4") * Decimal("0.3139") * Decimal("0.0004")
        fee_open = Decimal("62.4") * Decimal("0.32") * Decimal("0.0004")
        assert venue._balance == Decimal("1000") + loss - fee_close - fee_open

    async def test_tp_partial_close_keeps_position(self) -> None:
        venue = _venue()
        await venue.execute_order(ENTRY)
        await venue.execute_order(SL_LONG)
        await venue.execute_order(TP1_LONG)
        PRICES["RLCUSDT"] = Decimal("0.3299")
        venue.feed_price("RLCUSDT", Decimal("0.3299"))
        positions = await venue.positions()
        assert len(positions) == 1
        assert str(positions[0].qty) == "31.2"  # остаток после TP1
        # SL closePosition остался и покрывает остаток:
        assert [o.client_order_id for o in await venue.open_orders("RLCUSDT")] == ["wx1-sl"]

    async def test_events_full_lifecycle(self) -> None:
        venue = _venue()
        await venue.execute_order(ENTRY)
        await venue.execute_order(SL_LONG)
        PRICES["RLCUSDT"] = Decimal("0.31")
        venue.feed_price("RLCUSDT", Decimal("0.31"))
        updates = _drain(venue.events)
        kinds = [type(u).__name__ for u in updates]
        assert "VenueAccountUpdate" in kinds
        assert kinds.count("VenueOrderUpdate") >= 2  # вход + SL


class TestParityErrors:
    async def test_insufficient_balance_raises(self) -> None:
        venue = _venue()
        big = ENTRY.model_copy(update={"qty": Decimal("50000")})
        with pytest.raises(InsufficientFundsError):
            await venue.execute_order(big)

    async def test_reduce_only_over_position_rejected(self) -> None:
        venue = _venue()
        await venue.execute_order(ENTRY)
        over = TP1_LONG.model_copy(update={"qty": Decimal("70")})
        ack = await venue.execute_order(over)
        assert ack.status is OrderState.REJECTED
        assert ack.raw["code"] == "-2022"

    async def test_cancel_filled_raises_2011(self) -> None:
        venue = _venue()
        await venue.execute_order(ENTRY)
        await venue.execute_order(SL_LONG)
        PRICES["RLCUSDT"] = Decimal("0.31")
        venue.feed_price("RLCUSDT", Decimal("0.31"))
        with pytest.raises(UnknownOrderError):  # SL уже исполнился
            await venue.cancel_order("RLCUSDT", "wx1-sl")

    async def test_cancel_never_existed_404(self) -> None:
        venue = _venue()
        with pytest.raises(OrderNotFoundError):
            await venue.cancel_order("RLCUSDT", "wx999")


class TestQueries:
    async def test_query_and_user_trades(self) -> None:
        venue = _venue()
        await venue.execute_order(ENTRY)
        ack = await venue.query_order("RLCUSDT", "wx1-in")
        assert ack is not None and ack.status is OrderState.FILLED
        fills = await venue.user_trades("RLCUSDT", 0)
        assert len(fills) == 1
        assert fills[0].order_client_id == "wx1-in"

    async def test_commission_rate_parity(self) -> None:
        maker, taker = await _venue().commission_rate("RLCUSDT")
        assert maker == taker == Decimal("0.0004")


def _drain(queue: asyncio.Queue) -> list[object]:
    items: list[object] = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items
