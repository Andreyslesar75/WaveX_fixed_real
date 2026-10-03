"""Тесты RealVenue: resolve после таймаута, -2011, парсинг ack."""
import asyncio
from decimal import Decimal
from typing import Any

import pytest

from trading.binance.rest import (
    BinanceRestClient, TransportTimeout, UnknownOrderError,
)
from trading.binance.venue import RealVenue, build_order_params
from trading.ratelimit import RateLimiter
from trading.types import (
    OrderKind, OrderRequest, OrderSide, OrderState,
)
from trading.venue import InsufficientFundsError

ENTRY = OrderRequest(
    client_order_id="wx1-in", symbol="RLCUSDT", side=OrderSide.BUY,
    kind=OrderKind.MARKET, qty=Decimal("62.4"),
)


class FakeTransport:
    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)

    async def request(self, method: str, url: str, headers: dict[str, str],
                      timeout_s: float) -> tuple[int, dict[str, str], Any]:
        item = self._script.pop(0) if self._script else (200, {}, {})
        if isinstance(item, Exception):
            raise item
        return item


class FixedClock:
    def __init__(self) -> None:
        self.ts = 1_700_000_000_000
    def now_ms(self) -> int:
        return self.ts


def _venue(script: list[Any]) -> RealVenue:
    rest = BinanceRestClient(
        transport=FakeTransport(script), api_key="K", secret_key="S",
        base_url="https://fapi.binance.com", limiter=RateLimiter(),
        clock=FixedClock(),
    )
    return RealVenue(rest, asyncio.Queue(), resolve_interval_s=0.0)


class TestBuildParams:
    def test_flags_and_decimals(self) -> None:
        sl = OrderRequest(
            client_order_id="wx1-sl", symbol="RLCUSDT", side=OrderSide.SELL,
            kind=OrderKind.STOP_MARKET, stop_price=Decimal("0.3139"),
            close_position=True, price_protect=True,
        )
        params = build_order_params(sl)
        assert params["closePosition"] == "true"
        assert params["priceProtect"] == "TRUE"
        assert params["stopPrice"] == "0.3139"
        assert params["workingType"] == "MARK_PRICE"

    def test_qty_normalized(self) -> None:
        params = build_order_params(ENTRY)
        assert params["quantity"] == "62.4"


class TestExecuteOrder:
    async def test_ok_market_filled(self) -> None:
        venue = _venue([(200, {}, {"orderId": 1, "status": "FILLED",
                                   "avgPrice": "0.32", "executedQty": "62.4"})])
        ack = await venue.execute_order(ENTRY)
        assert ack.status is OrderState.FILLED
        assert ack.avg_price == Decimal("0.32")
        assert ack.executed_qty == Decimal("62.4")

    async def test_timeout_resolved_to_filled(self) -> None:
        venue = _venue([
            TransportTimeout("t/o"),  # POST потерян
            (200, {}, {"orderId": 2, "status": "FILLED",
                       "avgPrice": "0.321", "executedQty": "62.4"}),
        ])
        ack = await venue.execute_order(ENTRY)
        assert ack.status is OrderState.FILLED  # resolve нашёл исполнение

    async def test_timeout_stable_not_found(self) -> None:
        venue = _venue([
            TransportTimeout("t/o"),
            (400, {}, {"code": -2013, "msg": "Order does not exist"}),
            (400, {}, {"code": -2013, "msg": "Order does not exist"}),
        ])
        ack = await venue.execute_order(ENTRY)
        assert ack.status is OrderState.NOT_FOUND  # безопасный повтор

    async def test_insufficient_raises(self) -> None:
        venue = _venue([(400, {}, {"code": -2019, "msg": "Margin insufficient"})])
        with pytest.raises(InsufficientFundsError):
            await venue.execute_order(ENTRY)

    async def test_filter_failure_rejected_not_raised(self) -> None:
        venue = _venue([(400, {}, {"code": -1013, "msg": "Filter failure"})])
        ack = await venue.execute_order(ENTRY)
        assert ack.status is OrderState.REJECTED
        assert ack.raw["code"] == -1013  # движок: refresh фильтров + 1 повтор


class TestCancel:
    async def test_2011_propagates(self) -> None:
        venue = _venue([(400, {}, {"code": -2011, "msg": "Unknown order"})])
        with pytest.raises(UnknownOrderError):
            await venue.cancel_order("RLCUSDT", "wx1-sl")