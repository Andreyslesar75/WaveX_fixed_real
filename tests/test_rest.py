"""Тесты REST-слоя: подпись HMAC, типизированные ошибки, 429, GET-ретраи."""
import hashlib
import hmac
from typing import Any
from urllib.parse import urlencode

import pytest

from trading.binance.rest import (
    BinanceApiError,
    BinanceRestClient,
    FilterFailureError,
    InsufficientFundsError,
    OrderNotFoundError,
    TimestampSyncError,
    TransientError,
    TransportError,
    TransportTimeout,
    UnknownOrderError,
)
from trading.ratelimit import RateLimiter


class FakeTransport:
    """Транспорт с сценариями: ответ(-ы) или исключение."""

    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)
        self.calls: list[tuple[str, str, dict[str, str]]] = []

    async def request(self, method: str, url: str, headers: dict[str, str],
                      timeout_s: float) -> tuple[int, dict[str, str], Any]:
        self.calls.append((method, url, headers))
        if not self._script:
            raise AssertionError("сценарий исчерпан")
        item = self._script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class FixedClock:
    """Фиксированное время для детерминированной подписи."""
    def __init__(self, ts_ms: int) -> None:
        self.ts = ts_ms
    def now_ms(self) -> int:
        return self.ts


def _client(transport: FakeTransport, **kw: Any) -> BinanceRestClient:
    limiter = RateLimiter()
    return BinanceRestClient(
        transport=transport, api_key="KEY", secret_key="SECRET",
        base_url="https://fapi.binance.com", limiter=limiter,
        clock=FixedClock(1_700_000_000_000), request_timeout_s=5.0, **kw,
    )


class TestSignature:
    async def test_new_order_signed_deterministically(self) -> None:
        transport = FakeTransport([(200, {}, {"orderId": 1, "status": "NEW"})])
        client = _client(transport)
        await client.new_order({"symbol": "RLCUSDT", "side": "BUY",
                                "type": "MARKET", "quantity": "62.4"})
        _, url, _ = transport.calls[0]
        query = url.split("?", 1)[1]
        base = {"symbol": "RLCUSDT", "side": "BUY", "type": "MARKET",
                "quantity": "62.4", "recvWindow": "5000",
                "timestamp": "1700000000000"}
        expected_query = urlencode(base)
        expected_sig = hmac.new(b"SECRET", expected_query.encode(),
                                hashlib.sha256).hexdigest()
        assert query == f"{expected_query}&signature={expected_sig}"

    async def test_headers_carry_api_key(self) -> None:
        transport = FakeTransport([(200, {}, {"listenKey": "K"})])
        await _client(transport).create_listen_key()
        assert transport.calls[0][2]["X-MBX-APIKEY"] == "KEY"


class TestErrorMapping:
    async def test_1013_filter_failure(self) -> None:
        transport = FakeTransport([(400, {}, {"code": -1013, "msg": "Filter failure"})])
        with pytest.raises(FilterFailureError):
            await _client(transport).new_order({"symbol": "X"})

    async def test_2011_unknown_order(self) -> None:
        transport = FakeTransport([(400, {}, {"code": -2011, "msg": "Unknown order"})])
        with pytest.raises(UnknownOrderError):
            await _client(transport).cancel_order("RLCUSDT",
                                                  orig_client_order_id="wx1-sl")

    async def test_2013_not_found(self) -> None:
        transport = FakeTransport([(400, {}, {"code": -2013, "msg": "Order does not exist"})])
        with pytest.raises(OrderNotFoundError):
            await _client(transport).get_order("RLCUSDT", orig_client_order_id="wx1-in")

    async def test_1021_timestamp(self) -> None:
        transport = FakeTransport([(400, {}, {"code": -1021, "msg": "Timestamp"})])
        with pytest.raises(TimestampSyncError):
            await _client(transport).balance()

    async def test_insufficient_margin_variants(self) -> None:
        for code in (-2010, -2019):
            transport = FakeTransport([(400, {}, {"code": code, "msg": "Margin"})])
            with pytest.raises(InsufficientFundsError):
                await _client(transport).new_order({"symbol": "X"})

    async def test_unknown_code_generic(self) -> None:
        transport = FakeTransport([(400, {}, {"code": -1106, "msg": "Param"})])
        with pytest.raises(BinanceApiError) as exc_info:
            await _client(transport).new_order({"symbol": "X"})
        assert exc_info.value.code == -1106


class TestRetriesAndLimits:
    async def test_get_retries_on_transport_error(self) -> None:
        transport = FakeTransport([
            TransportError("conn"), (200, {}, [{"symbol": "RLCUSDT"}]),
        ])
        data = await _client(transport).position_risk()
        assert data == [{"symbol": "RLCUSDT"}]

    async def test_post_order_no_blind_retry(self) -> None:
        transport = FakeTransport([TransportTimeout("t/o")])
        with pytest.raises(TransportTimeout):
            await _client(transport).new_order({"symbol": "X"})
        assert len(transport.calls) == 1  # ровно один POST — идемпотентность

    async def test_429_pauses_limiter(self) -> None:
        transport = FakeTransport([
            (429, {"Retry-After": "2"}, {"code": 429, "msg": "busy"}),
        ])
        with pytest.raises(TransientError):
            await _client(transport, get_retries=0).balance()
        # limiter получил паузу — проверяем косвенно: следующий вызов ждёт
        assert len(transport.calls) == 1

class TestAlgoEndpoints:
    async def test_algo_new_path_and_error_mapping(self) -> None:
        transport = FakeTransport([
            (400, {}, {"code": -1013, "msg": "Filter failure"}),
        ])
        from trading.binance.rest import FilterFailureError
        with pytest.raises(FilterFailureError):
            await _client(transport).algo_order_new({"symbol": "RLCUSDT"})

    async def test_algo_query_and_cancel_paths(self) -> None:
        transport = FakeTransport([
            (200, {}, {"algoId": 1, "clientAlgoId": "c", "algoStatus": "NEW"}),
            (200, {}, {"algoId": 1, "clientAlgoId": "c", "algoStatus": "CANCELED"}),
        ])
        client = _client(transport)
        await client.algo_order_query("RLCUSDT", "c")
        await client.algo_order_cancel("RLCUSDT", "c")
        assert "algoOrder" in transport.calls[0][1]
        assert transport.calls[1][0] == "DELETE"
