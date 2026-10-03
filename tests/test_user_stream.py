"""Тесты user-stream: парсинг событий, тишина -> реконнект, keepalive."""
import asyncio
import json
from typing import Any

import pytest

from trading.binance.user_stream import (
    UserStream, is_listen_key_expired, parse_stream_message,
)
from trading.venue import VenueAccountUpdate, VenueOrderUpdate, VenueReconnected


class FakeApi:
    """listenKey-API: подсчёт keepalive, сценарий отказов."""

    def __init__(self, keepalive_fails: int = 0) -> None:
        self.keepalive_calls = 0
        self._fails_left = keepalive_fails

    async def create_listen_key(self) -> str:
        return "KEY1"

    async def keepalive_listen_key(self) -> None:
        self.keepalive_calls += 1
        if self._fails_left > 0:
            self._fails_left -= 1
            raise RuntimeError("scripted keepalive failure")


class FakeConn:
    """WS-соединение со сценарием: str=сообщение, TimeoutError=тишина."""

    def __init__(self, script: list[Any]) -> None:
        self._script = list(script)
        self.closed = False

    async def receive(self, timeout_s: float) -> Any:
        from trading.binance.user_stream import WsMessage

        if not self._script:
            await asyncio.sleep(3600)  # вечная тишина -> сессия живёт
        item = self._script.pop(0)
        if isinstance(item, TimeoutError):
            raise item
        if item == "CLOSED":
            return WsMessage(closed=True, data="")
        return WsMessage(closed=False, data=item)

    async def close(self) -> None:
        self.closed = True


class FakeFactory:
    """Фабрика с очередью сессий-сценариев."""

    def __init__(self, scripts: list[list[Any]]) -> None:
        self._scripts = list(scripts)
        self.connections: list[FakeConn] = []

    async def connect(self, url: str) -> FakeConn:
        assert "/ws/KEY1" in url
        conn = FakeConn(self._scripts.pop(0) if self._scripts else [])
        self.connections.append(conn)
        return conn


_ORDER_UPDATE = {
    "e": "ORDER_TRADE_UPDATE", "E": 123,
    "o": {"s": "RLCUSDT", "c": "wx1-in", "i": 77, "S": "BUY",
          "X": "FILLED", "x": "TRADE", "ap": "0.32", "q": "62.4",
          "z": "62.4", "l": "62.4", "n": "0.00798", "N": "USDT",
          "rp": "0", "m": False},
}
_ACCOUNT_UPDATE = {
    "e": "ACCOUNT_UPDATE",
    "a": {"B": [{"a": "USDT", "wb": "999.99", "cw": "1000"}],
          "P": [{"s": "RLCUSDT", "pa": "62.4", "ep": "0.32", "up": "-0.5"}]},
}


class TestParsing:
    def test_order_update_parsed(self) -> None:
        event = parse_stream_message(_ORDER_UPDATE)
        assert isinstance(event, VenueOrderUpdate)
        assert event.event.client_order_id == "wx1-in"
        assert event.event.state is not None
        assert str(event.event.avg_price) == "0.32"

    def test_account_update_parsed(self) -> None:
        event = parse_stream_message(_ACCOUNT_UPDATE)
        assert isinstance(event, VenueAccountUpdate)
        assert str(event.balances["USDT"]) == "999.99"
        pos = event.positions["RLCUSDT"]
        assert pos.side == "LONG" and str(pos.qty) == "62.4"

    def test_foreign_events_ignored(self) -> None:
        assert parse_stream_message({"e": "MARGIN_CALL"}) is None

    def test_listen_key_expired(self) -> None:
        assert is_listen_key_expired({"e": "listenKeyExpired"})


class TestStreamLifecycle:
    async def test_silence_causes_reconnect_with_event(self) -> None:
        import trading.binance.user_stream as us

        api = FakeApi()
        factory = FakeFactory([
            [json.dumps(_ORDER_UPDATE), TimeoutError()],  # тишина -> обрыв
            ["CLOSED"],                                    # вторая сессия
        ])
        queue: asyncio.Queue = asyncio.Queue()
        stream = UserStream(api, factory, "wss://x", queue,
                            silence_timeout_s=0.05, keepalive_interval_s=60)
        task = asyncio.create_task(stream.run())
        await asyncio.sleep(0.3)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        kinds = [type(e).__name__ for e in _drain(queue)]
        assert "VenueOrderUpdate" in kinds
        assert "VenueReconnected" in kinds  # реконнект сигналится движку

    async def test_two_failed_keepalives_recreate_session(self) -> None:
        api = FakeApi(keepalive_fails=10)  # все keepalive падают
        factory = FakeFactory([[TimeoutError()], [TimeoutError()]])
        queue: asyncio.Queue = asyncio.Queue()
        stream = UserStream(api, factory, "wss://x", queue,
                            silence_timeout_s=60,
                            keepalive_interval_s=0.05, reconnect_delay_s=0.01)
        task = asyncio.create_task(stream.run())
        await asyncio.sleep(0.4)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert api.keepalive_calls >= 2  # две неудачи -> пересбор
        assert len(factory.connections) >= 2


def _drain(queue: asyncio.Queue) -> list[Any]:
    items = []
    while not queue.empty():
        items.append(queue.get_nowait())
    return items