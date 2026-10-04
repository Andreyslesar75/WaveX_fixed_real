# tests/test_clock.py
"""Тесты clock.py: offset с поправкой на RTT, валидация serverTime."""
from collections.abc import Mapping
from typing import Any

import pytest

from trading.clock import Clock, ClockError


class FakeClock:
    """Инъекция времени: фиксированное 'сейчас'."""

    def __init__(self, start: int) -> None:
        self.now = start

    def __call__(self) -> int:
        return self.now


class TestClock:
    async def test_sync_offset(self) -> None:
        local = FakeClock(1_000_000)

        async def fetch(path: str, params: Mapping[str, str] | None) -> Any:
            return {"serverTime": 1_002_500}  # биржа «впереди» на 2500 мс

        clk = Clock(fetch, now_ms=local)
        offset = await clk.sync()
        assert offset == 2_500
        assert clk.now_ms() == 1_002_500

    async def test_invalid_payload_raises(self) -> None:
        async def fetch(path: str, params: Mapping[str, str] | None) -> Any:
            return {"foo": "bar"}

        with pytest.raises(ClockError):
            await Clock(fetch).sync()
