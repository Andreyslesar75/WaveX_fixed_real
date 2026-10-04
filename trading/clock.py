# trading/clock.py
"""Синхронизация времени с биржей (устранение -1021, §13 черновика).

offset считается с поправкой на RTT/2: середина интервала запроса —
несмещённая оценка «биржевого сейчас» при асимметричной задержке.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import Callable, Mapping

from .types import JsonFetcher
from typing import Protocol

logger = logging.getLogger(__name__)

class TimeProvider(Protocol):
    """Минимальный контракт времени для подписанных запросов (rest)."""
    def now_ms(self) -> int: ...

class ClockError(RuntimeError):
    """serverTime невалиден — ошибка границы, не молчать."""


class Clock:
    """Хранит offset (мс) относительно биржи; now_ms() — «биржевое» время.

    Инвариант: пока sync() ни разу не прошёл, offset=0 и подписанные
    запросы не отправляются (engine ждёт wait_synced()).
    """

    def __init__(self, fetch_json: JsonFetcher, now_ms: Callable[[], int] | None = None) -> None:
        """Args: fetch_json — транспорт; now_ms — инъекция для тестов."""
        self._fetch_json = fetch_json
        self._now_ms_fn = now_ms or (lambda: int(time.time() * 1000))
        self._offset_ms: int = 0
        self._synced = asyncio.Event()
        self._last_sync_ms: int = 0

    @property
    def offset_ms(self) -> int:
        """Текущий offset (диагностика; -1021 вызывает форс-ресинк)."""
        return self._offset_ms

    async def sync(self) -> int:
        """Одна синхронизация; возвращает новый offset (мс).

        Raises:
            ClockError: /fapi/v1/time вернул мусор; OSError/TimeoutError
            от транспорта.
        """
        t0 = self._now_ms_fn()
        payload = await self._fetch_json("/fapi/v1/time", None)
        t1 = self._now_ms_fn()
        if not isinstance(payload, Mapping):
            raise ClockError(f"/fapi/v1/time вернул не объект: {payload!r}")
        raw = payload.get("serverTime")
        if isinstance(raw, bool) or not isinstance(raw, int):
            raise ClockError(f"serverTime некорректен: {raw!r}")
        rtt = max(t1 - t0, 0)
        self._offset_ms = int(raw - (t1 - rtt / 2))
        self._last_sync_ms = t1
        self._synced.set()
        logger.info("clock: синхронизирован, offset=%dms, rtt=%dms", self._offset_ms, rtt)
        return self._offset_ms

    async def wait_synced(self) -> None:
        """Блок до первой успешной синхронизации."""
        await self._synced.wait()

    def now_ms(self) -> int:
        """«Биржевое» сейчас: локальное время + offset."""
        return self._now_ms_fn() + self._offset_ms

    async def run_background(
        self, interval_s: float, stop: asyncio.Event | None = None
    ) -> None:
        """Фоновый ресинк (§13: 15–30 мин); ошибка не сбрасывает offset."""
        while True:
            if stop is not None and stop.is_set():
                return
            await asyncio.sleep(interval_s)
            if stop is not None and stop.is_set():
                return
            try:
                await self.sync()
            except (ClockError, asyncio.TimeoutError, OSError) as exc:
                logger.warning(
                    "clock: фоновый ресинк не удался: %s (продолжаем с offset=%dms)",
                    exc,
                    self._offset_ms,
                )