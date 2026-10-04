# trading/ratelimit.py
"""Локальный троттлинг запросов Binance: вес + ордерные слоты.

Зачем: превышение лимитов — 429/418 вплоть до IP-ban; для торговой
системы блокировка на минуты недопустима. Модель: token-bucket по
каждому лимиту с коэффициентом SAFETY (запас), корректируемые
реальными заголовками ответов.

[НЕУВЕРЕН] точные имена заголовков (X-MBX-USED-WEIGHT-1M,
X-MBX-ORDER-COUNT-10S/1M) — снимаются V-API-7; чтение опционально:
отсутствие заголовка — не ошибка (не все эндпоинты их возвращают).
Дефолты соответствуют проде USDⓈ-M (проверено по exchangeInfo);
testnet (6000/мин) задаётся параметрами при сборке.
"""
from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Awaitable, Callable, Mapping

logger = logging.getLogger(__name__)

AsyncSleep = Callable[[float], Awaitable[None]]
NowMs = Callable[[], int]



class _TokenBucket:
    """Синхронный token-bucket (тестируется без asyncio).

    Инвариант: tokens <= capacity * safety — локальная модель всегда
    консервативнее реального лимита.
    """

    def __init__(
        self, name: str, capacity: int, window_ms: int, safety: float, now_ms: NowMs
    ) -> None:
        self.name = name
        self.capacity = int(capacity)
        self._safety = safety
        self._refill_per_ms = self.capacity * safety / window_ms
        self._tokens = float(self.capacity) * safety
        self._now_ms = now_ms
        self._last_ms = now_ms()

    def _refill(self) -> None:
        now = self._now_ms()
        elapsed = max(now - self._last_ms, 0)
        cap = float(self.capacity) * self._safety
        self._tokens = min(self._tokens + elapsed * self._refill_per_ms, cap)
        self._last_ms = now

    def try_acquire(self, n: int = 1) -> bool:
        """Взять n токенов немедленно; False — нехватка."""
        self._refill()
        if n <= self._tokens:
            self._tokens -= n
            return True
        return False

    def wait_ms(self, n: int = 1) -> int:
        """Мс до возможности взять n токенов (0 — можно сейчас)."""
        self._refill()
        if n <= self._tokens:
            return 0
        deficit = n - self._tokens
        return math.ceil(deficit / self._refill_per_ms)

    def update_used(self, used: int) -> None:
        """Синхронизация с заголовком биржи.

        Почему min(...): если биржа уже потратила больше, чем мы
        думали (shared IP), локальная модель обязана стать не
        оптимистичнее реальности.
        """
        cap = float(self.capacity) * self._safety
        self._refill()
        self._tokens = min(self._tokens, max(cap - float(used), 0.0))


class RateLimiter:
    """Асинхронный фасад над бакетами веса и ордеров.

    Использование движком: acquire_request(weight) перед любым
    REST-запросом; acquire_order() перед ордерным POST (вес + оба
    ордерных слота). Ожидание слота — норма: лучше подождать, чем
    получить 429 в критическом пути.
    """

    def __init__(
        self,
        weight_per_min: int = 2400,
        orders_per_10s: int = 300,
        orders_per_min: int = 1200,
        safety: float = 0.8,
        now_ms: NowMs | None = None,
        sleep: AsyncSleep = asyncio.sleep,
    ) -> None:
        """now_ms/sleep — инъекции для юнит-тестов без реального времени."""
        self._now_ms: NowMs = now_ms or (lambda: int(time.time() * 1000))
        self._sleep = sleep
        self._weight = _TokenBucket("weight-1m", weight_per_min, 60_000, safety, self._now_ms)
        self._orders10 = _TokenBucket("orders-10s", orders_per_10s, 10_000, safety, self._now_ms)
        self._orders_min = _TokenBucket("orders-1m", orders_per_min, 60_000, safety, self._now_ms)
        self._paused_until_ms = 0

    @staticmethod
    def _get_ci(headers: Mapping[str, str], name: str) -> int | None:
        """Значение числового заголовка независимо от регистра ключа."""
        low = name.lower()
        for key, value in headers.items():
            if key.lower() == low and value.isdigit():
                return int(value)
        return None

    def update_from_headers(self, headers: Mapping[str, str]) -> None:
        """Скорректировать бакеты по заголовкам ответа (если есть)."""
        used = self._get_ci(headers, "X-MBX-USED-WEIGHT-1M")
        if used is not None:
            self._weight.update_used(used)
        c10 = self._get_ci(headers, "X-MBX-ORDER-COUNT-10S")
        if c10 is not None:
            self._orders10.update_used(c10)
        c1m = self._get_ci(headers, "X-MBX-ORDER-COUNT-1M")
        if c1m is not None:
            self._orders_min.update_used(c1m)

    def pause(self, until_ms: int, reason: str) -> None:
        """Глобальная пауза запросов (429/418 + Retry-After)."""
        if until_ms > self._paused_until_ms:
            self._paused_until_ms = until_ms
            logger.warning("rate-limit: пауза до %d (%s)", until_ms, reason)

    def _respect_pause(self) -> int:
        """Оставшиеся мс паузы (0 — паузы нет)."""
        return max(self._paused_until_ms - self._now_ms(), 0)

    async def acquire_request(self, weight: int = 1) -> None:
        """Слот REST-запроса веса weight; ждёт при нехватке/паузе."""
        pause_ms = self._respect_pause()
        if pause_ms:
            await self._sleep(pause_ms / 1000.0)
        while True:
            wait = self._weight.wait_ms(weight)
            if wait == 0 and self._weight.try_acquire(weight):
                return
            await self._sleep(max(wait, 1) / 1000.0)

    async def acquire_order(self) -> None:
        """Слот ордерного запроса: вес + слоты 10с и 1м.

        Атомарность: между wait_ms и try_acquire нет await — в модели
        одного asyncio-цикла двойного взятия не бывает.
        """
        pause_ms = self._respect_pause()
        if pause_ms:
            await self._sleep(pause_ms / 1000.0)
        while True:
            wait = max(
                self._weight.wait_ms(1), self._orders10.wait_ms(1), self._orders_min.wait_ms(1)
            )
            if wait == 0:
                ok = (
                    self._weight.try_acquire(1)
                    and self._orders10.try_acquire(1)
                    and self._orders_min.try_acquire(1)
                )
                if ok:
                    return
                # теоретически недостижимо (wait==0 => токенов хватает);
                # страховка от busy-loop:
                await self._sleep(0.001)
                continue
            await self._sleep(wait / 1000.0)
