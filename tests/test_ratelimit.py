# tests/test_ratelimit.py
"""Тесты ratelimit.py: refill-математика, коррекция по заголовкам, пауза."""
from trading.ratelimit import RateLimiter, _TokenBucket


class FakeClock:
    def __init__(self, start: int = 0) -> None:
        self.now = start

    def __call__(self) -> int:
        return self.now


class TestTokenBucket:
    def test_wait_ms_math(self) -> None:
        clock = FakeClock(0)
        b = _TokenBucket("t", 100, 100, 0.5, clock)  # 100*0.5/100мс = 0.5 ток/мс
        assert b.try_acquire(40) is True      # осталось 10 из 50
        assert b.wait_ms(30) == 40            # дефицит 20 / 0.5 = 40 мс
        clock.now = 40
        assert b.try_acquire(30) is True

    def test_update_used_caps_tokens(self) -> None:
        clock = FakeClock(0)
        b = _TokenBucket("t", 100, 100_000, 0.5, clock)  # cap=50
        b.update_used(45)  # tokens = min(50, 50-45) = 5
        assert b.try_acquire(10) is False
        assert b.try_acquire(5) is True


class TestRateLimiter:
    async def test_acquire_order_waits_for_slot(self) -> None:
        clock = FakeClock(0)
        slept: list[float] = []

        async def fake_sleep(sec: float) -> None:
            slept.append(sec)
            clock.now += int(sec * 1000)

        rl = RateLimiter(
            weight_per_min=1000, orders_per_10s=2, orders_per_min=100,
            safety=1.0, now_ms=clock, sleep=fake_sleep,
        )
        await rl.acquire_order()
        await rl.acquire_order()
        await rl.acquire_order()  # 10с-слот исчерпан -> ожидание ~5с
        assert slept and slept[-1] > 0

    async def test_headers_and_pause(self) -> None:
        clock = FakeClock(0)
        slept: list[float] = []

        async def fake_sleep(sec: float) -> None:
            slept.append(sec)
            clock.now += int(sec * 1000)

        rl = RateLimiter(now_ms=clock, sleep=fake_sleep)
        rl.update_from_headers({"X-MBX-USED-WEIGHT-1M": "2395"})
        await rl.acquire_request(weight=10)  # вес почти исчерпан -> ждём
        assert slept

        rl.pause(clock.now + 5000, "429")
        before = len(slept)
        await rl.acquire_request(weight=1)
        assert len(slept) > before
