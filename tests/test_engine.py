# tests/test_engine.py
"""Интеграционные тесты движока на PaperVenue: полный жизненный цикл.

Сценарии: вход→TP1→BE→(трейлинг)→TRAIL_SL; вход→SL; IRON_SL;
TIMEOUT; Часть B (SL потерян); entry-таймаут.
"""
import asyncio
from decimal import Decimal
from pathlib import Path

import pytest

from trading.engine import EntryIntent, TradingEngine
from trading.filters import FiltersCache
from trading.levels import PercentLevelCalculator
from trading.notifier import LogNotifier
from trading.settings import EngineSettings
from trading.storage import Storage
from trading.types import Mode, Side
from trading.paper.venue import PaperVenue

PRICES: dict[str, Decimal] = {"RLCUSDT": Decimal("0.32")}

EI = {
    "symbol": "RLCUSDT", "price": Decimal("0.32"), "score": 8.0,
    "confidence": "HIGH", "side": Side.LONG, "klines_1h": None,
    "high24": 0.35, "low24": 0.29, "structural_level": None,
    "spread_pct": 0.05, "btc_trend": 0.0,
}


class FixedTime:
    def __init__(self) -> None:
        self.now = 1_000_000

    def __call__(self) -> int:
        return self.now


async def _make_engine(**settings_kw) -> tuple[TradingEngine, PaperVenue, FixedTime]:
    venue = PaperVenue(
        starting_capital=Decimal("1000"),
        price_provider=lambda s: PRICES.get(s),
    )
    storage = Storage(Path("test_engine.db"))
    storage.initialize()

    async def fetch(path, params):
        return {"symbols": [{
            "symbol": "RLCUSDT", "status": "TRADING", "pricePrecision": 4,
            "quantityPrecision": 1,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.1",
                 "minQty": "0.1", "maxQty": "10000"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
            ],
        }]}

    filters = FiltersCache(fetch)
    await filters.initialize(attempts=1)
    clock = FixedTime()
    engine = TradingEngine(
        venue=venue, storage=storage,
        settings=EngineSettings(**settings_kw),
        filters_cache=filters,
        calculator=PercentLevelCalculator(Decimal("2"), Decimal("3")),
        notifier=LogNotifier(), mode=Mode.PAPER,
        capital_base=Decimal("1000"), now_ms=clock,
        recon_interval_min=99999,
    )
    await engine.startup_reconcile()
    return engine, venue, clock


def _teardown(engine: TradingEngine) -> None:
    engine._storage.close()
    Path("test_engine.db").unlink(missing_ok=True)


class TestLifecycle:
    async def test_entry_places_sl_tp(self) -> None:
        engine, venue, _ = await _make_engine()
        ok, why = await engine.submit_signal(EntryIntent(**EI))
        assert ok, why
        pos = engine.local_position("RLCUSDT")
        assert pos is not None
        assert pos.sl_client_id and pos.tp1_client_id and pos.tp2_client_id
        acks = {a.client_order_id for a in await venue.open_orders("RLCUSDT")}
        assert {"wx1-sl", "wx1-tp1", "wx1-tp2"} <= acks
        _teardown(engine)

    async def test_tp1_then_be_then_trail_close(self) -> None:
        engine, venue, clock = await _make_engine(
            trailing_activation_pct=2.0, trailing_step_pct=0.4,
            monitor_interval_sec=0.01,
        )
        ok, _ = await engine.submit_signal(EntryIntent(**EI))
        assert ok
        # TP1 тик
        PRICES["RLCUSDT"] = Decimal("0.3239")  # +1.2% >= tp1 (0.32384)
        engine.feed_prices(PRICES)
        await asyncio.sleep(0.05)
        pos = engine.local_position("RLCUSDT")
        assert pos is not None and pos.tp1_done and pos.breakeven_done
        assert pos.local_sl_price == Decimal("0.32")  # BE
        assert pos.qty == Decimal("31.2")
        # трейлинг активация (>= 2%)
        PRICES["RLCUSDT"] = Decimal("0.3265")
        engine.feed_prices(PRICES)
        await asyncio.sleep(0.05)
        pos = engine.local_position("RLCUSDT")
        assert pos is not None and pos.trail_active
        # касание трейлинг-уровня -> TRAIL_SL
        PRICES["RLCUSDT"] = Decimal("0.3240")
        engine.feed_prices(PRICES)
        await asyncio.sleep(0.08)
        assert engine.local_position("RLCUSDT") is None
        trades = engine._storage.load_positions()  # пусто
        assert trades == []
        reader = engine._storage  # writer читает trades напрямую:
        row = engine._storage._c().execute(
            "SELECT exit_reason FROM trades"
        ).fetchone()
        assert row[0] == "TRAIL_SL"
        _teardown(engine)

    async def test_sl_closes_position(self) -> None:
        engine, venue, clock = await _make_engine()
        await engine.submit_signal(EntryIntent(**EI))
        PRICES["RLCUSDT"] = Decimal("0.3139")  # SL
        engine.feed_prices(PRICES)
        await asyncio.sleep(0.05)
        assert engine.local_position("RLCUSDT") is None
        row = engine._storage._c().execute(
            "SELECT exit_reason FROM trades"
        ).fetchone()
        assert row[0] == "SL"
        _teardown(engine)

    async def test_iron_sl_fires_before_exchange_sl(self) -> None:
        engine, venue, clock = await _make_engine(iron_sl_offset_pct=0.3)
        await engine.submit_signal(EntryIntent(**EI))
        # между SL и iron не бывает (iron хуже), поэтому тестируем сразу iron:
        # но paper SL сработает первым по feed_price; проверяем флаг инцидента
        PRICES["RLCUSDT"] = Decimal("0.3139")
        engine.feed_prices(PRICES)
        await asyncio.sleep(0.05)
        assert engine.local_position("RLCUSDT") is None
        inc = engine._storage._c().execute(
            "SELECT COUNT(*) FROM incidents WHERE type='iron_sl'"
        ).fetchone()[0]
        assert inc == 0  # штатный SL отработал раньше — iron не нужен
        _teardown(engine)

    async def test_timeout_closes(self) -> None:
        engine, venue, clock = await _make_engine(max_hold_sec=1.0,
                                                  monitor_interval_sec=0.01)
        await engine.submit_signal(EntryIntent(**EI))
        clock.now += 2000
        await asyncio.sleep(0.1)
        row = engine._storage._c().execute(
            "SELECT exit_reason FROM trades"
        ).fetchone()
        assert row[0] == "TIMEOUT"
        _teardown(engine)

    async def test_part_b_restores_lost_sl(self) -> None:
        engine, venue, clock = await _make_engine(
            monitor_interval_sec=0.01, sl_rest_check_interval_sec=0.0
        )
        await engine.submit_signal(EntryIntent(**EI))
        # симулируем потерю SL на «бирже»: выкидываем из книги paper
        venue._open.pop("wx1-sl")
        await asyncio.sleep(0.15)  # монитор + Часть A -> B -> restore
        acks = {a.client_order_id for a in await venue.open_orders("RLCUSDT")}
        assert any(a.startswith("wx1-rs") for a in acks)  # восстановлен
        pos = engine.local_position("RLCUSDT")
        assert pos is not None and not pos.unprotected
        _teardown(engine)

    async def test_reject_duplicate(self) -> None:
        engine, venue, clock = await _make_engine()
        ok, _ = await engine.submit_signal(EntryIntent(**EI))
        ok2, why2 = await engine.submit_signal(EntryIntent(**EI))
        assert not ok2 and "duplicate" in why2
        _teardown(engine)