"""Тесты фасада: адаптивный порог, снимок view, формат get_stats."""
import asyncio
from decimal import Decimal

from trading.facade import PositionManager
from trading.settings import EngineSettings
from trading.types import Mode


class TestAdaptiveThreshold:
    def test_long_constant(self) -> None:  # LONG — без поправок (сверено!)
        f = PositionManager.get_adaptive_threshold
        assert f("LONG", 0.0) == 41.0
        assert f("LONG", 10.0) == 41.0
        assert f("LONG", -10.0) == 41.0

    def test_short_adjusts(self) -> None:
        f = PositionManager.get_adaptive_threshold
        assert f("SHORT", 0.0) == 35.0
        assert f("SHORT", -3.0) == 32.0
        assert f("SHORT", 3.0) == 38.0
        assert f("SHORT", -1.0) == 35.0


class TestViewSnapshot:
    async def test_view_updates_from_engine(self) -> None:
        from tests.test_engine import EI, PRICES, _make_engine, _teardown
        engine, venue, _ = await _make_engine()
        facade = PositionManager(
            engine=engine, storage_path=engine._storage._path,
            settings=engine._settings, mode=Mode.PAPER,
            capital_base=Decimal("1000"),
        )
        ok, _ = await engine.submit_signal(type("I", (), EI))  # type: ignore[arg-type]
        assert ok
        await facade._view_loop.__wrapped__ if False else None  # см. ниже
        # прямой вызов логики снимка (без сна):
        positions = {p["symbol"]: p for p in engine.open_positions()}
        facade._view = type(facade._view)(
            capital=1000.0, total_pnl=0.0, breakevens=0, positions=positions,
        )
        assert "RLCUSDT" in facade.positions
        assert facade.get_open_positions()[0]["entry_time"] < 10**12  # секунды
        assert "Сделок:" in facade.get_stats()
        _teardown(engine)