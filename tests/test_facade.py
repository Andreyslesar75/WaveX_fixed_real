"""Тесты фасада: адаптивный порог (1:1), снимок позиций, get_stats."""
from decimal import Decimal

from trading.facade import PositionManager
from trading.types import Mode

from tests.test_engine import (
    EI, _make_engine, _reset, _start, _teardown,
)
from trading.engine import EntryIntent


class TestAdaptiveThreshold:
    def test_long_constant(self) -> None:
        f = PositionManager.get_adaptive_threshold
        assert f("LONG", 0.0) == 41.0
        assert f("LONG", 10.0) == 41.0
        assert f("LONG", -10.0) == 41.0

    def test_short_adjusts(self) -> None:
        f = PositionManager.get_adaptive_threshold
        assert f("SHORT", 0.0) == 35.0
        assert f("SHORT", -3.0) == 32.0
        assert f("SHORT", 3.0) == 38.0


class TestViewSnapshot:
    async def test_positions_and_stats_from_engine(self, tmp_path) -> None:
        _reset()
        db = tmp_path / "f.db"
        engine, venue, _ = await _make_engine(
            db, monitor_interval_sec=0.01, sl_rest_check_interval_sec=999.0,
        )
        stop, task = await _start(engine)
        try:
            ok, _ = await engine.submit_signal(EntryIntent(**EI))
            assert ok
            facade = PositionManager(
                engine=engine, storage_path=db, settings=engine._settings,
                mode=Mode.PAPER, capital_base=Decimal("1000"),
            )
            positions = engine.open_positions()
            assert positions and positions[0]["symbol"] == "RLCUSDT"
            # entry_time — epoch-СЕКУНДЫ (контракт GUI, §0 Части 4)
            assert positions[0]["entry_time"] < 10**12
            assert "Сделок:" in facade.get_stats()
            assert facade.get_trades() == []
        finally:
            await _teardown(stop, task, engine, db)