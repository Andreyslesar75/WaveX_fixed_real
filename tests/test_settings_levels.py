"""Тесты settings/levels: adaptive_threshold 1:1, R-множители PercentCalc."""
from decimal import Decimal

from trading.levels import PercentLevelCalculator
from trading.settings import EngineSettings
from trading.types import Side


class TestAdaptiveThreshold:
    """Формула старого get_adaptive_threshold (сверено по коду)."""

    def test_long_constant(self) -> None:
        s = EngineSettings()
        assert s.adaptive_threshold(True, 0.0) == 41.0
        assert s.adaptive_threshold(True, 10.0) == 41.0
        assert s.adaptive_threshold(True, -10.0) == 41.0

    def test_short_adjusts(self) -> None:
        s = EngineSettings()
        assert s.adaptive_threshold(False, 0.0) == 35.0
        assert s.adaptive_threshold(False, -3.0) == 32.0
        assert s.adaptive_threshold(False, 3.0) == 38.0

    def test_short_boundaries_strict(self) -> None:
        s = EngineSettings()
        # ровно -2.0 — ещё НЕ «сильный нисходящий» (условие < -2)
        assert s.adaptive_threshold(False, -2.0) == 35.0
        assert s.adaptive_threshold(False, -2.01) == 32.0
        assert s.adaptive_threshold(False, 2.0) == 35.0


class TestPercentCalculator:
    """R-множители: tp1 = sl_pct*1.0, tp2 = sl_pct*1.1 (Config §11)."""

    def test_long(self) -> None:
        lv = PercentLevelCalculator(Decimal("2")).calculate(
            Decimal("100"), Side.LONG, 0, 0, None, None,
        )
        assert lv.sl_price == Decimal("98")
        assert lv.tp1_price == Decimal("102")
        assert lv.tp2_price == Decimal("102.2")

    def test_short_mirror(self) -> None:
        lv = PercentLevelCalculator(Decimal("2")).calculate(
            Decimal("100"), Side.SHORT, 0, 0, None, None,
        )
        assert lv.sl_price == Decimal("102")
        assert lv.tp1_price == Decimal("98")
        assert lv.tp2_price == Decimal("97.8")

    def test_tp2_mult_param(self) -> None:
        lv = PercentLevelCalculator(Decimal("2"), Decimal("3")).calculate(
            Decimal("100"), Side.LONG, 0, 0, None, None,
        )
        assert lv.tp2_price == Decimal("106")