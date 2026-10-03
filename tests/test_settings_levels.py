"""Тесты settings/levels: формулы порогов, калькулятор уровней."""
from decimal import Decimal

from trading.levels import PercentLevelCalculator
from trading.settings import AdaptiveThresholds, EngineSettings
from trading.types import Side


class TestAdaptive:
    def test_long_threshold_follows_trend(self) -> None:
        a = AdaptiveThresholds()
        base = a.base_long
        assert a.threshold(True, 1.0) < base   # бычий тренд — вход легче
        assert a.threshold(True, -1.0) > base  # медвежий — труднее

    def test_short_mirror(self) -> None:
        a = AdaptiveThresholds()
        assert a.threshold(False, 1.0) > a.base_short
        assert a.threshold(False, -1.0) < a.base_short

    def test_adjust_capped(self) -> None:
        a = AdaptiveThresholds(trend_adjust=100.0, trend_cap=2.0)
        assert a.threshold(True, 10.0) == a.base_long - 2.0


class TestPercentCalculator:
    def test_long_levels(self) -> None:
        calc = PercentLevelCalculator(Decimal("2"), Decimal("3"))
        levels = calc.calculate(Decimal("100"), Side.LONG, 0, 0, None, None)
        assert levels.sl_price == Decimal("98")
        assert levels.tp2_price == Decimal("103")

    def test_short_levels(self) -> None:
        calc = PercentLevelCalculator(Decimal("2"), Decimal("3"))
        levels = calc.calculate(Decimal("100"), Side.SHORT, 0, 0, None, None)
        assert levels.sl_price == Decimal("102")
        assert levels.tp2_price == Decimal("97")