"""Расчёт уровней SL/TP: протокол + %-реализация.

Реальная обёртка над calculations.py (который НЕ меняем, В1) будет
в Части 4 [ТРЕБУЕТСЯ СВЕРКА: точная сигнатура функций]. Пока:
протокол для движка + PercentLevelCalculator для paper/тестов.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from .types import Side


@dataclass(frozen=True, slots=True)
class CalculatedLevels:
    """Уровни одной сделки до округления по tick (округляет движок).

    sl_pct/tp_pct — расстояния в % (для статистики trades), не цены.
    """

    sl_price: Decimal
    tp2_price: Decimal
    sl_pct: Decimal
    tp_pct: Decimal
    tp1_price: Decimal | None = None


class LevelCalculator(Protocol):
    """Контракт калькулятора уровней (реализация — Часть 4)."""

    def calculate(
        self,
        price: Decimal,
        side: Side,
        high24: float,
        low24: float,
        structural_level: Decimal | None,
        klines_1h: Any,
    ) -> CalculatedLevels:
        """Считает SL/TP по цене входа; исключения — ошибка данных."""


class PercentLevelCalculator:
    """Простейший калькулятор по % (тесты + paper до интеграции).

    Инвариант: LONG -> sl < price < tp; SHORT -> зеркально.
    """

    def __init__(self, sl_pct: Decimal, tp_pct: Decimal) -> None:
        self._sl = sl_pct / Decimal("100")
        self._tp = tp_pct / Decimal("100")

    def calculate(
        self,
        price: Decimal,
        side: Side,
        high24: float,
        low24: float,
        structural_level: Decimal | None,
        klines_1h: Any,
    ) -> CalculatedLevels:
        del high24, low24, structural_level, klines_1h
        if side is Side.LONG:
            return CalculatedLevels(
                sl_price=price * (Decimal("1") - self._sl),
                tp2_price=price * (Decimal("1") + self._tp),
                sl_pct=self._sl * Decimal("100"),
                tp_pct=self._tp * Decimal("100"),
            )
        return CalculatedLevels(
            sl_price=price * (Decimal("1") + self._sl),
            tp2_price=price * (Decimal("1") - self._tp),
            sl_pct=self._sl * Decimal("100"),
            tp_pct=self._tp * Decimal("100"),
        )