"""Расчёт уровней SL/TP: обёртка над calculations.py (НЕ меняем его, В1).

Паритет 1:1 со старым путём risk_manager.open_position:
- calc_sl_tp / calc_sl_tp_short — те же аргументы в том же порядке;
- fixup-блок защиты от «уровней не с той стороны» — дословно из старого кода;
- tp_pct для статистики = tp1_pct (как в событиях старого трекера).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Protocol

from .types import Side


@dataclass(frozen=True, slots=True)
class CalculatedLevels:
    """Уровни сделки до округления по tick (округляет движок)."""

    sl_price: Decimal
    tp1_price: Decimal
    tp2_price: Decimal
    sl_pct: Decimal
    tp_pct: Decimal          # = tp1_pct (паритет со старыми событиями)
    sl_source: str = "unknown"


class LevelCalculator(Protocol):
    """Контракт калькулятора (engine зависит только от него)."""

    def calculate(
        self, price: Decimal, side: Side, high24: float, low24: float,
        structural_level: Decimal | None, klines_1h: Any,
        spread_pct: float = 0.0,
    ) -> CalculatedLevels:
        """Считает уровни; исключения — ошибка данных (реджект сигнала)."""


def _d(value: float | int | Decimal) -> Decimal:
    """float->Decimal через str (без binary-мусора)."""
    return Decimal(str(value))


class CalculationsLevelCalculator:
    """Реальная обёртка над calculations.py (LONG+SHORT+fixups)."""

    def __init__(self) -> None:
        """Импорт calculations внутри: config-зависимость не тянется в тесты движка."""
        from calculations import calc_sl_tp, calc_sl_tp_short

        self._long = calc_sl_tp
        self._short = calc_sl_tp_short

    def calculate(
        self, price: Decimal, side: Side, high24: float, low24: float,
        structural_level: Decimal | None, klines_1h: Any,
        spread_pct: float = 0.0,
    ) -> CalculatedLevels:
        """calc_* + fixup-блок 1:1 (см. модуль). Возвращает уровни+sl_source."""
        from config import Config  # пороги fixup — из Config, как в старом коде

        entry = float(price)
        structural = float(structural_level) if structural_level is not None else None
        if side is Side.SHORT:
            sl, sl_pct, tp1, _tp1p, tp2, _tp2p, src = self._short(
                entry, klines_1h, structural, spread_pct, high24, low24,
            )
            # fixup 1:1: SHORT — SL выше входа, TP ниже
            if sl <= entry:
                sl_pct = float(Config.ATR_SL_MIN_PCT)
                sl = entry * (1 + sl_pct / 100)
            if tp1 >= entry:
                tp1_pct = sl_pct * float(Config.FIRST_TP_MULTIPLIER)
                tp1 = entry * (1 - tp1_pct / 100)
                tp2_pct = sl_pct * float(Config.SECOND_TP_MULTIPLIER)
                tp2 = entry * (1 - tp2_pct / 100)
        else:
            sl, sl_pct, tp1, _tp1p, tp2, _tp2p, src = self._long(
                entry, klines_1h, structural, spread_pct, high24, low24,
            )
            if sl >= entry:
                sl_pct = float(Config.ATR_SL_MIN_PCT)
                sl = entry * (1 - sl_pct / 100)
            if tp1 <= entry:
                tp1_pct = sl_pct * float(Config.FIRST_TP_MULTIPLIER)
                tp1 = entry * (1 + tp1_pct / 100)
                tp2_pct = sl_pct * float(Config.SECOND_TP_MULTIPLIER)
                tp2 = entry * (1 + tp2_pct / 100)
        return CalculatedLevels(
            sl_price=_d(sl), tp1_price=_d(tp1), tp2_price=_d(tp2),
            sl_pct=_d(sl_pct), tp_pct=_d(_tp1p if _tp1p else sl_pct), sl_source=src,
        )


class PercentLevelCalculator:
    """Процентный калькулятор для тестов/paper (структура вывода — как у обёртки)."""

    def __init__(self, sl_pct: Decimal, tp2_mult: Decimal = Decimal("1.1")) -> None:
        """tp1 = sl_pct × 1.0, tp2 = sl_pct × tp2_mult — как в Config."""
        self._sl = sl_pct / Decimal("100")
        self._tp2 = tp2_mult

    def calculate(
        self, price: Decimal, side: Side, high24: float, low24: float,
        structural_level: Decimal | None, klines_1h: Any,
        spread_pct: float = 0.0,
    ) -> CalculatedLevels:
        del high24, low24, structural_level, klines_1h, spread_pct
        tp1 = self._sl  # FIRST_TP_MULTIPLIER = 1.0
        tp2 = self._sl * self._tp2
        if side is Side.LONG:
            return CalculatedLevels(
                sl_price=price * (1 - self._sl), tp1_price=price * (1 + tp1),
                tp2_price=price * (1 + tp2), sl_pct=self._sl * 100,
                tp_pct=tp1 * 100, sl_source="PercentCalc",
            )
        return CalculatedLevels(
            sl_price=price * (1 + self._sl), tp1_price=price * (1 - tp1),
            tp2_price=price * (1 - tp2), sl_pct=self._sl * 100,
            tp_pct=tp1 * 100, sl_source="PercentCalc",
        )
