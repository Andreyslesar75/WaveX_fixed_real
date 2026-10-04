# trading/money.py
"""Арифметика денег/объёмов: Decimal + округление по правилам биржи.

Почему Decimal, а не float: двоичное представление 0.1 не точное —
на объёмах и ценах это даёт расхождения, недопустимые при округлении
qty по stepSize (риск отправить некратный шагу qty -> -1013).

Правила округления (почему):
- qty всегда floor до stepSize: увеличить объём нельзя (маржа),
  уменьшить — безопасно;
- цены условных ордеров — к tickSize: LONG -> floor, SHORT -> ceil.
  Для SL это «дальше» от входа (шире защита), для TP — «ближе»
  (консервативнее по прибыли); направление округления совпадает,
  семантика разная — см. round_price_tick;
- notional входа проверяется с запасом x1.05: к моменту исполнения
  MARKET цена может уйти вниз — запас снижает вероятность -1013
  на реальном ордере.

Исключения: MoneyError при NaN/Inf/step<=0 — всегда ошибка данных
на границе, глотать нельзя.
"""
from __future__ import annotations

from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, localcontext
from typing import Final

from .types import RejectReason, Side, SymbolFilters

#: Запас над minNotional при расчёте входа.
NOTIONAL_SAFETY: Final[Decimal] = Decimal("1.05")

#: Результат расчёта qty: (ok, None) | (None, причина).
QtyResult = tuple[Decimal, None] | tuple[None, RejectReason]


class MoneyError(ValueError):
    """Некорректные денежные данные (NaN/Inf/step<=0) — ошибка границы."""


def D(value: str | int | float | Decimal) -> Decimal:
    """Конвертировать в Decimal с запретом NaN/Inf.

    Args:
        value: строка биржи, int/float из Config или Decimal.

    Returns:
        Конечный Decimal.

    Raises:
        MoneyError: NaN/Infinity/неконвертируемое значение.
    """
    try:
        d = Decimal(str(value))
    except (ArithmeticError, ValueError) as exc:
        raise MoneyError(f"не конвертируется в Decimal: {value!r}") from exc
    if not d.is_finite():
        raise MoneyError(f"NaN/Inf запрещены: {value!r}")
    return d


def _quantize_to_step(value: Decimal, step: Decimal, rounding: str) -> Decimal:
    """Округлить value к кратному step в заданном режиме (FLOOR/CEILING).

    Raises:
        MoneyError: step <= 0.
    """
    if step <= 0:
        raise MoneyError(f"step должен быть > 0, получен {step}")
    with localcontext() as ctx:
        ctx.prec = 60  # деление value/step может быть бесконечной дробью
        units = (value / step).to_integral_value(rounding=rounding)
        return units * step


def floor_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Округлить вниз к шагу (qty)."""
    return _quantize_to_step(value, step, ROUND_FLOOR)


def ceil_to_step(value: Decimal, step: Decimal) -> Decimal:
    """Округлить вверх к шагу (диагностика/тесты)."""
    return _quantize_to_step(value, step, ROUND_CEILING)


def round_price_tick(price: Decimal, side: Side, tick: Decimal) -> Decimal:
    """Округлить цену условного ордера к tickSize.

    LONG -> floor, SHORT -> ceil. Семантика двойная (модуль): для SL
    это «дальше от входа», для TP — «ближе к входу»; направление
    округления одно, поэтому функция общая.
    """
    rounding = ROUND_FLOOR if side is Side.LONG else ROUND_CEILING
    return _quantize_to_step(price, tick, rounding)


def to_api_str(value: Decimal) -> str:
    """Строка для REST-параметра: без экспоненты, без хвостовых нулей."""
    return format(value.normalize(), "f")


def compute_entry_qty(size_usdt: Decimal, ref_price: Decimal, filters: SymbolFilters) -> QtyResult:
    """Вычислить qty входа: floor до stepSize + проверки минимумов.

    Args:
        size_usdt: расчётный объём позиции (FIXED_POSITION_SIZE_USDT);
        ref_price: референсная цена сигнала;
        filters: валидированные фильтры символа.

    Returns:
        (qty, None) при успехе; (None, точная причина) при провале —
        причина пишется в signals.reject_reason (статистика реджектов).
    """
    if size_usdt <= 0 or ref_price <= 0:
        return None, RejectReason.INVALID_SIGNAL
    with localcontext() as ctx:
        ctx.prec = 60
        raw_qty = size_usdt / ref_price
    qty = floor_to_step(raw_qty, filters.step_size)
    if qty < filters.min_qty:
        return None, RejectReason.QTY_BELOW_MIN
    if qty > filters.max_qty:
        return None, RejectReason.QTY_ABOVE_MAX
    if qty * ref_price < filters.min_notional * NOTIONAL_SAFETY:
        return None, RejectReason.NOTIONAL_BELOW_MIN
    return qty, None


def is_valid_exit_qty(qty: Decimal, ref_price: Decimal, filters: SymbolFilters) -> bool:
    """Проверить исполнимость закрывающего (reduceOnly) объёма.

    Без запаса NOTIONAL_SAFETY: реджект выхода — не вход, он
    обрабатывается аварийной веткой (Часть B / форс), а не реджектом.
    """
    return qty >= filters.min_qty and qty * ref_price >= filters.min_notional


def validate_split(
    total_qty: Decimal, share_qty: Decimal, ref_price: Decimal, filters: SymbolFilters
) -> bool:
    """Проверить допустимость дробления позиции на две закрывающие части.

    Правило «мельчайшей части» (требование владельца): если хотя бы
    одна часть (TP1-доля или остаток) меньше minQty или её notional
    меньше minNotional — дробить нельзя; позиция закрывается одним
    TP (см. Д8).
    """
    remainder = total_qty - share_qty
    if share_qty <= 0 or remainder <= 0:
        return False
    return is_valid_exit_qty(share_qty, ref_price, filters) and is_valid_exit_qty(
        remainder, ref_price, filters
    )
