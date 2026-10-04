# trading/protection.py
"""Защита позиции: Iron SL (каждый тик) + цикл восстановления SL (Часть B).

Iron SL — локальный контур: не биржевой ордер; проверяется движком на
КАЖДОМ тике (решение Б2-2а: без debounce, 1 тик = срабатывание;
каждый случай — инцидент iron_sl для разбора).

SlHealth/sl_health удалены (Е17): вердикт Части A принял на себя
REST-контроль движка, двойная классификация не нужна.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol

from .types import OrderAck, OrderRequest, OrderState, Side
from .venue import ExecutionVenue


class IronPositionLike(Protocol):
    """Минимальный контракт позиции для проверки iron-уровня."""

    side: Side
    iron_sl_price: Decimal | None


def iron_triggered(pos: IronPositionLike, price: Decimal) -> bool:
    """Пересёк ли тик iron-уровень позиции.

    Инвариант: iron_sl_price всегда хуже биржевого SL; если мы здесь,
    штатный SL не отработал (или не успел) — диагностический инцидент,
    а не норма (§10 черновика).
    """
    if pos.iron_sl_price is None:
        return False
    if pos.side is Side.LONG:
        return price <= pos.iron_sl_price
    return price >= pos.iron_sl_price


@dataclass(frozen=True, slots=True)
class RestoreResult:
    """Итог попыток восстановления SL."""

    ok: bool
    ack: OrderAck | None = None
    attempts: int = 0
    detail: str = ""


async def restore_stop_market(
    venue: ExecutionVenue,
    request: OrderRequest,
    attempts: int,
    interval_s: float,
) -> RestoreResult:
    """Поставить STOP_MARKET closePosition повторно (Часть B, шаг 3).

    Тот же request переиспользуется во всех попытках (анти-П1:
    side/цена не пересобираются между попытками).

    Returns:
        RestoreResult: ok + ack первой успешной постановки; при
        провале — attempts и последняя причина.
    """
    detail = ""
    for i in range(1, attempts + 1):
        try:
            ack = await venue.execute_order(request)
        except Exception as exc:  # транспорт venue — политика выше
            detail = f"attempt {i}: {exc}"
            await asyncio.sleep(interval_s)
            continue
        if ack.status in (OrderState.NEW, OrderState.PARTIALLY_FILLED):
            return RestoreResult(ok=True, ack=ack, attempts=i)
        detail = f"attempt {i}: status={ack.status.value} raw={dict(ack.raw)}"
        await asyncio.sleep(interval_s)
    return RestoreResult(ok=False, attempts=attempts, detail=detail)
