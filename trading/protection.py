"""Защита позиции: Iron SL (каждый тик) + Часть B (§11 черновика).

Iron SL — локальный контур: не биржевой ордер; проверяется в
engine.feed_price на КАЖДОМ тике (решение Б2-2а: без debounce,
1 тик = срабатывание; каждый случай — инцидент iron_sl для разбора).

Часть B «SL не подтверждён активным» живёт в engine (нужны локи и
событийная книга); здесь — переиспользуемые чистые куски:
проверка триггера, классификация состояния SL, цикл восстановления.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum, auto

from .types import OrderAck, OrderRequest, OrderState, PositionSnapshot, Side


def iron_triggered(pos: PositionSnapshot, price: Decimal) -> bool:
    """Пересёк ли тик iron-уровень позиции.

    Инвариант: iron_sl_price всегда хуже биржевого SL; если мы здесь,
    штатный SL не отработал (или не успел) — это диагностический
    инцидент, а не норма (§10 черновика).
    """
    if pos.iron_sl_price is None:
        return False
    if pos.side is Side.LONG:
        return price <= pos.iron_sl_price
    return price >= pos.iron_sl_price


class SlHealth(Enum):
    """Вердикт проверки активности биржевого SL (Часть A)."""

    OK = auto()          # SL числится активным
    MISSING = auto()     # SL отсутствует/не активен -> Часть B


def sl_health(sl_ack: OrderAck | None, tracked_state: OrderState | None) -> SlHealth:
    """SL активен по локальному трекингу + последнему факту биржи.

    Args:
        sl_ack: последний OrderAck SL (из open_orders/query) или None;
        tracked_state: состояние из книги движка или None.

    Returns:
        OK если хотя бы один источник подтверждает NEW; MISSING —
        если активного подтверждения нет (запуск Части B).
    """
    states = {s for s in (tracked_state, sl_ack.status if sl_ack else None)
              if s is not None}
    if OrderState.NEW in states or OrderState.PARTIALLY_FILLED in states:
        return SlHealth.OK
    return SlHealth.MISSING


@dataclass(frozen=True, slots=True)
class RestoreResult:
    """Итог попыток восстановления SL."""

    ok: bool
    ack: OrderAck | None = None
    attempts: int = 0
    detail: str = ""


async def restore_stop_market(
    venue,  # ExecutionVenue (протокол; без импорта — цикл зависимостей)
    request: OrderRequest,
    attempts: int,
    interval_s: float,
) -> RestoreResult:
    """Поставить STOP_MARKET closePosition повторно (Часть B, шаг 3).

    Args:
        venue: ExecutionVenue (тот же request переиспользуется —
            анти-П1: side/цена не пересобираются между попытками);
        request: неизменяемый OrderRequest восстановления;
        attempts: число попыток (2–3 по черновику);
        interval_s: пауза между попытками (доли секунды).

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