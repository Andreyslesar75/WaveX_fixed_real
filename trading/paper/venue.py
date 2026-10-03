"""PaperVenue: симулятор Binance Futures для paper-режима.

Паритет с real по контракту (не по внутренностям):
- события исполнения (OrderUpdate/Fill/AccountUpdate) кладутся в
  ту же очередь events — движок потребляет единообразно;
- условные ордера (STOP_MARKET/TAKE_PROFIT_MARKET) триггерятся в
  feed_price тем же правилом направления, что и на бирже;
- UnknownOrderError/-2022/InsufficientFundsError воспроизводятся,
  чтобы аварийные ветки движка отрабатывали одинаково в обоих
  режимах.

Осознанные упрощения (зафиксированы, не «недоделки»):
- маржа не блокируется: paper отслеживает только realized PnL и
  комиссии (старый симулятор не моделировал маржу тоже);
- комиссия — taker 0.04% с каждой стороны (паритет со старым
  _calc_pnl); real берёт ставки из commissionRate;
- один ценовой поток: workingType MARK_PRICE == last (расхождения
  mark/last и priceProtect не моделируются — это биржевые аномалии,
  их место — real-тесты V-API);
- slippage применяется против нас (adverse) на каждом market-fill.

Конкурентность: один asyncio-цикл; feed_price синхронный,
события кладутся put_nowait — порядок сохраняется.
"""
from __future__ import annotations

import asyncio
import itertools
import logging
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Callable, Mapping

from ..types import (
    Fill, OrderAck, OrderKind, OrderRequest, OrderSide, OrderState,
    Side,
)
from ..venue import (
    ExchangePosition, ExecutionVenue, InsufficientFundsError,
    UnknownOrderError, VenueAccountUpdate, VenueEvent, VenueOrderUpdate,
)
from ..binance.rest import OrderNotFoundError

logger = logging.getLogger(__name__)

PriceProvider = Callable[[str], Decimal | None]


def _dec(value: str) -> Decimal:
    """Строковый Decimal-литерал (читаемость и одинаковость вычислений)."""
    return Decimal(value)


@dataclass
class _PaperPosition:
    """Внутренняя позиция симулятора (One-way, положительный qty)."""

    symbol: str
    side: Side
    qty: Decimal
    entry_price: Decimal
    entry_ts_ms: int


@dataclass
class _PaperOrder:
    """Внутреннее состояние ордера симулятора."""

    request: OrderRequest
    status: OrderState
    order_id: int
    ts_ms: int
    filled_qty: Decimal = Decimal("0")
    avg_price: Decimal | None = None


class PaperVenue(ExecutionVenue):
    """Симулятор биржи: исполнение по live-цене + slippage (Б2-1а)."""

    def __init__(
        self,
        starting_capital: Decimal,
        price_provider: PriceProvider,
        slippage_pct: float = 0.0,
        fee_pct: Decimal = Decimal("0.0004"),
        now_ms: Callable[[], int] | None = None,
    ) -> None:
        """fee_pct 0.0004 = 0.04% — паритет со старым симулятором."""
        import time as _time

        self._balance = starting_capital
        self._price_provider = price_provider
        self._slippage = Decimal(str(slippage_pct)) / Decimal("100")
        self._fee = fee_pct
        self._now_ms = now_ms or (lambda: int(_time.time() * 1000))
        self._positions: dict[str, _PaperPosition] = {}
        self._open: dict[str, _PaperOrder] = {}
        self._history: dict[str, _PaperOrder] = {}
        self._fills_by_symbol: dict[str, list[Fill]] = {}
        self._ids = itertools.count(1)
        self._events: asyncio.Queue[VenueEvent] = asyncio.Queue()

    @property
    def events(self) -> "asyncio.Queue[VenueEvent]":
        """Очередь событий — движок потребляет её как у RealVenue."""
        return self._events

    def feed_price(self, symbol: str, price: Decimal) -> None:
        """Тик цены: триггер условных ордеров, генерация событий."""
        for order in list(self._open.values()):
            if order.request.symbol != symbol:
                continue
            if order.status is not OrderState.NEW:
                continue
            if not _triggered(order.request, price):
                continue
            self._fill_conditional(order, price)

    async def execute_order(self, request: OrderRequest) -> OrderAck:
        """Исполнить MARKET немедленно; условный — поставить в книгу."""
        if request.kind is OrderKind.MARKET:
            return self._execute_market(request)
        position = self._positions.get(request.symbol)
        if request.qty is not None and request.reduce_only:
            if position is None or request.qty > position.qty:
                return self._reject(request, "-2022", "ReduceOnly: позиции нет/qty")
        order = _PaperOrder(
            request=request, status=OrderState.NEW,
            order_id=next(self._ids), ts_ms=self._now_ms(),
        )
        self._open[request.client_order_id] = order
        self._history[request.client_order_id] = order
        return OrderAck(
            client_order_id=request.client_order_id,
            exchange_order_id=order.order_id,
            status=OrderState.NEW,
            raw={"symbol": request.symbol, "stopPrice":
                 str(request.stop_price) if request.stop_price else None},
        )

    async def cancel_order(self, symbol: str, client_order_id: str) -> OrderAck:
        """Отмена ордера; воспроизводит -2011/-2013 (см. модуль)."""
        del symbol
        order = self._open.pop(client_order_id, None)
        if order is None:
            known = self._history.get(client_order_id)
            if known is not None:
                raise UnknownOrderError(
                    -2011, 200, "order already executed or canceled",
                    "/fapi/v1/order", None,
                )
            raise OrderNotFoundError(
                -2013, 200, "order does not exist", "/fapi/v1/order", None
            )
        order.status = OrderState.CANCELED
        self._emit_update(order, Decimal("0"))
        return OrderAck(
            client_order_id=client_order_id,
            exchange_order_id=order.order_id,
            status=OrderState.CANCELED,
            raw={"canceled": True},
        )

    async def query_order(self, symbol: str, client_order_id: str) -> OrderAck | None:
        """Статус из книги/истории; None — ордера никогда не было."""
        del symbol
        order = self._open.get(client_order_id) or self._history.get(client_order_id)
        if order is None:
            return None
        return OrderAck(
            client_order_id=client_order_id,
            exchange_order_id=order.order_id,
            status=order.status,
            avg_price=order.avg_price,
            executed_qty=order.filled_qty if order.filled_qty > 0 else None,
            raw={"paper": True},
        )

    async def open_orders(self, symbol: str) -> list[OrderAck]:
        """Активные условные ордера символа (Часть A защиты работает и в paper)."""
        result: list[OrderAck] = []
        for order in self._open.values():
            if order.request.symbol == symbol:
                ack = OrderAck(
                    client_order_id=order.request.client_order_id,
                    exchange_order_id=order.order_id,
                    status=order.status,
                    raw={"paper": True},
                )
                result.append(ack)
        return result

    async def cancel_all_orders(self, symbol: str) -> int:
        """Снять все ордера символа (аварийная ветка отрабатывает в paper)."""
        removed = [
            cid for cid, order in self._open.items()
            if order.request.symbol == symbol
        ]
        for cid in removed:
            order = self._open.pop(cid)
            order.status = OrderState.CANCELED
            self._emit_update(order, Decimal("0"))
        return len(removed)

    async def positions(self) -> list[ExchangePosition]:
        """Позиции симулятора (unrealized — по price_provider)."""
        result: list[ExchangePosition] = []
        for position in self._positions.values():
            price = self._price_provider(position.symbol)
            unrealized: Decimal | None = None
            if price is not None:
                unrealized = _unrealized(position, price)
            result.append(
                ExchangePosition(
                    symbol=position.symbol, side=position.side.value,
                    qty=position.qty, entry_price=position.entry_price,
                    unrealized_pnl=unrealized,
                )
            )
        return result

    async def available_balance(self, asset: str = "USDT") -> Decimal:
        """Доступный баланс (USDT в paper)."""
        if asset != "USDT":
            raise ValueError(f"paper поддерживает только USDT, не {asset}")
        return self._balance

    async def user_trades(self, symbol: str, start_ms: int) -> list[Fill]:
        """Сделки символа с start_ms (reconciliation в paper тоже работает)."""
        return [
            fill for fill in self._fills_by_symbol.get(symbol, [])
            if fill.ts_ms >= start_ms
        ]

    async def commission_rate(self, symbol: str) -> tuple[Decimal, Decimal]:
        """Ставки симулятора: maker == taker == fee_pct (паритет)."""
        del symbol
        return self._fee, self._fee

    # ---------- внутреннее ----------

    def _reject(self, request: OrderRequest, code: str, msg: str) -> OrderAck:
        """OrderAck REJECTED с кодом — движок обработает политику кода."""
        return OrderAck(
            client_order_id=request.client_order_id,
            exchange_order_id=None, status=OrderState.REJECTED,
            raw={"code": code, "msg": msg},
        )

    def _fill_price(self, side: OrderSide, price: Decimal) -> Decimal:
        """Цена market-fill: adverse slippage против нас (Б2-1а)."""
        if side is OrderSide.BUY:
            return price * (Decimal("1") + self._slippage)
        return price * (Decimal("1") - self._slippage)

    def _execute_market(self, request: OrderRequest) -> OrderAck:
        """MARKET: немедленное исполнение по live-цене + комиссии."""
        price = self._price_provider(request.symbol)
        if price is None:
            return self._reject(request, "-1002", "paper: нет live-цены")
        fill_price = self._fill_price(request.side, price)
        notional = request.qty * fill_price  # qty не None: инвариант OrderRequest
        position = self._positions.get(request.symbol)
        closing = position is not None and (
            request.side.value != position.side.order_side_entry.value
        )
        if request.reduce_only or closing:
            if position is None:
                return self._reject(request, "-2022", "ReduceOnly: позиции нет")
            if request.qty > position.qty:
                return self._reject(request, "-2022", "ReduceOnly: qty > позиции")
        else:
            if notional > self._balance:
                raise InsufficientFundsError(
                    -2019, 200, "paper: insufficient balance",
                    "/fapi/v1/order", None,
                )
        order = _PaperOrder(
            request=request, status=OrderState.FILLED,
            order_id=next(self._ids), ts_ms=self._now_ms(),
            filled_qty=request.qty, avg_price=fill_price,
        )
        self._history[request.client_order_id] = order
        commission = notional * self._fee
        realized = self._apply_fill(position, request, fill_price, request.qty)
        self._balance -= commission
        if realized is not None:
            self._balance += realized
        self._emit_fill(order, fill_price, request.qty, commission)
        return OrderAck(
            client_order_id=request.client_order_id,
            exchange_order_id=order.order_id, status=OrderState.FILLED,
            avg_price=fill_price, executed_qty=request.qty,
            raw={"paper": True, "commission": str(commission)},
        )

    def _fill_conditional(self, order: _PaperOrder, trigger_price: Decimal) -> None:
        """Триггер условного ордера: fill по цене тика + slippage."""
        position = self._positions.get(order.request.symbol)
        if position is None or position.qty <= 0:
            order.status = OrderState.CANCELED
            self._open.pop(order.request.client_order_id, None)
            self._emit_update(order, Decimal("0"))
            return
        qty = position.qty if order.request.close_position else min(
            order.request.qty or position.qty, position.qty
        )
        fill_price = self._fill_price(order.request.side, trigger_price)
        commission = qty * fill_price * self._fee
        realized = self._apply_fill(position, order.request, fill_price, qty)
        self._balance -= commission
        if realized is not None:
            self._balance += realized
        order.status = OrderState.FILLED
        order.filled_qty = qty
        order.avg_price = fill_price
        self._open.pop(order.request.client_order_id, None)
        self._emit_fill(order, fill_price, qty, commission)

    def _apply_fill(
        self, position: _PaperPosition | None, request: OrderRequest,
        fill_price: Decimal, qty: Decimal,
    ) -> Decimal | None:
        """Применить fill к позиции; вернуть realized PnL или None.

        Правила: same-side — усреднение входа; opposite-side —
        сокращение (переворот не поддерживается — движок не шлёт
        таких ордеров, а симулятор обязан быть честным об этом).
        """
        if position is None:
            self._positions[request.symbol] = _PaperPosition(
                symbol=request.symbol,
                side=Side.LONG if request.side is OrderSide.BUY else Side.SHORT,
                qty=qty, entry_price=fill_price, entry_ts_ms=self._now_ms(),
            )
            return None
        entry_exit = (
            position.side.order_side_entry.value == request.side.value
        )
        if entry_exit and not request.reduce_only:
            total = position.qty + qty
            position.entry_price = (
                position.entry_price * position.qty + fill_price * qty
            ) / total
            position.qty = total
            return None
        # закрывающий fill
        if position.side is Side.LONG:
            realized = (fill_price - position.entry_price) * qty
        else:
            realized = (position.entry_price - fill_price) * qty
        position.qty -= qty
        if position.qty <= 0:
            del self._positions[request.symbol]
        return realized

    def _emit_fill(
        self, order: _PaperOrder, price: Decimal, qty: Decimal, commission: Decimal
    ) -> None:
        """Сгенерировать полный набор событий исполнения (паритет real)."""
        self._events.put_nowait(
            VenueOrderUpdate(
                event=_order_update_event(order, price, qty, commission),
                raw={"paper": True, "clientOrderId": order.request.client_order_id},
            )
        )
        self._fills_by_symbol.setdefault(order.request.symbol, []).append(
            Fill(
                order_client_id=order.request.client_order_id,
                exchange_order_id=order.order_id,
                trade_id=next(self._ids), ts_ms=self._now_ms(),
                price=price, qty=qty, commission=commission,
                commission_asset="USDT",
            )
        )
        self._emit_account()

    def _emit_update(self, order: _PaperOrder, price: Decimal) -> None:
        """Событие без fill (отмена и т.п.)."""
        self._events.put_nowait(
            VenueOrderUpdate(
                event=_order_update_event(order, price, Decimal("0"), Decimal("0")),
                raw={"paper": True, "clientOrderId": order.request.client_order_id},
            )
        )

    def _emit_account(self) -> None:
        """AccountUpdate: баланс + позиции (движок обновляет кэш)."""
        self._events.put_nowait(
            VenueAccountUpdate(
                balances={"USDT": self._balance},
                positions={
                    p.symbol: ExchangePosition(
                        symbol=p.symbol, side=p.side.value, qty=p.qty,
                        entry_price=p.entry_price,
                        unrealized_pnl=_unrealized(p, self._price_provider(p.symbol)),
                    )
                    for p in self._positions.values()
                },
                raw={"paper": True},
            )
        )


def _unrealized(position: _PaperPosition, price: Decimal | None) -> Decimal | None:
    """Unrealized PnL позиции по цене (None, если цены нет)."""
    if price is None:
        return None
    if position.side is Side.LONG:
        return (price - position.entry_price) * position.qty
    return (position.entry_price - price) * position.qty


def _order_update_event(
    order: _PaperOrder, price: Decimal, qty: Decimal, commission: Decimal
):
    """OrderUpdateEvent для события симулятора (формат — types.OrderUpdateEvent)."""
    from ..types import OrderUpdateEvent  # локальный импорт: разрыв цикла типов

    return OrderUpdateEvent(
        ts_ms=order.ts_ms, symbol=order.request.symbol,
        client_order_id=order.request.client_order_id,
        exchange_order_id=order.order_id,
        raw_status=order.status.value, state=order.status,
        avg_price=price if price > 0 else None,
        last_filled_qty=qty if qty > 0 else None,
        accumulated_qty=order.filled_qty if order.filled_qty > 0 else None,
        commission=commission if commission > 0 else None,
        commission_asset="USDT" if commission > 0 else None,
        realized_pnl=None, is_maker=False,
    )


def _triggered(request: OrderRequest, price: Decimal) -> bool:
    """Правило триггера условного ордера (единое с биржей).

    STOP: SELL триггерится падением до stopPrice, BUY — ростом;
    TAKE_PROFIT — зеркально.
    """
    stop = request.stop_price
    if stop is None:
        return False
    if request.kind is OrderKind.STOP_MARKET:
        if request.side is OrderSide.SELL:
            return price <= stop
        return price >= stop
    if request.kind is OrderKind.TAKE_PROFIT_MARKET:
        if request.side is OrderSide.SELL:
            return price >= stop
        return price <= stop
    return False