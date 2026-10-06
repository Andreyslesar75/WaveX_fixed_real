"""Оркестратор торговой части: сигналы, исполнение, сопровождение,
защита, события venue, сверка.

Модель конкурентности (Д3): один asyncio-цикл; per-symbol
asyncio.Lock на все операции над позицией (вход, закрытия, amend,
Часть B, Iron SL); события venue кладутся в очередь и обрабатываются
ОДНИМ потребителем — события никогда не торгуют напрямую, только
движок решает (устраняет класс бага П2).

Правило закрытия: флаг self._closing + лок; при гонке «наш market и
событие SL» побеждает то, что первым захватило лок, второе видит
отсутствие позиции и завершается no-op'ом.

Iron SL: engine.feed_price кладёт тик в _iron_q; задача _iron_loop
проверяет НЕМЕДЛЕННО (не ждёт монитора) — решение Б2-2а.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, cast

from .filters import FiltersCache
from .gates import GateState, check_gates
from .levels import LevelCalculator
from .manage import (
    breakeven_price,
    check_position,
    improves,
    profit_pct,
    update_mfe_mae,
)
from .money import (
    floor_to_step,
    round_price_tick,
)
from .notifier import Notifier
from .protection import iron_triggered, restore_stop_market
from .reconcile import Reconciler
from .settings import EngineSettings
from .storage import OrderRow, Storage, StoredPosition, TradeRecord
from .types import (
    Confidence,
    ExitReason,
    Fill,
    IncidentType,
    Mode,
    OrderAck,
    OrderKind,
    OrderRequest,
    OrderSide,
    OrderState,
    RejectReason,
    Side,
    SignalInput,
    SymbolFilters,
    make_client_id,
)
from .venue import (
    ExchangePosition,
    ExecutionVenue,
    UnknownOrderError,
    VenueAccountUpdate,
    VenueEvent,
    VenueOrderUpdate,
    VenueReconnected,
)

logger = logging.getLogger(__name__)

#: Источник свечей для VOL_DECAY: (symbol, interval, limit) -> сырые klines.
KlinesProvider = Callable[[str, str, int], Awaitable[list[Any]]]

@dataclass(slots=True)
class EntryIntent:
    """Входной сигнал в терминах движка (1:1 с open_position)."""

    symbol: str
    price: Decimal
    score: float
    confidence: str
    side: Side
    klines_1h: Any
    high24: float
    low24: float
    structural_level: Decimal | None
    spread_pct: float
    btc_trend: float


@dataclass(slots=True)
class TrackedOrder:
    """Ордер в книге движка (auditable runtime + role)."""

    client_order_id: str
    role: str  # ENTRY/SL/TP1/TP2/RS/FC/MC
    symbol: str
    side: OrderSide
    state: OrderState
    row_id: int
    qty: Decimal | None
    exchange_order_id: int | None = None
    commission_usdt: Decimal = Decimal("0")
    commission_bnb: Decimal = Decimal("0")
    filled_qty: Decimal = Decimal("0")
    avg_price: Decimal | None = None


@dataclass(slots=True)
class ManagedPosition:
    """Runtime-позиция движка (авторитетна в памяти; БД — снапшот)."""

    symbol: str
    side: Side
    signal_id: int
    entry_ts_ms: int
    entry_price: Decimal
    qty: Decimal
    size_usdt: Decimal
    score: float
    sl_price: Decimal
    local_sl_price: Decimal
    tp1_price: Decimal | None
    tp2_price: Decimal | None
    iron_sl_price: Decimal | None  # Optional: паритет с протоколом защиты
    sl_client_id: str | None
    tp1_client_id: str | None
    tp2_client_id: str | None
    entry_client_id: str
    tp1_done: bool = False
    tp2_done: bool = False
    breakeven_done: bool = False
    trail_active: bool = False
    unprotected: bool = False
    mfe_price: Decimal | None = None
    mae_price: Decimal | None = None
    realized_partial: Decimal = Decimal("0")
    fees_usdt: Decimal = Decimal("0")
    exit_fill_notional: Decimal = Decimal("0")
    exit_fill_qty: Decimal = Decimal("0")
    sl_pct: float = 0.0
    tp_pct: float = 0.0
    updated_ms: int = 0

    def to_stored(self, now_ms: int) -> StoredPosition:
        """Конверсия в снапшот БД (граница storage)."""
        return StoredPosition(
            symbol=self.symbol, side=self.side.value, entry_ts=self.entry_ts_ms,
            entry_price=self.entry_price, qty=self.qty, size_usdt=self.size_usdt,
            score=self.score, sl_price=self.sl_price, signal_id=self.signal_id,
            iron_sl_price=self.iron_sl_price, tp1_price=self.tp1_price,
            tp2_price=self.tp2_price, sl_client_id=self.sl_client_id,
            tp1_client_id=self.tp1_client_id, tp2_client_id=self.tp2_client_id,
            tp1_done=self.tp1_done, tp2_done=self.tp2_done,
            breakeven_done=self.breakeven_done, trail_active=self.trail_active,
            updated_ms=now_ms,
        )


class TradingEngine:
    """Единый оркестратор торговой части (real/paper через один venue-контракт)."""

    def __init__(
        self,
        venue: ExecutionVenue,
        storage: Storage,
        settings: EngineSettings,
        filters_cache: FiltersCache,
        calculator: LevelCalculator,
        notifier: Notifier,
        mode: Mode,
        capital_base: Decimal,
        now_ms: Callable[[], int] | None = None,
        recon_interval_min: float | None = None,
        volume_provider: KlinesProvider | None = None,
    ) -> None:
        """capital_base: real — wallet на старте; paper — STARTING_CAPITAL.

        now_ms — инъекция времени для тестов. volume_provider — источник
        klines для VOL_DECAY (реальный — REST сканера; None = ветка спит).
        """

        self._venue = venue
        self._storage = storage
        self._settings = settings
        self._filters = filters_cache
        self._calculator = calculator
        self._notifier = notifier
        self._mode = mode
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))
        self._capital_base = capital_base
        self._realized_total = Decimal("0")
        self._positions: dict[str, ManagedPosition] = {}
        self._orders: dict[str, TrackedOrder] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._gate_state = GateState()
        self._prices: dict[str, Decimal] = {}
        self._iron_q: asyncio.Queue[tuple[str, Decimal]] = asyncio.Queue()
        self._closing: set[str] = set()
        self._ready = asyncio.Event()
        self._stop: asyncio.Event | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._last_rest_check: dict[str, int] = {}
        self._volume_provider = volume_provider
        self._vol_last_fetch: dict[str, int] = {}
        self._breakevens = 0
        self._last_equity: Decimal = capital_base
        self._reconciler = Reconciler(venue, storage, self, mode, notifier)
        self._recon_interval_min = recon_interval_min or settings.reconcile_interval_min
        self._equity_dirty = False

    # ----------------------------------------------------------------
    # Реализация EnginePort для Reconciler (protocol из reconcile.py)
    # ----------------------------------------------------------------

    def lock(self, symbol: str) -> asyncio.Lock:
        """Per-symbol лок (создаётся лениво, один на символ)."""
        return self._locks.setdefault(symbol, asyncio.Lock())

    def local_position(self, symbol: str) -> ManagedPosition | None:
        """Позиция из runtime-книги движка."""
        return self._positions.get(symbol)

    def local_symbols(self) -> list[str]:
        """Все открытые символы (лёгкая сверка)."""
        return list(self._positions)

    def tracked_order_ids(self, symbol: str) -> set[str]:
        """Все живые clientOrderId позиции (SL/TP1/TP2) — «не сироты»."""
        pos = self._positions.get(symbol)
        if pos is None:
            return set()
        return {
            cid
            for cid in (pos.sl_client_id, pos.tp1_client_id, pos.tp2_client_id)
            if cid
        }

    async def cancel_order_safe(self, symbol: str, client_order_id: str) -> None:
        """Отмена без поднятия исключений (recon-контекст; ошибки логируются)."""
        try:
            await self._venue.cancel_order(symbol, client_order_id)
        except (UnknownOrderError, Exception) as exc:
            logger.warning("[RECON] cancel %s: %s", client_order_id, exc)

    # ----------------------------------------------------------------
    # Публичное API (для фасада Части 4)
    # ----------------------------------------------------------------

    @property
    def ready(self) -> bool:
        """Разрешены ли входы (стартовая сверка прошла)."""
        return self._ready.is_set()

    async def startup_reconcile(self) -> None:
        """Блокирующая стартовая сверка; затем — разрешение входов."""
        await self._reconciler.startup()
        self._sync_daily_counters()
        self._ready.set()
        logger.info("engine: готов (mode=%s)", self._mode.value)

    def feed_prices(self, prices: Mapping[str, Decimal]) -> None:
        """Тик цен от сканера: обновить кэш, прогнать paper, Iron-очередь.

        Синхронный метод (вызывается из async-кода фасада): реальные
        действия отложены в _iron_q/_monitor — здесь только put_nowait.
        """
        for symbol, price in prices.items():
            self._prices[symbol] = price
            self._venue.feed_price(symbol, price)  # paper: триггеры
            pos = self._positions.get(symbol)
            if pos is not None and not self._closing.__contains__(symbol):
                self._iron_q.put_nowait((symbol, price))

    async def submit_signal(self, intent: EntryIntent) -> tuple[bool, str]:
        """Обработать сигнал: гейты -> вход -> SL/TP. Возвращает (ok, причина).

        Возвращает (True, "") при успехе; иначе — точная причина реджекта
        (паритет возвращаемого значения старого open_position).
        """
        if not self._ready.is_set():
            return False, "движок не готов: сверка не завершена"
        now = self._now_ms()
        try:
            signal = SignalInput(
                symbol=intent.symbol, side=intent.side, price=intent.price,
                score=intent.score,
                confidence=cast(Confidence, intent.confidence),
                spread_pct=intent.spread_pct, btc_trend=intent.btc_trend,
                high24=intent.high24, low24=intent.low24,
                structural_level=intent.structural_level,
            )
        except Exception as exc:
            self._log_reject(intent, RejectReason.INVALID_SIGNAL, str(exc), now)
            return False, f"invalid_signal: {exc}"
        filters = self._filters.get(intent.symbol)
        if filters is None:
            self._log_reject(intent, RejectReason.SYMBOL_NOT_TRADING,
                             "нет в кэше exchangeInfo", now)
            return False, "symbol_not_trading"
        levels = self._calculator.calculate(
            intent.price, intent.side, intent.high24, intent.low24,
            intent.structural_level, intent.klines_1h, intent.spread_pct,
        )
        balance = await self._safe_balance() if self._mode is Mode.REAL else None
        outcome = check_gates(signal, levels, self._gate_state, self._settings,
                              filters, balance, now)
        if not outcome.ok:
            assert outcome.reason is not None
            self._log_reject(intent, outcome.reason, outcome.detail, now)
            return False, f"{outcome.reason.value}: {outcome.detail}"
        async with self.lock(intent.symbol):
            if intent.symbol in self._positions:  # двойная проверка под локом
                self._log_reject(intent, RejectReason.DUPLICATE, "гонка входов", now)
                return False, "duplicate"
            qty = outcome.qty
            assert qty is not None  # гейты прошли => qty посчитан
            sid = self._storage.insert_signal(
                now, self._mode, intent.symbol, intent.side.value, intent.score,
                intent.confidence, "accepted", None, {"btc_trend": intent.btc_trend},
            )
            entry_cid = make_client_id(sid, "in")
            sl_price = round_price_tick(levels.sl_price, intent.side, filters.tick_size)
            tp1_price = round_price_tick(levels.tp1_price, intent.side, filters.tick_size)
            tp2_price = round_price_tick(levels.tp2_price, intent.side, filters.tick_size)
            entry_req = OrderRequest(
                client_order_id=entry_cid, symbol=intent.symbol,
                side=intent.side.order_side_entry, kind=OrderKind.MARKET, qty=qty,
                signal_id=sid,
            )
            ack = await self._venue.execute_order(entry_req)
            self._track_new_order(entry_req, "ENTRY", ack, entry_cid)
            if ack.status is OrderState.REJECTED:
                await self._handle_entry_failure(intent, sid, ack)
                return False, f"order_failed: {ack.status.value}"
            if ack.status is OrderState.TIMEOUT_UNKNOWN:
                await self._handle_entry_failure(intent, sid, ack)
                return False, "order_unconfirmed: timeout"
            # MARKET-POST часто отвечает NEW без avgPrice/executedQty —
            # финал прилетает WS-событием; дождаться его опросом
            # (идемпотентно, по clientOrderId) и ставить SL/TP на факт
            deadline = self._now_ms() + 10_000
            while ack.status in (OrderState.NEW, OrderState.PARTIALLY_FILLED):
                if self._now_ms() > deadline:
                    break
                await asyncio.sleep(0.2)
                try:
                    ack2 = await self._venue.query_order(
                        intent.symbol, entry_cid
                    )
                except Exception as exc:
                    logger.warning("entry resolve %s: %s", entry_cid, exc)
                    ack2 = None
                if ack2 is not None:
                    ack = ack2
                    self._track_update_order(entry_req, "ENTRY", ack, entry_cid)
            executed = ack.executed_qty or qty
            avg = ack.avg_price or intent.price
            pos = ManagedPosition(
                symbol=intent.symbol, side=intent.side, signal_id=sid,
                entry_ts_ms=now, entry_price=avg, qty=executed,
                size_usdt=self._settings.position_size_usdt, score=intent.score,
                sl_price=sl_price, local_sl_price=sl_price,
                tp1_price=tp1_price, tp2_price=tp2_price,
                iron_sl_price=self._iron_price(sl_price, intent.side, filters),
                sl_client_id=None, tp1_client_id=None, tp2_client_id=None,
                entry_client_id=entry_cid,
                sl_pct=float(levels.sl_pct), tp_pct=float(levels.tp_pct),
                updated_ms=now,
            )
            self._positions[intent.symbol] = pos
            self._gate_state.open_symbols.add(intent.symbol)
            ok = await self._place_protection(pos, filters, sid, executed, now)
            if not ok:
                return True, "вход исполнен, защита в аварийной ветке"
            self._persist_position(pos, now)
            self._write_equity("open", intent.symbol, now)
            logger.info("engine: вход %s %s qty=%s entry=%s sl=%s tp2=%s",
                        intent.symbol, intent.side.value, executed, avg,
                        sl_price, tp2_price)
            return True, levels.sl_source

    # ----------------------------------------------------------------
    # Задачи жизненного цикла
    # ----------------------------------------------------------------

    async def run(self, stop: asyncio.Event) -> None:
        """Запустить все фоновые задачи движка (после startup_reconcile)."""
        self._stop = stop
        self._tasks = [
            asyncio.create_task(self._consume_loop(), name="engine-events"),
            asyncio.create_task(self._iron_loop(), name="engine-iron"),
            asyncio.create_task(self._monitor_loop(), name="engine-monitor"),
            asyncio.create_task(self._reconcile_loop(), name="engine-recon"),
        ]
        await stop.wait()
        for task in self._tasks:
            task.cancel()
        # дождаться фактического завершения (тесты/рестарт без «зависших» задач)
        await asyncio.gather(*self._tasks, return_exceptions=True)


    async def _consume_loop(self) -> None:
        """Единственный потребитель событий venue (анти-П2)."""
        while True:
            event: VenueEvent = await self._venue.events.get()
            try:
                await self._dispatch(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.exception("engine: ошибка обработки события: %s", exc)
                self._incident(IncidentType.UNKNOWN_ORDER_STATUS, None, "critical",
                               f"event error: {exc}")

    async def _iron_loop(self) -> None:
        """Iron SL: немедленная обработка каждого тика (Б2-2а)."""
        while True:
            symbol, price = await self._iron_q.get()
            pos = self._positions.get(symbol)
            if pos is None or symbol in self._closing:
                continue
            if iron_triggered(pos, price):
                async with self.lock(symbol):
                    pos2 = self._positions.get(symbol)
                    if pos2 is None or symbol in self._closing:
                        continue
                    self._incident(
                        IncidentType.IRON_SL, symbol, "critical",
                        f"mark={price} iron={pos2.iron_sl_price} "
                        f"sl={pos2.sl_price} local={pos2.local_sl_price}",
                    )
                    await self._close_position_locked(
                        symbol, ExitReason.IRON_SL, "iron_sl"
                    )

    async def _monitor_loop(self) -> None:
        """Монитор каждые settings.monitor_interval_sec (единственный — анти-П3)."""
        while True:
            await asyncio.sleep(self._settings.monitor_interval_sec)
            now = self._now_ms()
            self._roll_daily(now)
            for symbol in list(self._positions):
                pos = self._positions.get(symbol)
                if pos is None or symbol in self._closing:
                    continue
                price = self._prices.get(symbol)
                async with self.lock(symbol):
                    pos = self._positions.get(symbol)
                    if pos is None or symbol in self._closing:
                        continue
                    if price is not None:
                        update_mfe_mae(pos, price)
                    # manage вызывается ВСЕГДА: TIMEOUT/VOL_DECAY не зависят
                    # от цены (внутри check_position ценовые ветки гвардятся)
                    ratio = await self._vol_ratio_if_due(symbol, pos, now)
                    action = check_position(pos, price, now, self._settings, ratio)
                    if action.kind == "breakeven" and action.new_local_sl:
                        pos.breakeven_done = True
                        pos.local_sl_price = action.new_local_sl
                        self._persist_position(pos, now)
                        continue
                    if action.kind == "close":
                        assert action.exit_reason is not None
                        await self._close_position_locked(
                            symbol, action.exit_reason, action.detail
                        )
                        continue
                    if action.kind == "trail_move" and action.new_local_sl:
                        pos.local_sl_price = action.new_local_sl
                        self._persist_position(pos, now)
                    await self._part_a(symbol, pos, now)
            self._maybe_write_equity_periodic(now)

    async def _reconcile_loop(self) -> None:
        """Периодическая лёгкая сверка (RECONCILE_INTERVAL_MIN)."""
        while True:
            await asyncio.sleep(self._recon_interval_min * 60.0)
            try:
                await self._reconciler.light_check()
            except Exception as exc:
                logger.exception("engine: периодическая сверка упала: %s", exc)

    # ----------------------------------------------------------------
    # Обработка событий venue
    # ----------------------------------------------------------------

    async def _dispatch(self, event: VenueEvent) -> None:
        """Маршрутизация события по типу (единственная точка входа)."""
        if isinstance(event, VenueOrderUpdate):
            await self._on_order_update(event)
        elif isinstance(event, VenueAccountUpdate):
            pass  # баланс читается по требованию (available_balance)
        elif isinstance(event, VenueReconnected):
            self._incident(IncidentType.WS_RECONNECT, None, "warning", event.reason)
            for symbol in list(self._positions):
                await self._reconciler.full_symbol(symbol)

    async def _on_order_update(self, update: VenueOrderUpdate) -> None:
        """ORDER_TRADE_UPDATE: обновить книгу, исполнить последствия."""
        ev = update.event
        tracked = self._orders.get(ev.client_order_id)
        if tracked is None:
            if ev.client_order_id.startswith("wx"):
                self._incident(IncidentType.UNKNOWN_ORDER_STATUS, ev.symbol,
                               "critical", f"неизвестный наш ордер {ev.client_order_id}")
            return
        if ev.state is not None:
            tracked.state = ev.state
            self._storage.update_order_status(
                tracked.row_id, ev.state.value, ev.exchange_order_id,
                dict(update.raw),
            )
        if ev.state is not OrderState.FILLED:
            return
        # исполнения
        if ev.commission is not None and (ev.commission_asset or "USDT") == "USDT":
            tracked.commission_usdt += ev.commission
        elif ev.commission is not None:
            tracked.commission_bnb += ev.commission  # feeBurn — учитываем отдельно
        if ev.last_filled_qty:
            tracked.filled_qty += ev.last_filled_qty
            fill = Fill(
                order_client_id=ev.client_order_id,
                exchange_order_id=ev.exchange_order_id,
                trade_id=0, ts_ms=ev.ts_ms or self._now_ms(),
                price=ev.avg_price or Decimal("0"), qty=ev.last_filled_qty,
                commission=ev.commission or Decimal("0"),
                commission_asset=ev.commission_asset or "USDT",
            )
            self._storage.insert_fill(
                tracked.row_id, f"ws-{ev.ts_ms}", fill.ts_ms,
                fill.price, fill.qty, fill.commission, fill.commission_asset,
            )
        symbol = tracked.symbol
        async with self.lock(symbol):
            pos = self._positions.get(symbol)
            if pos is None:
                await self._on_fill_without_position(tracked, ev)
                return
            if tracked.role == "ENTRY":
                if ev.avg_price:
                    pos.entry_price = ev.avg_price  # точная цена вместо ack
                if (
                    ev.commission is not None
                    and (ev.commission_asset or "USDT") == "USDT"
                ):
                    pos.fees_usdt += ev.commission  # комиссия входа — в PnL сделки
            elif tracked.role in ("SL", "RS"):
                if symbol in self._closing:
                    return  # наша ветка закрытия уже владеет позицией
                await self._close_by_event(
                    pos, ExitReason.SL, ev.avg_price or pos.sl_price,
                    ev.ts_ms or self._now_ms(), tracked,
                )
            elif tracked.role == "TP1":
                await self._on_tp1_filled(pos, tracked, ev)
            elif tracked.role == "TP2":
                if symbol in self._closing:
                    return
                await self._close_by_event(
                    pos, ExitReason.TP2, ev.avg_price or pos.tp2_price or
                    pos.entry_price, ev.ts_ms or self._now_ms(), tracked,
                )
            elif tracked.role in ("FC", "MC"):
                if symbol in self._closing:
                    return  # закрывающая ветка сама завершит книги
                await self._close_by_event(
                    pos, ExitReason.FORCED if tracked.role == "FC"
                    else ExitReason.UNKNOWN_RECONCILE,
                    ev.avg_price or pos.entry_price,
                    ev.ts_ms or self._now_ms(), tracked,
                )

    async def _on_tp1_filled(
        self, pos: ManagedPosition, tracked: TrackedOrder, ev: Any
    ) -> None:
        """TP1 исполнен: частичная фиксация + breakeven (Д8-v2).

        BE ставится немедленно по факту TP1 (событие — истина).
        """
        qty = ev.last_filled_qty or tracked.qty or Decimal("0")
        price = ev.avg_price or pos.entry_price
        direction = Decimal("1") if pos.side is Side.LONG else Decimal("-1")
        pos.realized_partial += (price - pos.entry_price) * qty * direction
        pos.fees_usdt += tracked.commission_usdt
        pos.qty = max(pos.qty - qty, Decimal("0"))
        pos.tp1_done = True

        pos.breakeven_done = True
        be = breakeven_price(pos.side, pos.entry_price, self._settings)
        if improves(pos.side, be, pos.local_sl_price):
            pos.local_sl_price = be

        pos.tp1_client_id = None
        tracked.state = OrderState.FILLED
        self._persist_position(pos, self._now_ms())
        logger.info("engine: TP1 %s qty=%s по %s, BE активирован",
                    pos.symbol, qty, price)

    async def _on_fill_without_position(
        self, tracked: TrackedOrder, ev: Any
    ) -> None:
        """Fill без позиции: entry-таймаут-сценарий или мусор — разбираемся.

        Entry исполнен, но позиция не создана (ответ был потерян и
        resolve не подтвердил): капитал беззащитен — немедленный
        emergency close + инцидент.
        """
        if tracked.role == "ENTRY" and ev.state is OrderState.FILLED:
            self._incident(
                IncidentType.ORDER_TIMEOUT, tracked.symbol, "critical",
                f"entry {tracked.client_order_id} исполнен без позиции — "
                f"аварийное закрытие qty={ev.last_filled_qty}",
            )
            # сторона выхода = противоположна входу (SHORT-баг, найден аудитом Ч.5)
            exit_side = (
                Side.LONG if tracked.side is OrderSide.BUY else Side.SHORT
            ).order_side_exit
            close_req = OrderRequest(
                client_order_id=tracked.client_order_id + "-fc",
                symbol=tracked.symbol, side=exit_side,
                kind=OrderKind.MARKET,
                qty=ev.last_filled_qty or tracked.qty,
                reduce_only=True,
            )
            # [ИСПРАВЛЕНО] вызов был утерян при применении Е24: ордер
            # создавался, но НЕ отправлялся — аварийная ветка не закрывала
            # позицию. Поймано ruff (F841), в REPORT.
            await self._venue.execute_order(close_req)
        else:
            self._incident(
                IncidentType.UNKNOWN_ORDER_STATUS, tracked.symbol, "warning",
                f"fill {tracked.role} {tracked.client_order_id} без позиции",
            )

    # ----------------------------------------------------------------
    # Защита: Часть A/B (§11)
    # ----------------------------------------------------------------

    async def _part_a(self, symbol: str, pos: ManagedPosition, now: int) -> None:
        """Часть A (§11): SL подтверждён активным? Нет -> Часть B.

        Два независимых источника вердикта:
        - событийный: tracked.state из книги (CANCELED/FILLED/REJECTED
          подтверждены событием биржи);
        - REST-контроль (не чаще sl_rest_check_interval_sec): успешный
          openOrders АВТОРИТЕТЕН — SL cid отсутствует = SL не активен,
          даже если книга ещё считает его NEW (событие могло не долететь;
          «исполнился или пропал» различает Часть B).
        Ошибка REST -> вердикт по книге (аварию из-за сети не поднимаем).
        """
        sl_cid = pos.sl_client_id
        if sl_cid is None:
            await self._part_b(symbol, pos, now)
            return
        tracked = self._orders.get(sl_cid)
        missing_by_events = tracked is None or tracked.state in (
            OrderState.CANCELED, OrderState.FILLED, OrderState.REJECTED,
        )
        last = self._last_rest_check.get(symbol, 0)
        if (now - last) >= int(self._settings.sl_rest_check_interval_sec * 1000):
            try:
                acks = await self._venue.open_orders(symbol)
            except Exception as exc:
                logger.warning("engine: REST-контроль %s не удался: %s", symbol, exc)
                acks = None
            if acks is not None:
                self._last_rest_check[symbol] = now
                by_cid = {a.client_order_id: a for a in acks}
                for cid in (pos.tp1_client_id, pos.tp2_client_id):
                    if cid and cid in by_cid:
                        t = self._orders.get(cid)
                        if t and t.state is not by_cid[cid].status:
                            t.state = by_cid[cid].status
                            self._storage.update_order_status(t.row_id, t.state.value)
                sl_ack = by_cid.get(sl_cid)
                if sl_ack is not None:
                    if tracked is not None and tracked.state is not sl_ack.status:
                        tracked.state = sl_ack.status
                        self._storage.update_order_status(
                            tracked.row_id, sl_ack.status.value
                        )
                    if sl_ack.status in (OrderState.NEW, OrderState.PARTIALLY_FILLED):
                        return  # SL подтверждён биржей — штатно
                await self._part_b(symbol, pos, now)  # SL нет в openOrders
                return
        if missing_by_events:
            await self._part_b(symbol, pos, now)

    async def _part_b(self, symbol: str, pos: ManagedPosition, now: int) -> None:
        """Часть B: SL не подтверждён — восстановить или закрыть (§11).

        Ветки: (a) SL на бирже есть -> кэш устарел, синхронизируем;
        (b) позиции нет -> сверка закроет книги по фактам;
        (c) SL нет, позиция есть -> восстановление, при провале — FORCED.
        """
        if not pos.unprotected:
            pos.unprotected = True
            self._persist_position(pos, now)
            self._incident(IncidentType.UNPROTECTED, symbol, "critical",
                           "SL не подтверждён активным")
        try:
            acks = await self._venue.open_orders(symbol)
        except Exception as exc:
            logger.error("engine: Часть B: openOrders упал: %s", exc)
            return
        sl_ack = next(
            (a for a in acks if a.client_order_id == pos.sl_client_id), None
        )
        if sl_ack is not None and pos.sl_client_id:
            # (a) кэш устарел
            tracked = self._orders.get(pos.sl_client_id)
            if tracked:
                tracked.state = sl_ack.status
                self._storage.update_order_status(tracked.row_id, sl_ack.status.value)
            pos.unprotected = False
            self._persist_position(pos, now)
            self._incident(IncidentType.SL_RESTORED, symbol, "warning",
                           "рассинхрон кэша SL устранён из openOrders")
            return
        ex_positions = {p.symbol: p for p in await self._venue.positions()}
        ex_qty = ex_positions.get(symbol)
        if ex_qty is None:
            # (b) позиции уже нет — события/сверка закроют книги
            await self._reconciler.full_symbol(symbol)
            return
        # (c) восстановление SL
        filters = self._filters.get(symbol)
        if filters is None:
            logger.error("engine: Часть B: нет фильтров %s — форс-закрытие", symbol)
            await self._close_position_locked(symbol, ExitReason.FORCED,
                                              "no_filters_for_restore")
            return
        restore_cid = make_client_id(pos.signal_id, "rs")
        req = OrderRequest(
            client_order_id=restore_cid, symbol=symbol,
            side=pos.side.order_side_exit, kind=OrderKind.STOP_MARKET,
            stop_price=pos.sl_price, close_position=True, price_protect=True,
            signal_id=pos.signal_id,
        )
        result = await restore_stop_market(
            self._venue, req, self._settings.sl_restore_attempts,
            self._settings.sl_restore_interval_sec,
        )
        if result.ok and result.ack is not None:
            self._track_new_order(req, "RS", result.ack, pos.entry_client_id)
            pos.sl_client_id = restore_cid
            pos.unprotected = False
            self._persist_position(pos, self._now_ms())
            self._incident(IncidentType.SL_RESTORED, symbol, "warning",
                           f"SL восстановлен ({result.attempts} попыток)")
            return
        # провал восстановления -> форс-закрытие (приоритет капитала)
        self._incident(
            IncidentType.FORCE_CLOSE, symbol, "critical",
            f"восстановление SL провалилось: {result.detail}",
        )
        await self._close_position_locked(symbol, ExitReason.FORCED,
                                          "sl_restore_failed")

    # ----------------------------------------------------------------
    # Закрытие позиции
    # ----------------------------------------------------------------

    async def _close_position_locked(
        self, symbol: str, reason: ExitReason, detail: str
    ) -> None:
        """Программное закрытие: отменить ордера -> market -> книги.

        Требует УДЕРЖАННОГО лока символа (инвариант вызова).
        """
        pos = self._positions.get(symbol)
        if pos is None or symbol in self._closing:
            return
        self._closing.add(symbol)
        try:
            # 1) отменить биржевые SL/TP; -2011 = исполнен -> событие закроет
            for cid in (pos.tp1_client_id, pos.tp2_client_id, pos.sl_client_id):
                if cid is None:
                    continue
                try:
                    await self._venue.cancel_order(symbol, cid)
                    tracked = self._orders.get(cid)
                    if tracked:
                        tracked.state = OrderState.CANCELED
                        self._storage.update_order_status(
                            tracked.row_id, OrderState.CANCELED.value
                        )
                except UnknownOrderError:
                    # ордер успел исполниться: выход зафиксирует событие
                    logger.info("engine: %s: %s уже исполнен (гонка cancel/fill)",
                                symbol, cid)
                except Exception as exc:
                    logger.error("engine: отмена %s: %s", cid, exc)
            # 2) market reduceOnly на остаток
            if pos.qty > 0:
                fc_cid = make_client_id(pos.signal_id, "fc")
                req = OrderRequest(
                    client_order_id=fc_cid, symbol=symbol,
                    side=pos.side.order_side_exit, kind=OrderKind.MARKET,
                    qty=pos.qty, reduce_only=True, signal_id=pos.signal_id,
                )
                ack = await self._venue.execute_order(req)
                role = "FC" if reason is ExitReason.FORCED else "MC"
                self._track_new_order(req, role, ack, pos.entry_client_id)
                if ack.status is OrderState.FILLED and ack.avg_price:
                    await self._close_by_event(
                        pos, reason, ack.avg_price, self._now_ms(), None
                    )
                    return
                if ack.status is OrderState.REJECTED:
                    # позиция уже закрыта извне — сверка вырулит
                    logger.warning(
                        "engine: market-close %s отклонён — точечная сверка", symbol
                    )
                    await self._reconciler.full_symbol(symbol)
                    return
                logger.error("engine: market-close %s статус %s — сверка",
                             symbol, ack.status.value)
                await self._reconciler.full_symbol(symbol)
                return
            # остатка нет — фиксируем по последней известной цене
            price = self._prices.get(symbol) or pos.entry_price
            await self._close_by_event(pos, reason, price, self._now_ms(), None)
        finally:
            self._closing.discard(symbol)

    async def _close_by_event(
        self, pos: ManagedPosition, reason: ExitReason, exit_price: Decimal,
        exit_ts: int, tracked: TrackedOrder | None,
    ) -> None:
        """Фиксация закрытия в книгах (по факту исполнения — истина).

        Требует удержанного лока. qty сделки = исполненный объём входа
        (частичные TP не теряются). Кулдауны/лимиты — 1:1 (Е7).
        """
        symbol = pos.symbol
        if symbol not in self._positions:
            return  # уже закрыта параллельной веткой (гонка разрешена)
        qty_closed = pos.qty
        direction = Decimal("1") if pos.side is Side.LONG else Decimal("-1")
        final_pnl = (exit_price - pos.entry_price) * qty_closed * direction
        gross = pos.realized_partial + final_pnl
        if tracked is not None:
            pos.fees_usdt += tracked.commission_usdt
        fees = pos.fees_usdt
        net = gross - fees
        size_f = float(pos.size_usdt)
        pnl_pct = float(net) / size_f * 100.0 if size_f else 0.0
        mfe = float(profit_pct(pos, pos.mfe_price)) if pos.mfe_price else 0.0
        mae = float(profit_pct(pos, pos.mae_price)) if pos.mae_price else 0.0
        entry_tracked = self._orders.get(pos.entry_client_id)
        rec_qty = (
            entry_tracked.filled_qty
            if entry_tracked and entry_tracked.filled_qty > 0
            else qty_closed
        )
        rec = TradeRecord(
            signal_id=pos.signal_id, symbol=symbol, side=pos.side.value,
            entry_ts=pos.entry_ts_ms, exit_ts=exit_ts,
            entry_price=pos.entry_price, exit_price=exit_price, qty=rec_qty,
            gross_pnl=gross, fees=fees, net_pnl=net, pnl_pct=pnl_pct,
            exit_reason=reason.value, mfe=mfe, mae=mae,
            sl_pct=pos.sl_pct, tp_pct=pos.tp_pct,
            tp1_done=pos.tp1_done, tp2_done=reason is ExitReason.TP2,
            breakeven_done=pos.breakeven_done,
        )
        self._storage.insert_trade(rec)
        self._storage.delete_position(symbol)
        del self._positions[symbol]
        state = self._gate_state
        state.open_symbols.discard(symbol)
        now = self._now_ms()
        if reason is ExitReason.SL:
            until = now + int(self._settings.sl_cooldown_sec * 1000)
            state.cooldown_until_ms[symbol] = max(
                state.cooldown_until_ms.get(symbol, 0), until
            )
            history = state.stop_history_ms.setdefault(symbol, [])
            history.append(now)
            recent = [t for t in history if t > now - 86_400_000]
            if len(recent) >= self._settings.repeat_stop_limit:
                state.repeat_block_until_ms[symbol] = max(
                    state.repeat_block_until_ms.get(symbol, 0),
                    now + int(self._settings.repeat_block_sec * 1000),
                )
        elif reason is ExitReason.GAP_SL:  # ветка спит (паритет, §0-7)
            state.gap_block_until_ms[symbol] = max(
                state.gap_block_until_ms.get(symbol, 0),
                now + int(self._settings.gap_block_sec * 1000),
            )
        if reason is ExitReason.TP2:
            state.cooldown_until_ms.pop(symbol, None)
            state.stop_history_ms.pop(symbol, None)
            state.repeat_block_until_ms.pop(symbol, None)
        if abs(net) < Decimal("0.005"):
            self._breakevens += 1  # паритет: старый считал pnl==0 события
        state.today_trades += 1
        state.today_realized += net
        self._realized_total += net
        self._write_equity("close", symbol, now)
        logger.info(
            "engine: закрыто %s reason=%s exit=%s net=%s pnl%%=%.2f",
            symbol, reason.value, exit_price, net, pnl_pct,
        )

    # ----------------------------------------------------------------
    # Вспомогательное
    # ----------------------------------------------------------------

    def _iron_price(
        self, sl_price: Decimal, side: Side, filters: SymbolFilters
    ) -> Decimal:
        """iron = SL хуже на IRON_SL_OFFSET_PCT, округление в «худшую» сторону."""
        offset = Decimal(str(self._settings.iron_sl_offset_pct)) / Decimal("100")
        if side is Side.LONG:
            raw = sl_price * (Decimal("1") - offset)
        else:
            raw = sl_price * (Decimal("1") + offset)
        return round_price_tick(raw, side, filters.tick_size)

    async def _place_protection(
        self, pos: ManagedPosition, filters: SymbolFilters, sid: int,
        executed_qty: Decimal, now: int
    ) -> bool:
        """SL (closePosition) + TP1/TP2 (reduceOnly) на executedQty (§5/§6).

        Returns:
            True если SL стоит; TP-провалы не критичны (SL защищает).
        """
        sl_cid = make_client_id(sid, "sl")
        sl_req = OrderRequest(
            client_order_id=sl_cid, symbol=pos.symbol,
            side=pos.side.order_side_exit, kind=OrderKind.STOP_MARKET,
            stop_price=pos.sl_price, close_position=True, price_protect=True,
            signal_id=sid, position_ref=pos.entry_client_id,
        )
        sl_ack = await self._venue.execute_order(sl_req)
        # -1013 (устаревший кэш фильтров): точечный refresh + ОДИН повтор
        # тем же request (§3 черновика). Иначе Part B реставрирует SL
        # по тем же устаревшим фильтрам и тоже промахнётся.
        if (
            sl_ack.status is OrderState.REJECTED
            and str(sl_ack.raw.get("code")) == "-1013"
        ):
            await self._filters.refresh_symbol(pos.symbol)
            sl_ack = await self._venue.execute_order(sl_req)
        self._track_new_order(sl_req, "SL", sl_ack,
                              pos.entry_client_id)
        if sl_ack.status not in (OrderState.NEW, OrderState.PARTIALLY_FILLED):
            self._incident(IncidentType.SL_LOST, pos.symbol, "critical",
                            f"SL не встал: {dict(sl_ack.raw)}")
            await self._part_b(pos.symbol, pos, now)
            return not pos.unprotected
        pos.sl_client_id = sl_cid
        # TP1/TP2: динамическая доля 1:1 со старым tracker.open_position:
        # меньшая часть (size×min(frac,1−frac)) обязана быть >= minNotional×1.05;
        # иначе — упрощённая стратегия без TP1 (tp1_skip).
        size = self._settings.position_size_usdt
        min_req = filters.min_notional * Decimal(
            str(self._settings.min_notional_safety)
        )
        share = Decimal(str(self._settings.tp1_share))
        smaller = size * min(share, Decimal("1") - share)
        full_strategy = smaller >= min_req
        frac = Decimal("0")
        if full_strategy:
            max_frac = Decimal("1") - min_req / size
            frac = max(Decimal("0.1"), min(share, max_frac))
        else:
            pos.tp1_done = True
            self._incident(
                IncidentType.TP1_SKIP, pos.symbol, "info",
                f"малый размер: TP1 пропущен (меньшая часть < {min_req} USDT)",
            )
        tp1_qty = floor_to_step(executed_qty * frac, filters.step_size)
        split_ok = (
            full_strategy
            and tp1_qty >= filters.min_qty
            and executed_qty - tp1_qty >= filters.min_qty
        )
        if full_strategy and not split_ok:
            frac = Decimal("0")
            tp1_qty = Decimal("0")
            pos.tp1_done = True
            self._incident(
                IncidentType.TP1_SKIP, pos.symbol, "warning",
                "floor по stepSize сделал части < minQty — TP1 пропущен",
            )
        if split_ok:
            tp1_cid = make_client_id(sid, "tp1")
            tp1_req = OrderRequest(
                client_order_id=tp1_cid, symbol=pos.symbol,
                side=pos.side.order_side_exit,
                kind=OrderKind.TAKE_PROFIT_MARKET,
                stop_price=pos.tp1_price, qty=tp1_qty, reduce_only=True,
                price_protect=True, signal_id=sid,
            )
            tp1_ack = await self._venue.execute_order(tp1_req)
            self._track_new_order(tp1_req, "TP1", tp1_ack, pos.entry_client_id)
            if tp1_ack.status is OrderState.NEW:
                pos.tp1_client_id = tp1_cid
            else:
                self._incident(IncidentType.TP1_SKIP, pos.symbol, "warning",
                               f"TP1 не встал: {dict(tp1_ack.raw)}")
        else:
            self._incident(IncidentType.TP1_SKIP, pos.symbol, "info",
                           "сплит TP1/TP2 ниже минимумов — TP2 на весь объём")
        tp2_qty = executed_qty - tp1_qty if (split_ok and pos.tp1_client_id) \
            else executed_qty
        tp2_cid = make_client_id(sid, "tp2")
        tp2_req = OrderRequest(
            client_order_id=tp2_cid, symbol=pos.symbol,
            side=pos.side.order_side_exit, kind=OrderKind.TAKE_PROFIT_MARKET,
            stop_price=pos.tp2_price, qty=tp2_qty, reduce_only=True,
            price_protect=True, signal_id=sid,
        )
        tp2_ack = await self._venue.execute_order(tp2_req)
        self._track_new_order(tp2_req, "TP2", tp2_ack, pos.entry_client_id)
        if tp2_ack.status is OrderState.NEW:
            pos.tp2_client_id = tp2_cid
        else:
            self._incident(IncidentType.SL_LOST, pos.symbol, "warning",
                           f"TP2 не встал: {dict(tp2_ack.raw)} — SL защищает")
        return True

    async def _handle_entry_failure(
        self, intent: EntryIntent, sid: int, ack: Any
    ) -> None:
        """Вход не подтверждён: reject записи + защита от «призрака»."""
        self._storage.insert_signal(
            self._now_ms(), self._mode, intent.symbol, intent.side.value,
            intent.score, intent.confidence, "rejected",
            RejectReason.ORDER_FAILED.value, {"ack": dict(ack.raw)},
        )
        self._incident(IncidentType.ORDER_TIMEOUT, intent.symbol, "critical",
                       f"entry не подтверждён: {ack.status.value}")
        # позиция не создана; если ордер окажется исполненным — событие
        # _on_fill_without_position закроет его аварийно

    def _track_new_order(
        self, request: OrderRequest, role: str, ack: OrderAck,
        position_ref: str,
    ) -> TrackedOrder:
        """Зарегистрировать ордер: БД + книга; side/qty — из request
        (единый источник параметров, анти-П1)."""
        row_id = self._storage.insert_order(OrderRow(
            ts_ms=self._now_ms(), mode=self._mode, symbol=request.symbol,
            client_order_id=request.client_order_id,
            exchange_order_id=ack.exchange_order_id,
            side=request.side.value, type=request.kind.value, role=role,
            qty=request.qty, stop_price=request.stop_price,
            reduce_only=request.reduce_only,
            close_position=request.close_position,
            status=ack.status.value, position_ref=position_ref,
            raw_response=None,
        ))
        tracked = TrackedOrder(
            client_order_id=request.client_order_id, role=role,
            symbol=request.symbol, side=request.side,
            state=ack.status, row_id=row_id, qty=request.qty,
            exchange_order_id=ack.exchange_order_id,
        )
        self._orders[request.client_order_id] = tracked
        return tracked

    def _track_update_order(
        self, request: OrderRequest, role: str, ack: OrderAck,
        position_ref: str,
    ) -> None:
        """Обновить статус существующего ордера в книге/БД после resolve."""
        tracked = self._orders.get(request.client_order_id)
        if tracked is None:
            self._track_new_order(request, role, ack, position_ref)
            return
        if tracked.exchange_order_id is None and ack.exchange_order_id is not None:
            tracked.exchange_order_id = ack.exchange_order_id
        if ack.executed_qty is not None:
            tracked.filled_qty = ack.executed_qty
        if ack.avg_price is not None:
            tracked.avg_price = ack.avg_price
        self._storage.update_order_status(
            tracked.row_id, ack.status.value, tracked.exchange_order_id,
        )

    def _persist_position(self, pos: ManagedPosition, now: int) -> None:
        """Снапшот в БД (dirty-check внутри storage — минимум записи)."""
        pos.updated_ms = now
        self._storage.upsert_position(pos.to_stored(now))

    def _write_equity(self, reason: str, symbol: str | None, now: int) -> None:
        """equity по событию: open/close или ΔE≥порога (Б2-3а)."""
        unrealized = Decimal("0")
        for pos in self._positions.values():
            price = self._prices.get(pos.symbol)
            if price is None:
                continue
            direction = Decimal("1") if pos.side is Side.LONG else Decimal("-1")
            unrealized += (price - pos.entry_price) * pos.qty * direction
            unrealized += pos.realized_partial
        total = self._capital_base + self._realized_total + unrealized
        self._storage.insert_equity(
            now, self._mode, self._capital_base, self._realized_total,
            unrealized, total, reason, symbol,
        )
        self._last_equity = total
        self._equity_dirty = False

    def _maybe_write_equity_periodic(self, now: int) -> None:
        """Пороговая запись equity (только при Δ≥equity_delta_pct)."""
        unrealized = Decimal("0")
        for pos in self._positions.values():
            price = self._prices.get(pos.symbol)
            if price is None:
                continue
            direction = Decimal("1") if pos.side is Side.LONG else Decimal("-1")
            unrealized += (price - pos.entry_price) * pos.qty * direction
            unrealized += pos.realized_partial
        total = self._capital_base + self._realized_total + unrealized
        if self._last_equity != 0:
            delta = abs(total - self._last_equity) / abs(self._last_equity)
            if delta >= Decimal(str(self._settings.equity_delta_pct)):
                self._write_equity("delta", None, now)

    def _sync_daily_counters(self) -> None:
        """Восстановить дневной лимит из БД.

        Граница суток — локальная полночь (паритет со старым
        datetime.now().date()).
        """
        day_start = self._local_day_start_ms(self._now_ms())
        self._gate_state.day_start_ms = day_start
        self._gate_state.today_trades = self._storage.today_trades_count(day_start)
        self._gate_state.today_realized = self._storage.today_realized(
            self._mode, day_start
        )

    @staticmethod
    def _local_day_start_ms(now_ms: int) -> int:
        """Граница суток в локальном времени (паритет со старым .date()).

        Чистая арифметика, без ОС-конверсий: и time.mktime, и наивный
        datetime.timestamp() на Windows падают на pre-epoch (локальная
        полночь ранних timestamp — отрицательное время). Смещение локали
        берём разницей wall-clock (fromtimestamp - fromtimestamp(UTC)):
        обе точки post-epoch и в проде (реальное время), и в тестах.
        Украина с 2024 без перехода DST (постоянный сдвиг), так что сдвиг
        полуночи совпадает со сдвигом now; на DST-календарях возможна
        погрешность 1 ч в два дня года (в проде неактуально).
        """
        seconds = now_ms / 1000
        local_wall = datetime.fromtimestamp(seconds)
        utc_wall = datetime.fromtimestamp(seconds, tz=timezone.utc).replace(
            tzinfo=None
        )
        offset_s = int((local_wall - utc_wall).total_seconds())
        midnight = local_wall.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        days = midnight.toordinal() - 719_163  # 1970-01-01 .toordinal()
        return (days * 86_400 - offset_s) * 1000

    def _roll_daily(self, now: int) -> None:
        """Сброс дневных счётчиков при смене суток (вызывается монитором)."""
        day = self._local_day_start_ms(now)
        if day > self._gate_state.day_start_ms:
            self._gate_state.day_start_ms = day
            self._gate_state.today_trades = 0
            self._gate_state.today_realized = Decimal("0")

    async def _safe_balance(self) -> Decimal | None:
        """Баланс venue без падения движка (ошибка -> None, гейт пропустит)."""
        try:
            return await self._venue.available_balance()
        except Exception as exc:
            logger.warning("engine: баланс недоступен: %s", exc)
            return None

    async def _vol_ratio_if_due(
        self, symbol: str, pos: ManagedPosition, now: int
    ) -> float | None:
        """Отношение recent/prior объёмов для VOL_DECAY.

        [ИСПРАВЛЕНО] старый код брал k[1] (open-цену) вместо k[5] (объём) —
        ветка была мертва; здесь объём корректный (REPORT). Троттлинг
        60 с/символ (старый код дёргал klines каждые 2 с — тарифная
        нагрузка без изменения решений, задокументировано).
        """
        if self._volume_provider is None:
            return None
        hold_min = (now - pos.entry_ts_ms) / 60_000
        if hold_min < self._settings.vol_decay_after_min:
            return None
        if now - self._vol_last_fetch.get(symbol, 0) < 60_000:
            return None
        w = self._settings.vol_decay_window_min
        p = self._settings.vol_decay_prior_min
        try:
            klines = await self._volume_provider(symbol, "1m", w + p + 1)
        except Exception as exc:
            logger.warning("engine: klines для VOL_DECAY %s: %s", symbol, exc)
            return None
        self._vol_last_fetch[symbol] = now
        if not klines or len(klines) < w + p + 1:
            return None
        try:
            volumes = [float(k[5]) for k in klines]
        except (TypeError, ValueError, IndexError):
            return None
        prior = volumes[-(w + p):-w]
        if not prior or sum(prior) <= 0:
            return None
        recent_avg = sum(volumes[-w:]) / w
        prior_avg = sum(prior) / len(prior)
        return recent_avg / prior_avg

    def _log_reject(
        self, intent: EntryIntent, reason: RejectReason, detail: str, now: int
    ) -> None:
        """Реджект: запись в signals с точной причиной (статистика §3)."""
        self._storage.insert_signal(
            now, self._mode, intent.symbol, intent.side.value, intent.score,
            intent.confidence, "rejected", reason.value,
            {"detail": detail, "btc_trend": intent.btc_trend},
        )
        logger.info("engine: реджект %s: %s (%s)", intent.symbol, reason.value,
                    detail)

    def _incident(
        self, type_: IncidentType, symbol: str | None, severity: str, details: str
    ) -> None:
        """Инцидент: incidents + алерт (никаких тихих потерь)."""
        self._storage.insert_incident(
            self._now_ms(), self._mode, type_.value, symbol, severity, details
        )
        self._notifier.alert(severity, type_.value, details)

    # ----------------------------------------------------------------
    # Реconciliation-колбэки (EnginePort)
    # ----------------------------------------------------------------

    async def adopt_exchange_position(
        self, symbol: str, ex: ExchangePosition, sl_client_id: str | None,
        tp1_client_id: str | None, tp2_client_id: str | None,
    ) -> None:
        """Восстановить позицию из данных биржи (биржа приоритетна, §12)."""
        local = self._positions.get(symbol)
        side = Side.LONG if ex.side == "LONG" else Side.SHORT
        if local is None:
            stored = next(
                (p for p in self._storage.load_positions() if p.symbol == symbol),
                None,
            )
            local = ManagedPosition(
                symbol=symbol, side=side,
                signal_id=stored.signal_id if stored else 0,
                entry_ts_ms=stored.entry_ts if stored else self._now_ms(),
                entry_price=ex.entry_price, qty=ex.qty,
                size_usdt=stored.size_usdt if stored else ex.qty * ex.entry_price,
                score=stored.score if stored else 0.0,
                sl_price=stored.sl_price if stored else ex.entry_price,
                local_sl_price=stored.sl_price if stored else ex.entry_price,
                tp1_price=stored.tp1_price if stored else None,
                tp2_price=stored.tp2_price if stored else None,
                iron_sl_price=stored.iron_sl_price if stored and stored.iron_sl_price
                else ex.entry_price,
                sl_client_id=sl_client_id, tp1_client_id=tp1_client_id,
                tp2_client_id=tp2_client_id, entry_client_id="",
            )
            self._positions[symbol] = local
            self._gate_state.open_symbols.add(symbol)
            self._incident(IncidentType.RECON_MISMATCH, symbol, "warning",
                           f"позиция восстановлена из биржи qty={ex.qty}")
        else:
            # биржа приоритетна по цифрам
            local.qty = ex.qty
            local.entry_price = ex.entry_price
            if sl_client_id:
                local.sl_client_id = sl_client_id
        self._persist_position(local, self._now_ms())

    async def close_dead_position(
        self, stored: StoredPosition, exit_price: Decimal, exit_ts_ms: int,
        reason: ExitReason, gross: Decimal, fees: Decimal, source: str,
        entry_qty: Decimal | None = None,
    ) -> None:
        """Dead-close (§12): позиция закрылась, пока бот был мёртв.

        Runtime-позиции нет — TradeRecord строится из снапшота БД.
        gross/fees посчитаны Reconciler'ом из фактических fills
        (userTrades), pnl_pct — от size_usdt. MFE/MAE неизвестны (0.0):
        runtime-статистика не переживает смерть процесса — осознанное
        ограничение (REPORT, Часть 5).

        Инвариант: вызывается под удержанным локом символа (стартовая
        сверка и full_symbol берут лок до вызова). Гейт-состояние
        обновляется как при штатном закрытии (дневные лимиты, кулдауны);
        при старте _sync_daily_counters затем перечитает totals из БД —
        двойного счёта нет (присваивание, не инкремент).
        """
        symbol = stored.symbol
        local = self._positions.get(symbol)
        if local is not None:
            # позиция успела появиться в runtime (гонка adopt) — штатный путь
            await self._close_by_event(local, reason, exit_price, exit_ts_ms, None)
            return
        qty = entry_qty if entry_qty is not None else stored.qty
        net = gross - fees
        size_f = float(stored.size_usdt)
        pnl_pct = float(net) / size_f * 100.0 if size_f else 0.0
        sl_pct = float(
            abs(stored.entry_price - stored.sl_price)
            / stored.entry_price * Decimal("100")
        )
        tp_pct = 0.0
        if stored.tp2_price is not None:
            tp_pct = float(
                abs(stored.tp2_price - stored.entry_price)
                / stored.entry_price * Decimal("100")
            )
        rec = TradeRecord(
            signal_id=stored.signal_id, symbol=symbol, side=stored.side,
            entry_ts=stored.entry_ts, exit_ts=exit_ts_ms,
            entry_price=stored.entry_price, exit_price=exit_price,
            qty=qty, gross_pnl=gross, fees=fees, net_pnl=net,
            pnl_pct=pnl_pct, exit_reason=reason.value,
            mfe=0.0, mae=0.0, sl_pct=sl_pct, tp_pct=tp_pct,
            tp1_done=stored.tp1_done, tp2_done=stored.tp2_done,
            breakeven_done=stored.breakeven_done,
        )
        self._storage.insert_trade(rec)
        self._storage.delete_position(symbol)

        state = self._gate_state
        now = self._now_ms()
        state = self._gate_state
        if reason is ExitReason.SL:
            until = now + int(self._settings.sl_cooldown_sec * 1000)
            state.cooldown_until_ms[symbol] = max(
                state.cooldown_until_ms.get(symbol, 0), until
            )
            history = state.stop_history_ms.setdefault(symbol, [])
            history.append(now)
            recent = [t for t in history if t > now - 86_400_000]
            if len(recent) >= self._settings.repeat_stop_limit:
                state.repeat_block_until_ms[symbol] = max(
                    state.repeat_block_until_ms.get(symbol, 0),
                    now + int(self._settings.repeat_block_sec * 1000),
                )
        elif reason is ExitReason.GAP_SL:  # ветка спит (как в старом коде, §0-7)
            state.gap_block_until_ms[symbol] = max(
                state.gap_block_until_ms.get(symbol, 0),
                now + int(self._settings.gap_block_sec * 1000),
            )
        if reason is ExitReason.TP2:
            state.cooldown_until_ms.pop(symbol, None)
            state.stop_history_ms.pop(symbol, None)
            state.repeat_block_until_ms.pop(symbol, None)
        if abs(net) < Decimal("0.005"):
            self._breakevens += 1  # паритет: старый считал pnl==0 события
        state.today_trades += 1
        self._realized_total += net

        self._write_equity("close", symbol, now)
        logger.info(
            "engine: dead-close %s reason=%s exit=%s net=%s (%s)",
            symbol, reason.value, exit_price, net, source,
        )

    async def emergency_protect(self, symbol: str) -> None:
        """Reconciler-вызов: проверить/восстановить SL позиции (Часть A/B)."""
        pos = self._positions.get(symbol)
        if pos is None:
            return
        async with self.lock(symbol):
            pos = self._positions.get(symbol)
            if pos is not None:
                await self._part_a(symbol, pos, self._now_ms())

    # ---------------- читатели для фасада (Часть 4) ----------------

    def open_positions(self) -> list[dict[str, Any]]:
        """Копии позиций для GUI (без shared-mutable)."""
        out: list[dict[str, Any]] = []
        for pos in self._positions.values():
            price = self._prices.get(pos.symbol) or pos.entry_price
            direction = Decimal("1") if pos.side is Side.LONG else Decimal("-1")
            unreal = (price - pos.entry_price) * pos.qty * direction
            out.append({
                "symbol": pos.symbol, "side": pos.side.value,
                "entry_time": pos.entry_ts_ms / 1000,  # epoch-секунды (контракт GUI)
                "entry_price": float(pos.entry_price),
                "sl_price": float(pos.local_sl_price),
                "tp1_price": float(pos.tp1_price) if pos.tp1_price else None,
                "tp2_price": float(pos.tp2_price) if pos.tp2_price else None,
                "size_usdt": float(pos.size_usdt), "score": pos.score,
                "tp1_done": pos.tp1_done, "tp2_done": pos.tp2_done,
                "qty": float(pos.qty), "unrealized_pnl": float(unreal),
                "breakeven_done": pos.breakeven_done,
            })
        return out

    def realized_total(self) -> Decimal:
        """Σ net_pnl закрытых сделок режима (для GUI total_pnl)."""
        return self._realized_total

    def breakevens(self) -> int:
        """Счётчик BE-закрытий (паритет pm.breakevens, память процесса)."""
        return self._breakevens

    async def reconcile_light(self) -> None:
        """Лёгкая сверка для фасада (совместимость pm.reconcile)."""
        await self._reconciler.light_check()

    def set_capital_base(self, capital: Decimal) -> None:
        """Обновить базу капитала (real: из available_balance)."""
        self._capital_base = capital
