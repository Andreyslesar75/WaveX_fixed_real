"""Сверка состояния: старт (блокирующий), периодическая, точечная.

Источник истины — биржа (решение §7/§12 черновика): БД — локальный
кэш и история. Таблица исходов стартовой сверки — точно по §12
(совпадение/была-нет/есть-без-SL/внешняя/осиротевшие).

Paper-режим: по решению владельца (П16) — только внутренняя сверка
БД↔движок, внешних запросов нет.
"""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Protocol

from .notifier import Notifier
from .storage import Storage, StoredPosition
from .types import ExitReason, Fill, Mode
from .venue import ExchangePosition, ExecutionVenue

logger = logging.getLogger(__name__)

#: Роль последнего закрывающего fill -> exit_reason (dead-close маппинг).
_ROLE_TO_REASON: dict[str, ExitReason] = {
    "SL": ExitReason.SL,
    "RS": ExitReason.SL,
    "TP1": ExitReason.TP1,
    "TP2": ExitReason.TP2,
    "FC": ExitReason.FORCED,
    "MC": ExitReason.UNKNOWN_RECONCILE,  # наш market-close, умер до записи
}


class EnginePort(Protocol):
    """Узкий доступ Reconciler к движку (без цикла импортов)."""

    def lock(self, symbol: str) -> asyncio.Lock: ...
    def local_position(self, symbol: str) -> object | None: ...
    def local_symbols(self) -> list[str]: ...
    def tracked_order_ids(self, symbol: str) -> set[str]: ...
    async def adopt_exchange_position(
        self, symbol: str, ex: ExchangePosition, sl_client_id: str | None,
        tp1_client_id: str | None, tp2_client_id: str | None,
    ) -> None: ...
    async def close_dead_position(
        self, stored: StoredPosition, exit_price: Decimal, exit_ts_ms: int,
        reason: ExitReason, gross: Decimal, fees: Decimal, source: str,
        entry_qty: Decimal | None = None,
    ) -> None: ...
    async def emergency_protect(self, symbol: str) -> None: ...
    async def cancel_order_safe(self, symbol: str, client_order_id: str) -> None: ...


class Reconciler:
    """Реализация всех трёх видов сверки."""

    def __init__(
        self, venue: ExecutionVenue, storage: Storage, engine: EnginePort,
        mode: Mode, notifier: Notifier | None = None,
    ) -> None:
        self._venue = venue
        self._storage = storage
        self._engine = engine
        self._mode = mode
        self._notifier = notifier

    # ---------------- стартовая (блокирующая) ----------------

    async def startup(self) -> None:
        """Полная сверка до разрешения входов (§12).

        Исключения не глотаются: неудачная сверка = движок не готов.
        """
        from .paper.venue import PaperVenue  # осознанный isinstance (П16)

        if isinstance(self._venue, PaperVenue):
            logger.info("[RECON] Paper-режим — сверка только локальная")
            # PaperVenue живёт в памяти: после рестарта «биржи» нет, и
            # позиция из БД невосстановима. Снапшот удаляем (runtime и БД
            # снова согласованы), инцидент для разбора; в журнал trades НЕ
            # пишем — сделка не была закрыта, фейковый PnL исказил бы
            # статистику. Осознанное paper-ограничение (REPORT, Часть 5).
            for stored in self._storage.load_positions():
                self._incident(
                    "recon_mismatch", stored.symbol, "warning",
                    "paper-рестарт: PaperVenue в памяти, позиция не "
                    "восстановима; снапшот удалён",
                )
                self._storage.delete_position(stored.symbol)
            return
        db_positions = self._storage.load_positions()
        ex_positions = {p.symbol: p for p in await self._venue.positions()}
        db_symbols = {p.symbol for p in db_positions}
        # 1) совпадающие: биржа приоритетна по цифрам; SL-здоровье после
        for symbol in db_symbols & set(ex_positions):
            async with self._engine.lock(symbol):
                stored = next(p for p in db_positions if p.symbol == symbol)
                await self._engine.adopt_exchange_position(
                    symbol, ex_positions[symbol],
                    stored.sl_client_id, stored.tp1_client_id,
                    stored.tp2_client_id,
                )
            await self._engine.emergency_protect(symbol)  # SL есть? нет -> Часть B
        # 2) была в БД, нет на бирже: закрылась, пока мы были мертвы
        for symbol in db_symbols - set(ex_positions):
            await self._resolve_dead_close(symbol)
        # 3) есть на бирже, нет в БД: внешняя — не трогаем, алерт
        for symbol in set(ex_positions) - db_symbols:
            self._incident(
                "external_position", symbol, "critical",
                f"позиция на бирже qty={ex_positions[symbol].qty}, в БД нет",
            )
        # 4) осиротевшие ордера без позиции
        for symbol in set(ex_positions) | db_symbols:
            await self._cancel_orphans(symbol)
        logger.info("[RECON] стартовая сверка завершена")

    async def _resolve_dead_close(self, symbol: str) -> None:
        """Позиция была в БД, на бирже её нет: закрылась, пока бот был мёртв.

        Алгоритм (§12 «дозаписать журнал»): userTrades с entry_ts ->
        классификация fills по роли ордера (ENTRY — вход, остальные —
        закрытия; не наш ордер -> внешний) -> gross/fees из фактических
        сделок -> exit_reason по роли ПОСЛЕДНЕГО закрывающего fill ->
        engine.close_dead_position (журнал дозаписывается).

        Ограничение: BNB-комиссии (feeBurn) в fees не суммируются,
        только USDT — фиксируется в REPORT (Часть 5).

        Инвариант: вызывается под удержанным локом символа.
        """
        stored = next(
            (p for p in self._storage.load_positions() if p.symbol == symbol),
            None,
        )
        if stored is None:
            return
        fills = await self._venue.user_trades(symbol, stored.entry_ts)
        if not fills:
            self._incident(
                "recon_mismatch", symbol, "warning",
                "БД говорит открыта, сделок на бирже нет — дозапись с нулевым PnL",
            )
            await self._engine.close_dead_position(
                stored, stored.entry_price, stored.entry_ts,
                ExitReason.UNKNOWN_RECONCILE, Decimal("0"), Decimal("0"),
                "no_trades_found",
            )
            return
        direction = Decimal("1") if stored.side == "LONG" else Decimal("-1")
        entry_price = stored.entry_price
        gross = Decimal("0")
        fees_usdt = Decimal("0")
        entry_qty_sum = Decimal("0")
        last_closing: Fill | None = None
        last_role: str | None = None
        for fill in sorted(fills, key=lambda f: f.ts_ms):
            role = (
                self._storage.find_order_role_by_exchange_id(
                    self._mode, fill.exchange_order_id
                )
                if fill.exchange_order_id is not None
                else None
            )
            if fill.commission_asset == "USDT":
                fees_usdt += fill.commission
            if role == "ENTRY":
                entry_qty_sum += fill.qty
                continue
            gross += (fill.price - entry_price) * fill.qty * direction
            last_closing = fill
            last_role = role
        if last_closing is None:
            # только входные fills — закрытия не было (мусор/partial)
            self._incident(
                "recon_mismatch", symbol, "warning",
                "только входные fills — закрытия не найдено",
            )
            await self._engine.close_dead_position(
                stored, stored.entry_price, stored.entry_ts,
                ExitReason.UNKNOWN_RECONCILE, Decimal("0"), fees_usdt,
                "only_entry_fills",
            )
            return
        reason = _ROLE_TO_REASON.get(last_role or "", None)
        if reason is None:
            # Реальный ордер исполнения algo может нести биржевый id
            # (actualOrderId) — роль по exchange_order_id не находится.
            # Фолбэк: классификация по цене закрытия против уровней
            # снапшота (допуск 0.5%); иначе честный EXTERNAL_CLOSE.
            entry = stored.entry_price
            tol = abs(entry) * Decimal("0.005")
            exit_p = last_closing.price
            if stored.sl_price is not None and abs(exit_p - stored.sl_price) <= tol:
                reason = ExitReason.SL
            elif stored.tp1_price is not None and abs(exit_p - stored.tp1_price) <= tol:
                reason = ExitReason.TP1
            elif stored.tp2_price is not None and abs(exit_p - stored.tp2_price) <= tol:
                reason = ExitReason.TP2
            else:
                reason = ExitReason.EXTERNAL_CLOSE
            logger.info(
                "[RECON] dead-close %s: роль не найдена, классификация по цене -> %s",
                symbol, reason.value,
            )

        await self._engine.close_dead_position(
            stored, last_closing.price, last_closing.ts_ms,
            reason, gross, fees_usdt, "dead_close",
            entry_qty=entry_qty_sum if entry_qty_sum > 0 else None,
        )

    async def _cancel_orphans(self, symbol: str) -> None:
        """Отменить ордера символа, не нужные позиции (осиротевшие)."""
        position = await self._local_or_exchange_position(symbol)
        # «Нужными» считаются ВСЕ ордера позиции (SL+TP1+TP2), не только
        # SL — иначе живые TP восстановленной позиции отменялись бы как
        # «осиротевшие» (Б-2, найдено тестом adopt)
        wanted: set[str] = set()
        if position is not None:
            wanted = self._engine.tracked_order_ids(symbol)

        # полный список открытых снимаем, фильтруем по префиксу wx
        for ack in await self._venue.open_orders(symbol):
            if ack.client_order_id.startswith("wx") and ack.client_order_id not in wanted:
                logger.warning("[RECON] осиротевший ордер %s по %s — отмена",
                               ack.client_order_id, symbol)
                await self._engine.cancel_order_safe(symbol, ack.client_order_id)

    async def _local_or_exchange_position(
        self, symbol: str
    ) -> ExchangePosition | None:
        """Есть ли фактически позиция символа (по бирже)."""
        for p in await self._venue.positions():
            if p.symbol == symbol:
                return p
        return None

    # ---------------- лёгкая периодическая ----------------

    async def light_check(self) -> None:
        """Периодическая: объёмы/количество vs биржа; расхождение -> полный разбор."""
        ex_positions = {p.symbol: p for p in await self._venue.positions()}
        local = self._engine.local_symbols()
        for symbol in local:
            if symbol not in ex_positions:
                logger.warning("[RECON] лёгкая: %s локально есть, на бирже нет", symbol)
                await self.full_symbol(symbol)
                continue
            # позиции с нулевым qty на бирже = закрыта
        for symbol in set(ex_positions) - set(local):
            logger.warning("[RECON] лёгкая: %s на бирже есть, локально нет", symbol)
            await self.full_symbol(symbol)

    # ---------------- точечная ----------------

    async def full_symbol(self, symbol: str) -> None:
        """Полный разбор одного символа (после реконнекта/триггера)."""
        ex_positions = {p.symbol: p for p in await self._venue.positions()}
        ex = ex_positions.get(symbol)
        local = self._engine.local_position(symbol)
        async with self._engine.lock(symbol):
            if ex is not None and local is not None:
                await self._engine.adopt_exchange_position(
                    symbol, ex, None, None, None
                )
            elif ex is not None and local is None:
                self._incident("external_position", symbol, "critical",
                               f"позиция на бирже qty={ex.qty}, в движке нет")
            elif ex is None and local is not None:
                await self._resolve_dead_close(symbol)
        if ex is not None:
            await self._engine.emergency_protect(symbol)
        await self._cancel_orphans(symbol)

    def _incident(
        self, type_: str, symbol: str, severity: str, details: str
    ) -> None:
        """Записать инцидент + алерт (без молчаливых потерь)."""
        import time

        self._storage.insert_incident(
            int(time.time() * 1000), self._mode, type_, symbol, severity, details
        )
        if self._notifier is not None:
            self._notifier.alert(severity, type_, details)
