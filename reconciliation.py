#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# ФАЙЛ: reconciliation.py
# СОХРАНИТЬ КАК: reconciliation.py
Модуль сверки состояния БД ↔ биржа при старте бота.
Реализует раздел 12 документа "Алгоритм модуля реальной торговли".

Сценарии расхождений:
1. Совпадение (БД + биржа) → восстановить in-memory из биржи (биржа приоритетна)
2. Была в БД, нет на бирже → найти closing-сделку, дозаписать в trades
3. Позиция есть, SL/TP нет → аварийная ветка (восстановить SL)
4. Позиции нет в БД, есть на бирже → аномалия, логировать (не трогаем)
5. Осиротевший ордер → отменить

До завершения reconciliation новые позиции НЕ открываются.
"""
import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from logger import log, fmt_price, debug_log
from config import Config


class ReconciliationResult:
    """Результат reconciliation для логирования/статистики."""
    def __init__(self):
        self.restored = []          # символы, восстановленные из биржи
        self.closed_missing = []    # символы, закрытые как "пропавшие"
        self.emergency_restored = []  # символы, где восстановили SL
        self.emergency_closed = []  # символы, закрытые форс-закрытием
        self.anomalies = []         # символы-аномалии (есть на бирже, нет в БД)
        self.orphan_orders = []     # отменённые осиротевшие ордера
        self.errors = []            # ошибки при обработке

    def summary(self) -> str:
        parts = []
        if self.restored:
            parts.append(f"восстановлено={len(self.restored)}")
        if self.closed_missing:
            parts.append(f"закрыто_пропавших={len(self.closed_missing)}")
        if self.emergency_restored:
            parts.append(f"восстановлено_SL={len(self.emergency_restored)}")
        if self.emergency_closed:
            parts.append(f"форс-закрыто={len(self.emergency_closed)}")
        if self.anomalies:
            parts.append(f"аномалий={len(self.anomalies)}")
        if self.orphan_orders:
            parts.append(f"осиротевших_ордеров={len(self.orphan_orders)}")
        if self.errors:
            parts.append(f"ошибок={len(self.errors)}")
        return ", ".join(parts) if parts else "расхождений нет"


class Reconciliator:
    """
    Выполняет reconciliation при старте.
    Работает только в real-режиме. В paper-режиме возвращает успех сразу.
    """

    def __init__(self, rest_client, tracker, db, exchange):
        """
        rest_client — BinanceFuturesRestClient
        tracker — PositionTracker (с db внутри)
        db — Database
        exchange — ExchangeAdapter (RealExchange или PaperExchange)
        """
        self.rest = rest_client
        self.tracker = tracker
        self.db = db
        self.exchange = exchange

    async def run(self) -> Tuple[bool, ReconciliationResult]:
        """
        Главная точка входа.
        Возвращает (success, result).
        success=False означает критическую ошибку — бот не должен торговать.
        """
        result = ReconciliationResult()

        # В paper-режиме reconciliation не нужен
        if not isinstance(self.exchange, type(self.exchange)) or \
           self.exchange.__class__.__name__ == "PaperExchange":
            log.info("[RECON] Paper-режим — reconciliation пропущен")
            return True, result

        log.info("[RECON] === Начало reconciliation ===")
        t0 = time.time()

        try:
            # 1. Загружаем позиции из БД
            db_positions = self.db.load_all_open_positions()
            db_by_symbol: Dict[str, dict] = {
                p["symbol"]: p for p in db_positions if p.get("symbol")
            }
            log.info(f"[RECON] В БД найдено {len(db_by_symbol)} открытых позиций")

            # 2. Запрашиваем реальное состояние биржи
            exchange_positions = await self.rest.get_position_risk()
            ex_by_symbol: Dict[str, dict] = {
                p["symbol"]: p for p in exchange_positions
            }
            log.info(f"[RECON] На бирже найдено {len(ex_by_symbol)} позиций")

            # 3. Запрашиваем все открытые ордера (для поиска осиротевших)
            all_open_orders = await self.rest.get_open_orders()
            log.info(f"[RECON] На бирже найдено {len(all_open_orders)} открытых ордеров")

            # 4. Обрабатываем каждую позицию из БД
            for symbol, db_pos in db_by_symbol.items():
                try:
                    await self._process_db_position(symbol, db_pos, ex_by_symbol, result)
                except Exception as e:
                    log.error(f"[RECON] Ошибка обработки {symbol}: {e}")
                    result.errors.append(f"{symbol}: {e}")

            # 5. Ищем аномалии: позиции на бирже, которых нет в БД
            for symbol, ex_pos in ex_by_symbol.items():
                if symbol not in db_by_symbol:
                    log.warning(
                        f"[RECON] АНОМАЛИЯ: {symbol} есть на бирже "
                        f"(qty={ex_pos.get('position_amt', 0):.6f}), "
                        f"но нет в БД. Не трогаем автоматически."
                    )
                    result.anomalies.append(symbol)

            # 6. Ищем осиротевшие ордера (ордер есть, позиции под него нет)
            await self._process_orphan_orders(all_open_orders, ex_by_symbol, result)

            elapsed = time.time() - t0
            log.info(
                f"[RECON] === Reconciliation завершён за {elapsed:.2f}с: "
                f"{result.summary()} ==="
            )

            return True, result

        except Exception as e:
            log.error(f"[RECON] Критическая ошибка reconciliation: {e}")
            result.errors.append(f"critical: {e}")
            return False, result

    async def _process_db_position(
        self,
        symbol: str,
        db_pos: dict,
        ex_by_symbol: Dict[str, dict],
        result: ReconciliationResult,
    ):
        """Обрабатывает одну позицию из БД."""
        ex_pos = ex_by_symbol.get(symbol)

        if ex_pos is None:
            # Сценарий 2: была в БД, нет на бирже
            await self._handle_missing_position(symbol, db_pos, result)
        else:
            # Сценарий 1 или 3: позиция есть на бирже
            await self._handle_existing_position(symbol, db_pos, ex_pos, result)

    async def _handle_missing_position(
        self,
        symbol: str,
        db_pos: dict,
        result: ReconciliationResult,
    ):
        """
        Сценарий 2: позиция была в БД, но на бирже её нет.
        Ищем closing-сделку в истории, дозаписываем в trades.
        """
        log.info(f"[RECON] {symbol}: была в БД, но на бирже нет. Ищу closing-сделку...")

        try:
            trades = await self.rest.get_user_trades(
                to_binance_symbol_local(symbol),
                limit=50,
            )
        except Exception as e:
            log.error(f"[RECON] {symbol}: не удалось получить историю сделок: {e}")
            result.errors.append(f"{symbol}: no trades history")
            # Удаляем из БД — позиции всё равно нет
            self.db.delete_open_position(symbol)
            result.closed_missing.append(symbol)
            return

        # Ищем самую свежую closing-сделку (по qty совпадающую с db_pos)
        db_qty = abs(db_pos.get("quantity", 0.0))
        closing_trades = []
        for t in trades:
            t_qty = abs(t.get("quantity", 0))
            if t_qty > 0 and abs(t_qty - db_qty) / max(db_qty, 1e-12) < 0.05:
                closing_trades.append(t)

        if closing_trades:
            # Берём самую свежую
            closing_trades.sort(key=lambda t: t.get("time", 0), reverse=True)
            last = closing_trades[0]

            # Пытаемся определить exit_reason
            # Если realizedPnl близок к 0 или отрицательный — скорее всего SL
            # Если положительный и большой — TP
            realized_pnl = last.get("realized_pnl", 0.0)
            exit_price = last.get("price", db_pos.get("entry_price", 0.0))
            entry_price = db_pos.get("entry_price", 0.0)

            # Простая эвристика определения причины
            if entry_price > 0:
                side = db_pos.get("side", "LONG")
                if side == "LONG":
                    profit_pct = (exit_price - entry_price) / entry_price * 100
                else:
                    profit_pct = (entry_price - exit_price) / entry_price * 100
            else:
                profit_pct = 0.0

            if realized_pnl < -0.01:
                exit_reason = "SL"
            elif profit_pct > 1.0:
                exit_reason = "TP"
            else:
                exit_reason = "UNKNOWN_RECONCILE"

            # Записываем в trades
            self.db.log_trade({
                "timestamp": datetime.now().isoformat(),
                "symbol": symbol,
                "side": db_pos.get("side", "LONG"),
                "entry_price": entry_price,
                "exit_price": exit_price,
                "size_usdt": db_pos.get("size_usdt", 0.0),
                "qty": db_qty,
                "pnl_pct": 0.0,
                "pnl_usdt": realized_pnl,
                "exit_reason": exit_reason,
                "entry_time": datetime.fromtimestamp(
                    db_pos.get("entry_time", time.time())
                ).isoformat() if db_pos.get("entry_time") else None,
                "exit_time": datetime.fromtimestamp(
                    last.get("time", time.time() * 1000) / 1000.0
                ).isoformat(),
                "score": db_pos.get("score", 0),
                "sl_pct": db_pos.get("sl_pct", 0.0),
                "tp_pct": db_pos.get("tp1_pct", 0.0),
                "mfe": db_pos.get("mfe", 0.0),
                "mae": db_pos.get("mae", 0.0),
            })

            log.info(
                f"[RECON] {symbol}: дозаписана сделка "
                f"exit_price={exit_price}, pnl={realized_pnl:+.4f}, "
                f"reason={exit_reason}"
            )
        else:
            log.warning(
                f"[RECON] {symbol}: closing-сделка не найдена в истории. "
                f"Просто удаляем из БД."
            )

        # Удаляем позицию из БД
        self.db.delete_open_position(symbol)
        result.closed_missing.append(symbol)

    async def _handle_existing_position(
        self,
        symbol: str,
        db_pos: dict,
        ex_pos: dict,
        result: ReconciliationResult,
    ):
        """
        Сценарий 1 или 3: позиция есть и в БД, и на бирже.
        Проверяем наличие SL/TP. Если нет — аварийная ветка.
        """
        # Восстанавливаем in-memory из данных биржи (биржа приоритетна)
        restored_pos = self._build_position_from_exchange(symbol, db_pos, ex_pos)
        self.tracker.positions[symbol] = restored_pos

        # Обновляем БД актуальными данными с биржи
        self.db.save_open_position(restored_pos)

        result.restored.append(symbol)

        # Проверяем наличие SL/TP
        has_sl = bool(restored_pos.get("sl_order_id") or restored_pos.get("sl_client_id"))
        has_tp = bool(restored_pos.get("tp_order_id") or restored_pos.get("tp_client_id"))

        if not has_sl:
            # Сценарий 3: позиция есть, SL нет → аварийная ветка
            log.warning(f"[RECON] {symbol}: позиция есть, но SL отсутствует! Аварийная ветка.")
            await self._emergency_restore_sl(symbol, restored_pos, result)
        else:
            log.info(
                f"[RECON] {symbol}: восстановлена из биржи "
                f"[{restored_pos.get('side', 'LONG')}] "
                f"qty={restored_pos.get('quantity', 0):.6f} "
                f"SL={fmt_price(restored_pos.get('sl_price', 0))} "
                f"SL_id={restored_pos.get('sl_order_id')}"
            )

    def _build_position_from_exchange(
        self,
        symbol: str,
        db_pos: dict,
        ex_pos: dict,
    ) -> dict:
        """
        Строит позицию для in-memory, используя данные биржи как приоритетные.
        """
        position_amt = ex_pos.get("position_amt", 0.0)
        side = "LONG" if position_amt > 0 else "SHORT"

        # Берём из биржи: qty, entry_price
        # Из БД: всё остальное (SL/TP цены, score и т.д.)
        restored = dict(db_pos)  # копия
        restored["side"] = side
        restored["quantity"] = abs(position_amt)
        restored["remaining_qty"] = abs(position_amt)
        restored["entry_price"] = ex_pos.get("entry_price", db_pos.get("entry_price", 0.0))

        # Пытаемся восстановить sl_order_id из БД (если он там был)
        # В будущем здесь можно запрашивать algo-ордера с биржи
        # Но пока полагаемся на данные из БД

        return restored

    async def _emergency_restore_sl(
        self,
        symbol: str,
        pos: dict,
        result: ReconciliationResult,
    ):
        """
        Аварийное восстановление SL.
        2-3 попытки, если не удалось — форс-закрытие.
        """
        sl_price = pos.get("sl_price", 0.0)
        if sl_price <= 0:
            log.error(f"[RECON] {symbol}: sl_price=0, форс-закрытие")
            await self._force_close_position(symbol, pos, result)
            return

        # Помечаем позицию как unprotected
        pos["unprotected"] = True

        for attempt in range(3):
            try:
                log.info(
                    f"[RECON] {symbol}: попытка восстановления SL "
                    f"(attempt {attempt + 1}/3, price={sl_price})"
                )
                sl_order = await self.exchange.place_sl(symbol, sl_price)
                if sl_order:
                    pos["sl_order_id"] = sl_order.get("order_id")
                    pos["sl_client_id"] = sl_order.get("client_order_id")
                    pos["unprotected"] = False
                    self.db.save_open_position(pos)
                    log.info(
                        f"[RECON] {symbol}: SL восстановлен "
                        f"(id={sl_order.get('order_id')})"
                    )
                    result.emergency_restored.append(symbol)
                    return
            except Exception as e:
                log.error(f"[RECON] {symbol}: ошибка восстановления SL: {e}")

            await asyncio.sleep(0.5)

        # Все попытки провалились → форс-закрытие
        log.error(f"[RECON] {symbol}: не удалось восстановить SL после 3 попыток. Форс-закрытие.")
        await self._force_close_position(symbol, pos, result)

    async def _force_close_position(
        self,
        symbol: str,
        pos: dict,
        result: ReconciliationResult,
    ):
        """Форс-закрытие позиции без SL."""
        try:
            qty = pos.get("remaining_qty", 0.0)
            if qty <= 0:
                log.warning(f"[RECON] {symbol}: qty=0, просто удаляем")
            else:
                # Закрываем через exchange
                close_order = await self.exchange.close_position(symbol, qty, 0.0)
                if close_order and close_order.get("filled_amount", 0) > 0:
                    log.info(
                        f"[RECON] {symbol}: форс-закрытие успешно "
                        f"(qty={close_order.get('filled_amount', 0):.6f})"
                    )
                else:
                    log.error(f"[RECON] {symbol}: форс-закрытие не удалось!")
        except Exception as e:
            log.error(f"[RECON] {symbol}: ошибка форс-закрытия: {e}")

        # Удаляем из БД и in-memory
        self.db.delete_open_position(symbol)
        self.tracker.positions.pop(symbol, None)
        result.emergency_closed.append(symbol)

    async def _process_orphan_orders(
        self,
        all_open_orders: List[dict],
        ex_by_symbol: Dict[str, dict],
        result: ReconciliationResult,
    ):
        """
        Сценарий 5: осиротевшие ордера.
        Ордер есть на бирже, но позиции под него нет.
        """
        # Собираем символы, по которым есть ордера
        orders_by_symbol: Dict[str, List[dict]] = {}
        for o in all_open_orders:
            sym = o.get("symbol", "")
            if sym:
                orders_by_symbol.setdefault(sym, []).append(o)

        for symbol, orders in orders_by_symbol.items():
            if symbol in ex_by_symbol:
                # Позиция есть — ордера не осиротевшие
                continue

            # Позиции нет, но ордера есть → осиротевшие
            for o in orders:
                try:
                    log.warning(
                        f"[RECON] Осиротевший ордер: {symbol} "
                        f"id={o.get('order_id')} type={o.get('type')} "
                        f"side={o.get('side')} — отменяю"
                    )
                    await self.rest.cancel_order(
                        symbol,
                        o.get("order_id"),
                        client_order_id=o.get("client_order_id"),
                    )
                    result.orphan_orders.append(f"{symbol}:{o.get('order_id')}")
                except Exception as e:
                    log.error(f"[RECON] Ошибка отмены осиротевшего ордера {symbol}: {e}")


def to_binance_symbol_local(internal_symbol: str) -> str:
    """Локальная копия to_binance_symbol, чтобы не импортировать из api."""
    return internal_symbol.replace("_", "")