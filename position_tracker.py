#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# ФАЙЛ: position_tracker.py
# СОХРАНИТЬ КАК: position_tracker.py

Ядро управления позицией.
Отвечает за:
- трейлинг (все ступени);
- TP1 / TP2 (частичное закрытие);
- breakeven;
- проверку SL;
- timeout;
- MFE / MAE.

НЕ отвечает за:
- решение "открывать или нет" (это risk_manager);
- кулдауны и блокировки (это risk_manager);
- статистику и БД (это risk_manager).

Работает через ExchangeAdapter, не зная, paper это или real.
Возвращает список событий закрытия, чтобы вызывающий код мог их обработать.

Теперь PositionTracker синхронизирует открытые позиции с БД:
- При открытии позиции — save_open_position()
- При изменении SL (трейлинг/breakeven) — save_open_position()
- При TP1 — save_open_position()
- При полном закрытии — delete_open_position()
- При старте — load_positions_from_db() восстанавливает позиции
"""
import asyncio
import time
from typing import Dict, List, Optional, Any
from config import Config
from exchange_adapter import ExchangeAdapter
from logger import log, fmt_price, debug_log


class PositionTracker:
    """
    Управляет открытыми позициями.
    Работает через ExchangeAdapter.
    """
    
    def __init__(self, exchange: ExchangeAdapter, db=None):
        self.exchange = exchange
        # [НОВОЕ] База данных для персистентности открытых позиций
        self.db = db
        # Открытые позиции: symbol -> dict
        self.positions: Dict[str, dict] = {}
        # Шаги трейлинга из config
        self.trail_steps = sorted(Config.TRAILING_STEPS.keys())
    
    # ================================================================
    # ОТКРЫТИЕ ПОЗИЦИИ
    # ================================================================
    async def open_position(
        self,
        symbol: str,
        side: str,
        entry_price: float,
        qty: float,
        sl_price: float,
        tp1_price: float,
        tp2_price: float,
        sl_pct: float,
        tp1_pct: float,
        tp2_pct: float,
        size_usdt: float,
        score: float = 0.0,
        confidence: str = "MEDIUM",
        sl_source: str = "unknown",
    ) -> bool:
        """Открывает позицию."""
        if symbol in self.positions:
            log.warning(f"{symbol}: позиция уже открыта")
            return False
        
        if qty <= 0 or entry_price <= 0:
            log.error(f"{symbol}: некорректные параметры qty={qty}, entry_price={entry_price}")
            return False
        
        # [НОВОЕ] Регистрируем позицию в exchange, передавая цены для защиты
        if side == "LONG":
            order = await self.exchange.place_market_buy(
                symbol, qty, price=entry_price, sl_price=sl_price, tp_price=tp2_price
            )
        else:
            order = await self.exchange.place_market_sell(
                symbol, qty, price=entry_price, sl_price=sl_price, tp_price=tp2_price
            )
                
        if not order or order.get("filled_amount", 0) <= 0:
            log.error(f"{symbol}: не удалось открыть позицию через exchange")
            return False

        # [НОВОЕ] Сохраняем entry_order_id
        entry_order_id = order.get("order_id")
        client_order_id = order.get("client_order_id")
        
        # Используем реальные значения из ответа exchange
        actual_qty = order.get("filled_amount", qty)
        actual_price = order.get("avg_price", entry_price)
        if actual_price <= 0:
            actual_price = entry_price

        # # [НОВОЕ] Получаем minNotional для символа
        min_notional = await self.exchange.get_min_notional(symbol)
        margin = Config.MIN_NOTIONAL_SAFETY_MARGIN

        # [ИСПРАВЛЕНО] Устойчивая проверка полной стратегии
        # Обе части (TP1 и остаток на TP2) должны быть >= minNotional * margin
        # Поэтому проверяем МЕНЬШУЮ из двух частей
        tp1_frac = Config.TP1_SIZE_FRAC
        tp2_frac = 1.0 - tp1_frac
        smaller_frac = min(tp1_frac, tp2_frac)  # меньшая из двух частей

        # Меньшая часть позиции в USDT
        smaller_part = size_usdt * smaller_frac
        # Минимально допустимый размер с запасом
        min_required = min_notional * margin

        full_strategy = smaller_part >= min_required

        
        if full_strategy:
            # Динамически пересчитываем TP1_SIZE_FRAC
            # Остаток после TP1 должен быть >= min_notional * margin
            # size * (1 - frac) >= min_notional * margin
            # frac <= 1 - (min_notional * margin / size)
            max_frac = 1.0 - (min_notional * margin / size_usdt)
            dynamic_tp1_frac = min(Config.TP1_SIZE_FRAC, max_frac)
            dynamic_tp1_frac = max(0.1, dynamic_tp1_frac)  # Минимум 10%
        else:
            dynamic_tp1_frac = 0.0  # TP1 не будет
        
        now = time.time()
        
        # Создаём локальную позицию с реальными значениями
        pos = {
            "symbol": symbol,
            "side": side,
            "entry_price": actual_price,
            "entry_time": now,
            "quantity": actual_qty,
            "remaining_qty": actual_qty,
            "sl_price": sl_price,
            "sl_pct": sl_pct,
            "tp1_price": tp1_price,
            "tp1_pct": tp1_pct,
            "tp2_price": tp2_price,
            "tp2_pct": tp2_pct,
            "score": score,
            "confidence": confidence,
            "size_usdt": size_usdt,
            "highest": actual_price,
            "breakeven_set": False,
            "trailing_activated": False,
            "tp1_done": False,
            "tp2_done": False,
            "sl_source": sl_source,
            "realized_pnl": 0.0,
            "closed_qty": 0.0,
            "trail_active": False,
            "mfe": 0.0,
            "mae": 0.0,
            "tp1_closed_qty": 0.0,
            "closing": False,
            "last_watch_price": actual_price,
            # [НОВОЕ] Сохраняем ID защитных ордеров, полученных от RealExchange
            "sl_order_id": order.get("sl_order_id"),
            "sl_client_id": order.get("sl_client_id"),
            "tp_order_id": order.get("tp_order_id"),
            "tp_client_id": order.get("tp_client_id"),
            # [НОВОЕ] Параметры для гибридной стратегии
            "min_notional": min_notional,
            "full_strategy": full_strategy,
            "tp1_size_frac": dynamic_tp1_frac,
            
            "entry_order_id": entry_order_id,
            "client_order_id": client_order_id,
        }

        # [НОВОЕ] Если маленькая сделка — сразу помечаем TP1 как пропущенный
        if not full_strategy:
            pos["tp1_done"] = True
            pos["tp1_skipped"] = True
            log.info(
                f"{symbol}: малый размер ({size_usdt:.2f} USDT < "
                f"{2 * min_notional * margin:.2f}), "
                f"упрощённая стратегия (без TP1)"
            )
        else:
            log.info(
                f"{symbol}: полная стратегия, "
                f"TP1_SIZE_FRAC={dynamic_tp1_frac:.2f}"
            )
                
        
        self.positions[symbol] = pos

        # [НОВОЕ] Сохраняем позицию в БД
        if self.db:
            try:
                self.db.save_open_position(pos)
                debug_log(f"[TRACKER] {symbol}: позиция сохранена в БД")
            except Exception as e:
                log.error(f"{symbol}: ошибка сохранения позиции в БД: {e}")
        
        log.info(
            f"TRACKER OPEN {symbol} [{side}] @ {fmt_price(actual_price)} "
            f"qty={actual_qty:.6f} size=${size_usdt:.1f} "
            f"SL={fmt_price(sl_price)} ({sl_pct:.1f}%) "
            f"TP1={fmt_price(tp1_price)} ({tp1_pct:.1f}%) "
            f"TP2={fmt_price(tp2_price)} ({tp2_pct:.1f}%)"
        )
        
        return True
    
    # ================================================================
    # ОБНОВЛЕНИЕ ЦЕН
    # ================================================================
    async def update_prices(self, prices: Dict[str, float]) -> List[dict]:
        """
        Обновляет цены и проверяет условия закрытия.
        
        Возвращает список событий:
        [
            {"symbol": "BTC_USDT", "reason": "TP1", "price": 52000, "qty": 0.06, "pnl": 120.0},
            {"symbol": "ETH_USDT", "reason": "SL", "price": 2800, "qty": 0.5, "pnl": -50.0},
        ]
        """
        events = []
        now = time.time()
        
        for symbol, pos in list(self.positions.items()):
            price = prices.get(symbol)
            if not price or price <= 0:
                continue

            # [ИСПРАВЛЕНО] Проверяем здоровье SL раз в 30 секунд
            close_event = await self._check_sl_health(symbol, pos)
            if close_event:
                # Позиция закрыта на бирже — добавляем событие
                events.append(close_event)
                continue

            # [НОВОЕ] Если позиция unprotected — блокируем трейлинг
            if pos.get("unprotected", False):
                debug_log(f"[DEBUG-TRACKER] {symbol}: unprotected, пропускаем трейлинг")

            # Обновляем MFE/MAE
            side = pos.get("side", "LONG")
            is_short = side == "SHORT"
            
            if is_short:
                profit_pct = (pos["entry_price"] - price) / pos["entry_price"] * 100.0
                if price < pos["highest"]:
                    pos["highest"] = price
            else:
                profit_pct = (price - pos["entry_price"]) / pos["entry_price"] * 100.0
                if price > pos["highest"]:
                    pos["highest"] = price
            
            if profit_pct > pos.get("mfe", 0.0):
                pos["mfe"] = profit_pct
            
            adverse_pct = -profit_pct
            if adverse_pct > pos.get("mae", 0.0):
                pos["mae"] = adverse_pct

            # Запоминаем старый SL для отслеживания изменений
            old_sl = pos.get("sl_price")
            
            # Проверяем условия закрытия
            event = await self._check_conditions(symbol, pos, price, now)
            if event:
                debug_log(f"[DEBUG-TRACKER] {symbol}: event returned from _check_conditions, reason={event.get('reason')}")
                events.append(event)
            else:
                debug_log(f"[DEBUG-TRACKER] {symbol}: no event from _check_conditions")

                # [НОВОЕ] Если SL изменился (трейлинг/breakeven), сохраняем в БД
                if pos.get("sl_price") != old_sl:
                    if self.db:
                        try:
                            self.db.save_open_position(pos)
                            debug_log(f"[TRACKER] {symbol}: SL изменён, позиция обновлена в БД")
                        except Exception as e:
                            log.error(f"{symbol}: ошибка обновления позиции в БД: {e}")

            # Обновляем последнюю цену
            if symbol in self.positions:
                self.positions[symbol]["last_watch_price"] = price

        debug_log(f"[DEBUG-TRACKER] update_prices: total events={len(events)}")
        return events
    
    async def _check_sl_health(self, symbol: str, pos: dict) -> Optional[dict]:
        """
        Проверяет статус SL-ордера на бирже.
        Возвращает:
        - None — всё ок или ошибка API
        - dict — событие закрытия (позиция закрыта на бирже)
        """
        now = time.time()
        last_check = pos.get("last_sl_check_ts", 0.0)
        
        # Проверяем не чаще чем раз в 30 секунд
        if now - last_check < 30:
            return None
        
        pos["last_sl_check_ts"] = now
        
        sl_id = pos.get("sl_order_id")
        sl_cid = pos.get("sl_client_id")
        
        if not sl_id and not sl_cid:
            log.warning(f"{symbol}: SL отсутствует в позиции! Аварийная ветка.")
            await self._emergency_restore_sl(symbol, pos)
            return None
        
        # Проверяем статус на бирже
        status = await self.exchange.get_sl_status(symbol)
        
        if status == "active":
            return None  # Всё ок
        
        if status == "triggered":
            # SL сработал, ждём событие закрытия
            return None
        
        if status == "error":
            # Ошибка API — не делаем ничего
            log.warning(f"{symbol}: ошибка проверки статуса SL, пропускаем")
            return None
        
        if status is None:
            # Позиции реально нет на бирже
            log.info(f"{symbol}: позиция закрыта на бирже. Обрабатываю закрытие...")
            return await self._close_position(symbol, pos.get("last_watch_price", 0.0), "EXTERNAL_CLOSE")
        
        if status == "missing":
            # [ИСПРАВЛЕНО] Вызываем аварийную ветку
            log.warning(f"{symbol}: SL пропал с биржи! Пытаюсь восстановить...")
            await self._emergency_restore_sl(symbol, pos)
            return None
        
        # unknown
        log.warning(f"{symbol}: статус SL неизвестен ({status})")
        return None

    async def _emergency_restore_sl(self, symbol: str, pos: dict):
        """
        [НОВОЕ]
        Аварийное восстановление SL.
        2-3 попытки, если не удалось — форс-закрытие.
        [ИСПРАВЛЕНО] Если позиция закрыта на бирже — удаляем из tracker.
        """
        # [ИСПРАВЛЕНО] Сначала проверяем, есть ли позиция на бирже
        pos_info = await self.exchange.get_position_info(symbol)
        if not pos_info or abs(pos_info.get("position_amt", 0)) <= 0:
            # Позиции на бирже нет — значит она была закрыта вручную
            log.info(f"{symbol}: позиция закрыта на бирже. Удаляю из tracker.")
            self.positions.pop(symbol, None)
            if self.db:
                try:
                    self.db.delete_open_position(symbol)
                except Exception as e:
                    log.error(f"{symbol}: ошибка удаления из БД: {e}")
            return
        
        # Помечаем позицию как unprotected (блокирует трейлинг)
        pos["unprotected"] = True
        
        sl_price = pos.get("sl_price", 0.0)
        if sl_price <= 0:
            log.error(f"{symbol}: sl_price=0, форс-закрытие")
            await self._force_close_position(symbol, pos)
            return
        
        for attempt in range(3):
            try:
                log.info(
                    f"{symbol}: попытка восстановления SL "
                    f"(attempt {attempt + 1}/3, price={sl_price})"
                )
                sl_order = await self.exchange.place_sl(symbol, sl_price)
                if sl_order:
                    pos["sl_order_id"] = sl_order.get("order_id")
                    pos["sl_client_id"] = sl_order.get("client_order_id")
                    pos["unprotected"] = False
                    log.info(
                        f"{symbol}: SL восстановлен "
                        f"(id={sl_order.get('order_id')})"
                    )
                    # Сохраняем в БД
                    if self.db:
                        try:
                            self.db.save_open_position(pos)
                        except Exception as e:
                            log.error(f"{symbol}: ошибка сохранения после восстановления SL: {e}")
                    return
                else:
                    # place_sl вернул None — проверяем, есть ли позиция
                    pos_info = await self.exchange.get_position_info(symbol)
                    if not pos_info or abs(pos_info.get("position_amt", 0)) <= 0:
                        log.info(f"{symbol}: позиция закрыта на бирже. Удаляю из tracker.")
                        self.positions.pop(symbol, None)
                        if self.db:
                            try:
                                self.db.delete_open_position(symbol)
                            except Exception as e:
                                log.error(f"{symbol}: ошибка удаления из БД: {e}")
                        return
            except Exception as e:
                log.error(f"{symbol}: ошибка восстановления SL: {e}")
            
            await asyncio.sleep(0.5)
        
        # Все попытки провалились → форс-закрытие
        log.error(f"{symbol}: не удалось восстановить SL после 3 попыток. Форс-закрытие.")
        await self._force_close_position(symbol, pos)

        
    async def _force_close_position(self, symbol: str, pos: dict):
        """
        [НОВОЕ]
        Форс-закрытие позиции без SL.
        Используется, когда SL не удалось восстановить.
        """
        final_qty = pos.get("remaining_qty", 0.0)
        if final_qty <= 0:
            log.warning(f"{symbol}: qty=0, просто удаляем")
        else:
            try:
                close_order = await self.exchange.close_position(symbol, final_qty, 0.0)
                if close_order and close_order.get("filled_amount", 0) > 0:
                    log.info(
                        f"{symbol}: форс-закрытие успешно "
                        f"(qty={close_order.get('filled_amount', 0):.6f})"
                    )
                else:
                    log.error(f"{symbol}: форс-закрытие не удалось!")
            except Exception as e:
                log.error(f"{symbol}: ошибка форс-закрытия: {e}")

        # [ИСПРАВЛЕНО] Отменяем защитные ордера после форс-закрытия
        sl_id = pos.get("sl_order_id")
        tp_id = pos.get("tp_order_id")
        sl_cid = pos.get("sl_client_id")
        tp_cid = pos.get("tp_client_id")

        if sl_id or tp_id:
            try:
                cancel_result = await self.exchange.cancel_sl_tp(
                    symbol,
                    sl_order_id=sl_id,
                    tp_order_id=tp_id,
                    sl_client_id=sl_cid,
                    tp_client_id=tp_cid,
                )
                log.info(
                    f"{symbol}: защитные ордера отменены после форс-закрытия "
                    f"(SL={'✓' if cancel_result.get('sl') else '✗'}, "
                    f"TP={'✓' if cancel_result.get('tp') else '✗'})"
                )
            except Exception as e:
                log.error(f"{symbol}: ошибка отмены ордеров после форс-закрытия: {e}")
        
        # Удаляем из in-memory и БД
        self.positions.pop(symbol, None)
        if self.db:
            try:
                self.db.delete_open_position(symbol)
            except Exception as e:
                log.error(f"{symbol}: ошибка удаления из БД после форс-закрытия: {e}")


    async def _check_conditions(
        self,
        symbol: str,
        pos: dict,
        price: float,
        now: float,
    ) -> Optional[dict]:
        """
        Проверяет все условия закрытия для позиции.
        Возвращает событие закрытия или None.
        """
        side = pos.get("side", "LONG")
        is_short = side == "SHORT"
        
        # ================================================================
        # 1. TIMEOUT
        # ================================================================
        timeout_sec = Config.POSITION_TIMEOUT_HOURS * 3600
        if now - pos["entry_time"] > timeout_sec:
            return await self._close_position(symbol, price, "TIMEOUT")

        hold_minutes = (now - pos["entry_time"]) / 60.0
        if hold_minutes >= Config.VOL_DECAY_CHECK_AFTER_MIN:
            if await self._check_volume_decay(symbol):
                return await self._close_position(symbol, price, "VOL_DECAY")
        
        # ================================================================
        # 2. ТРЕЙЛИНГ
        # ================================================================
        profit_pct = self._calc_profit_pct(pos, price)

        # [НОВОЕ] Если позиция unprotected — блокируем трейлинг
        if pos.get("unprotected", False):
            debug_log(f"[DEBUG-TRACKER] {symbol}: unprotected, трейлинг заблокирован")
        elif profit_pct >= Config.TRAILING_ACTIVATION_PCT:
            new_sl = self._calc_trailing_sl(profit_pct, side, price)
        
        if profit_pct >= Config.TRAILING_ACTIVATION_PCT:
            new_sl = self._calc_trailing_sl(profit_pct, side, price)
            improves = (
                (not is_short and new_sl > pos["sl_price"])
                or (is_short and new_sl < pos["sl_price"])
            )
            if improves:
                pos["sl_price"] = new_sl
                if not pos.get("trail_active", False):
                    pos["trail_active"] = True
                    log.info(
                        f"Trailing activated {symbol} [{side}] "
                        f"profit={profit_pct:.2f}% SL={fmt_price(new_sl)}"
                    )
                else:
                    log.info(
                        f"Trailing update {symbol} [{side}]: "
                        f"SL -> {fmt_price(new_sl)} profit={profit_pct:.2f}%"
                    )
        
        # ================================================================
        # 3. TP1
        # ================================================================
        tp1_condition = (
            (is_short and price <= pos["tp1_price"])
            or (not is_short and price >= pos["tp1_price"])
        )
        
        if not pos.get("tp1_done", False) and tp1_condition:
            # [НОВОЕ] Проверяем, что позиция ещё есть на бирже
            pos_info = await self.exchange.get_position_info(symbol)
            if not pos_info or abs(pos_info.get("position_amt", 0)) <= 0:
                log.info(f"{symbol}: позиция уже закрыта на бирже, пропускаем TP1")
                pos["tp1_done"] = True  # Помечаем как выполненный, чтобы не пытаться снова
                return None
            target_tp1_qty = pos["quantity"] * pos.get("tp1_size_frac", Config.TP1_SIZE_FRAC)
            tp1_closed_qty = pos.get("tp1_closed_qty", 0.0)
            need_qty = target_tp1_qty - tp1_closed_qty
            
            if need_qty > 0:
                closed_qty = await self._execute_partial_close(
                    symbol, need_qty, pos["tp1_price"], "TP1"
                )
                if closed_qty > 0:
                    pos["tp1_closed_qty"] = tp1_closed_qty + closed_qty
                    
                    if (
                        pos["tp1_closed_qty"] >= target_tp1_qty * 0.999
                        or pos["remaining_qty"] <= target_tp1_qty * 0.01
                    ):
                        pos["tp1_done"] = True
                        
                        # После TP1 — breakeven
                        await self._set_breakeven(symbol, pos)

                        # [НОВОЕ] Сохраняем позицию в БД после TP1
                        if self.db:
                            try:
                                self.db.save_open_position(pos)
                                debug_log(f"[TRACKER] {symbol}: позиция обновлена в БД после TP1")
                            except Exception as e:
                                log.error(f"{symbol}: ошибка обновления позиции в БД после TP1: {e}")

                        
                        pnl = self._calc_pnl(pos["entry_price"], pos["tp1_price"], closed_qty, side)
                       
                        return {
                                "symbol": symbol,
                                "reason": "TP1",
                                "price": pos["tp1_price"],
                                "qty": closed_qty,
                                "pnl": pnl,
                                "entry_price": pos["entry_price"],
                                "entry_time": pos["entry_time"],
                                "size_usdt": pos["size_usdt"],
                                "score": pos["score"],
                                "side": side,
                                "mfe": pos.get("mfe", 0.0),
                                "mae": pos.get("mae", 0.0),
                                "sl_pct": pos.get("sl_pct", 0.0),
                                "tp_pct": pos.get("tp1_pct", 0.0),
                            }
        
        # ================================================================
        # 4. TP2
        # ================================================================
        tp2_condition = (
            (is_short and price <= pos["tp2_price"])
            or (not is_short and price >= pos["tp2_price"])
        )
        
        if not pos.get("tp2_done", False) and tp2_condition:
            return await self._close_position(symbol, price, "TP2")
        
        # ================================================================
        # 5. STOP LOSS
        # ================================================================
        sl_condition = (
            (is_short and price >= pos["sl_price"])
            or (not is_short and price <= pos["sl_price"])
        )
        
        if sl_condition:
            # Определяем причину
            if pos.get("trail_active", False):
                sl_reason = "TRAIL_SL"
            elif pos.get("breakeven_set", False):
                sl_reason = "BE_SL"
            else:
                sl_reason = "SL"
            
            return await self._close_position(symbol, price, sl_reason)
        
        # ================================================================
        # 6. Обычный Breakeven (без TP1)
        # ================================================================
        if (
            not pos.get("breakeven_set", False)
            and profit_pct >= Config.BREAKEVEN_ACTIVATION_PCT
        ):
            await self._set_breakeven(symbol, pos)
        
        return None
    
    # ================================================================
    # ЗАКРЫТИЕ ПОЗИЦИИ
    # ================================================================
    async def _close_position(
        self,
        symbol: str,
        exit_price: float,
        reason: str,
    ) -> Optional[dict]:
        """
        Полностью закрывает позицию.
        Возвращает событие закрытия.
        """
        pos = self.positions.get(symbol)
        if not pos:
            log.warning(f"[DEBUG-TRACKER] {symbol}: pos is None, returning None")
            return None
        if pos.get("closing"):
            log.warning(f"[DEBUG-TRACKER] {symbol}: closing flag set, returning None")
            return None
        final_qty = pos["remaining_qty"]
        if final_qty <= 0:
            log.warning(f"[DEBUG-TRACKER] {symbol}: final_qty <= 0, removing and returning None")
            self.positions.pop(symbol, None)
            # [НОВОЕ] Удаляем из БД
            if self.db:
                try:
                    self.db.delete_open_position(symbol)
                    debug_log(f"[TRACKER] {symbol}: позиция удалена из БД (qty=0)")
                except Exception as e:
                    log.error(f"{symbol}: ошибка удаления позиции из БД: {e}")
            return None
        pos["closing"] = True
        
        debug_log(f"[DEBUG-TRACKER] {symbol}: entering _close_position, reason={reason}, final_qty={final_qty}")
        
        try:
            # [НОВОЕ] Проверяем, есть ли позиция на бирже
            pos_info = await self.exchange.get_position_info(symbol)
            has_position = pos_info is not None and abs(pos_info.get("position_amt", 0)) > 0

            debug_log(f"[DEBUG-TRACKER] {symbol}: has_position={has_position}")         

            if not has_position:
                debug_log(f"[DEBUG-TRACKER] {symbol}: позиция закрыта на бирже, начинаем обработку")
                
                # Сначала считаем расчётный PnL
                side = pos.get("side", "LONG")
                pnl = self._calc_pnl(pos["entry_price"], exit_price, final_qty, side)
                debug_log(f"[DEBUG-TRACKER] {symbol}: расчётный PnL={pnl:+.2f}$")
                
                # Пытаемся получить реальную цену выхода из истории сделок
                try:
                    debug_log(f"[DEBUG-TRACKER] {symbol}: запрашиваем историю сделок...")
                    trades = await self.exchange.get_user_trades(symbol, limit=50)
                    debug_log(f"[DEBUG-TRACKER] {symbol}: получено {len(trades) if trades else 0} сделок из истории")
                    
                    if trades:
                        closing_trades = [
                            t for t in trades 
                            if abs(t.get("quantity", 0) - final_qty) < 1e-6
                        ]
                        debug_log(f"[DEBUG-TRACKER] {symbol}: найдено {len(closing_trades)} closing-сделок")
                        
                        if closing_trades:
                            # Сортируем по времени и берём последнюю (exit-сделку)
                            closing_trades.sort(key=lambda t: t.get("time", 0))
                            last_trade = closing_trades[-1]  # последняя = exit
                            real_exit_price = last_trade.get("price", exit_price)
                            real_pnl = last_trade.get("realized_pnl", pnl)
                            # Если realizedPnl == 0 (бывает для entry-сделки), используем расчётный
                            if abs(real_pnl) < 1e-9:
                                real_pnl = pnl
                                log.info(f"{symbol}: realizedPnl=0 из истории, использую расчётный PnL={pnl:+.2f}$")
                            else:
                                log.info(f"{symbol}: реальная цена выхода={real_exit_price}, PnL={real_pnl}")
                                
                            exit_price = real_exit_price
                            pnl = real_pnl
                        else:
                            log.warning(f"{symbol}: не найдено closing-сделок в истории, использую расчётный PnL")
                    else:
                        log.warning(f"{symbol}: история сделок пуста, использую расчётный PnL")
                except Exception as e:
                    log.error(f"{symbol}: ошибка получения истории сделок: {e}")
                    import traceback
                    log.error(traceback.format_exc())
                
                debug_log(f"[DEBUG-TRACKER] {symbol}: история сделок обработана, переходим к отмене SL/TP")
                
                # Отменяем защитные ордера на бирже
                sl_id = pos.get("sl_order_id")
                tp_id = pos.get("tp_order_id")
                sl_cid = pos.get("sl_client_id")
                tp_cid = pos.get("tp_client_id")
                
                debug_log(f"[DEBUG-TRACKER] {symbol}: sl_id={sl_id}, tp_id={tp_id}, sl_cid={sl_cid}, tp_cid={tp_cid}")
                
                if sl_id or tp_id:
                    try:
                        debug_log(f"[DEBUG-TRACKER] {symbol}: отменяем защитные ордера...")
                        cancel_result = await self.exchange.cancel_sl_tp(
                            symbol,
                            sl_order_id=sl_id,
                            tp_order_id=tp_id,
                            sl_client_id=sl_cid,
                            tp_client_id=tp_cid,
                        )
                        log.info(
                            f"{symbol}: защитные ордера отменены "
                            f"(SL={'✓' if cancel_result.get('sl') else '✗'}, "
                            f"TP={'✓' if cancel_result.get('tp') else '✗'})"
                        )
                    except Exception as e:
                        log.error(f"{symbol}: ошибка отмены защитных ордеров: {e}")
                        import traceback
                        log.error(traceback.format_exc())
                
                debug_log(f"[DEBUG-TRACKER] {symbol}: SL/TP отменены, удаляем позицию локально")
                
                # Удаляем позицию локально
                self.positions.pop(symbol, None)



                # [НОВОЕ] Удаляем позицию из БД
                if self.db:
                    try:
                        self.db.delete_open_position(symbol)
                        debug_log(f"[TRACKER] {symbol}: позиция удалена из БД (закрыта на бирже)")
                    except Exception as e:
                        log.error(f"{symbol}: ошибка удаления позиции из БД: {e}")
                
                # Определяем причину закрытия
                original_reason = reason
                if pos.get("trail_active", False):
                    reason = "TRAIL_SL"
                elif pos.get("breakeven_set", False):
                    reason = "BE_SL"
                else:
                    reason = "SL"
                
                debug_log(f"[DEBUG-TRACKER] {symbol}: причина закрытия: {original_reason} -> {reason}")
                
                event = {
                    "symbol": symbol,
                    "reason": reason,
                    "price": exit_price,
                    "qty": final_qty,
                    "pnl": pnl,
                    "entry_price": pos["entry_price"],
                    "entry_time": pos["entry_time"],
                    "size_usdt": pos["size_usdt"],
                    "score": pos["score"],
                    "side": side,
                    "mfe": pos.get("mfe", 0.0),
                    "mae": pos.get("mae", 0.0),
                    "sl_pct": pos.get("sl_pct", 0.0),
                    "tp_pct": pos.get("tp1_pct", 0.0),
                    "entry_order_id": pos.get("entry_order_id"),
                    "exit_order_id": None,
                    "sl_order_id": pos.get("sl_order_id"),
                    "tp_order_id": pos.get("tp_order_id"),
                    "sl_client_id": pos.get("sl_client_id"),
                    "tp_client_id": pos.get("tp_client_id"),
                    "client_order_id": pos.get("client_order_id"),
                    "sl_price": pos.get("sl_price"),
                    "tp1_price": pos.get("tp1_price"),
                    "tp2_price": pos.get("tp2_price"),
                }
                
                debug_log(f"[DEBUG-TRACKER] {symbol}: event сформирован, возвращаем из _close_position")
                debug_log(f"[DEBUG-TRACKER] {symbol}: returning event with reason={reason}, pnl={pnl:+.2f}$")
                return event

            
            
            # Закрываем через exchange
            close_order = await self.exchange.close_position(symbol, final_qty, exit_price)
            if not close_order or close_order.get("filled_amount", 0) <= 0:
                log.error(f"{symbol}: не удалось закрыть позицию {reason}")
                return None
            
            actual_qty = min(
                close_order.get("filled_amount", 0),
                final_qty,
            )
            
            # Считаем PnL
            side = pos.get("side", "LONG")
            pnl = self._calc_pnl(pos["entry_price"], exit_price, actual_qty, side)
            
            pos["realized_pnl"] += pnl
            pos["closed_qty"] += actual_qty
            pos["remaining_qty"] -= actual_qty
            
            # Если закрыта только часть — не удаляем позицию
            if pos["remaining_qty"] > 1e-12:
                log.warning(
                    f"{symbol}: закрытие {reason} частично: "
                    f"filled={actual_qty:.6f}, remaining={pos['remaining_qty']:.6f}"
                )
                return None

            # [ИСПРАВЛЕНО] Отменяем защитные ордера после закрытия позиции
            sl_id = pos.get("sl_order_id")
            tp_id = pos.get("tp_order_id")
            sl_cid = pos.get("sl_client_id")
            tp_cid = pos.get("tp_client_id")

            if sl_id or tp_id:
                try:
                    cancel_result = await self.exchange.cancel_sl_tp(
                        symbol,
                        sl_order_id=sl_id,
                        tp_order_id=tp_id,
                        sl_client_id=sl_cid,
                        tp_client_id=tp_cid,
                    )
                    log.info(
                        f"{symbol}: защитные ордера отменены после закрытия "
                        f"(SL={'✓' if cancel_result.get('sl') else '✗'}, "
                        f"TP={'✓' if cancel_result.get('tp') else '✗'})"
                    )
                except Exception as e:
                    log.error(f"{symbol}: ошибка отмены ордеров: {e}")
            
            # Позиция полностью закрыта
            self.positions.pop(symbol, None)

            # [НОВОЕ] Удаляем позицию из БД
            if self.db:
                try:
                    self.db.delete_open_position(symbol)
                    debug_log(f"[TRACKER] {symbol}: позиция удалена из БД (полное закрытие)")
                except Exception as e:
                    log.error(f"{symbol}: ошибка удаления позиции из БД: {e}")
            
            log.debug(
                f"TRACKER CLOSE {symbol} [{side}] @ {fmt_price(exit_price)} "
                f"PnL={pnl:+.2f}$ {reason}"
            )
            
            return {
                "symbol": symbol,
                "reason": reason,
                "price": exit_price,
                "qty": actual_qty,
                "pnl": pnl,
                "entry_price": pos["entry_price"],
                "entry_time": pos["entry_time"],
                "size_usdt": pos["size_usdt"],
                "score": pos["score"],
                "side": side,
                "mfe": pos.get("mfe", 0.0),
                "mae": pos.get("mae", 0.0),
                "sl_pct": pos.get("sl_pct", 0.0),
                "tp_pct": pos.get("tp1_pct", 0.0),
                # [НОВОЕ] Идентификаторы биржи
                "entry_order_id": pos.get("entry_order_id"),
                "exit_order_id": close_order.get("order_id") if close_order else None,
                "sl_order_id": pos.get("sl_order_id"),
                "tp_order_id": pos.get("tp_order_id"),
                "sl_client_id": pos.get("sl_client_id"),
                "tp_client_id": pos.get("tp_client_id"),
                "client_order_id": pos.get("client_order_id"),
                # [НОВОЕ] Реально выставленные уровни
                "sl_price": pos.get("sl_price"),
                "tp1_price": pos.get("tp1_price"),
                "tp2_price": pos.get("tp2_price"),
            }
            
        
        finally:
            if symbol in self.positions:
                self.positions[symbol].pop("closing", None)
    
    # ================================================================
    # ЧАСТИЧНОЕ ЗАКРЫТИЕ
    # ================================================================
    async def _execute_partial_close(
        self,
        symbol: str,
        qty: float,
        price: float,
        reason: str,
    ) -> float:
        """
        Частично закрывает позицию.
        Возвращает реально закрытое количество.
        """
        pos = self.positions.get(symbol)
        if not pos or qty <= 0:
            return 0.0
        
        if pos.get("closing"):
            return 0.0
        
        actual_qty = min(qty, pos["remaining_qty"])
        if actual_qty <= 0:
            return 0.0
        
        # Закрываем через exchange
        close_order = await self.exchange.close_position(symbol, actual_qty, price)
        
        if not close_order or close_order.get("filled_amount", 0) <= 0:
            log.error(f"{symbol}: частичное закрытие {reason} не исполнилось")
            return 0.0
        
        filled = min(close_order.get("filled_amount", 0), actual_qty)
        
        # Считаем PnL
        side = pos.get("side", "LONG")
        pnl = self._calc_pnl(pos["entry_price"], price, filled, side)
        
        pos["realized_pnl"] += pnl
        pos["closed_qty"] += filled
        pos["remaining_qty"] -= filled
        
        if pos["remaining_qty"] < 1e-12:
            pos["remaining_qty"] = 0.0
        
        log.info(
            f"{reason} PARTIAL {symbol} qty={filled:.6f} @ {fmt_price(price)} "
            f"pnl={pnl:+.2f}$ remaining={pos['remaining_qty']:.6f}"
        )

        # [НОВОЕ] После TP1 обновляем TP на бирже
        if reason == "TP1":
            # Отменяем старый TP
            old_tp_id = pos.get("tp_order_id")
            old_tp_cid = pos.get("tp_client_id")
            if old_tp_id:
                try:
                    await self.exchange.cancel_sl_tp(
                        symbol,
                        tp_order_id=old_tp_id,
                        tp_client_id=old_tp_cid,
                    )
                    log.info(f"{symbol}: старый TP отменён после TP1")
                except Exception as e:
                    log.error(f"{symbol}: ошибка отмены старого TP: {e}")

            # Ставим новый TP на остаток qty на уровне TP2
            remaining = pos["remaining_qty"]
            if remaining > 0:
                try:
                    new_tp = await self.exchange.place_tp(
                        symbol,
                        pos["tp2_price"],
                        remaining,
                    )
                    if new_tp:
                        pos["tp_order_id"] = new_tp.get("order_id")
                        pos["tp_client_id"] = new_tp.get("client_order_id")
                        log.info(
                            f"{symbol}: новый TP на TP2 ({pos['tp2_price']}) "
                            f"qty={remaining:.6f}, id={new_tp.get('order_id')}"
                        )
                    else:
                        log.error(
                            f"{symbol}: не удалось поставить новый TP после TP1! "
                            f"Позиция без TP-защиты на бирже."
                        )
                except Exception as e:
                    log.error(f"{symbol}: ошибка постановки нового TP: {e}")
        
        return filled
    
    # ================================================================
    # BREAKEVEN
    # ================================================================
    async def _set_breakeven(self, symbol: str, pos: dict):
        """Перемещает SL в безубыток."""
        side = pos.get("side", "LONG")
        is_short = side == "SHORT"
        
        if is_short:
            be_price = pos["entry_price"] * (1 - Config.BREAKEVEN_BUFFER_PCT / 100)
        else:
            be_price = pos["entry_price"] * (1 + Config.BREAKEVEN_BUFFER_PCT / 100)
        
        improves = (
            (is_short and be_price < pos["sl_price"])
            or (not is_short and be_price > pos["sl_price"])
        )
        
        if improves:
            pos["sl_price"] = be_price
            pos["breakeven_set"] = True
            log.info(f"Breakeven set {symbol} @ {fmt_price(be_price)}")
    
    # ================================================================
    # ТРЕЙЛИНГ
    # ================================================================
    def _calc_trailing_sl(
        self,
        profit_pct: float,
        side: str,
        current_price: float,
    ) -> float:
        """Считает новый Stop Loss для трейлинга."""
        step = self.trail_steps[0]
        for s in self.trail_steps:
            if profit_pct >= s:
                step = s
        
        offset_pct = Config.TRAILING_STEPS[step]
        
        if side == "LONG":
            return current_price * (1 - offset_pct / 100.0)
        return current_price * (1 + offset_pct / 100.0)
    
    # ================================================================
    # ВСПОМОГАТЕЛЬНЫЕ
    # ================================================================
    def _calc_profit_pct(self, pos: dict, price: float) -> float:
        """Считает текущую прибыль в процентах."""
        side = pos.get("side", "LONG")
        if side == "SHORT":
            return (pos["entry_price"] - price) / pos["entry_price"] * 100.0
        return (price - pos["entry_price"]) / pos["entry_price"] * 100.0
    
    @staticmethod
    def _calc_pnl(
        entry_price: float,
        exit_price: float,
        qty: float,
        side: str = "LONG",
    ) -> float:
        """
        Считает PnL для закрытой части позиции.
        Учитывает приблизительную комиссию 0.04% на вход и выход.
        """
        entry_notional = qty * entry_price
        exit_notional = qty * exit_price
        fee_entry = entry_notional * 0.0004
        fee_exit = exit_notional * 0.0004
        
        if side == "SHORT":
            return entry_notional - exit_notional - fee_entry - fee_exit
        return exit_notional - entry_notional - fee_entry - fee_exit
    
    # ================================================================
    # ПУБЛИЧНЫЕ МЕТОДЫ
    # ================================================================
    def get_open_positions(self) -> List[dict]:
        """Возвращает список открытых позиций."""
        return list(self.positions.values())
    
    def get_position(self, symbol: str) -> Optional[dict]:
        """Возвращает позицию по символу или None."""
        return self.positions.get(symbol)

    # [НОВОЕ] Метод для загрузки позиций из БД при старте
    def load_positions_from_db(self):
        """
        Загружает открытые позиции из БД в self.positions.
        Используется при рестарте бота для восстановления состояния.
        [ИСПРАВЛЕНО] Восстанавливает недостающие runtime-поля.
        """
        if not self.db:
            log.warning("[TRACKER] load_positions_from_db: db=None, пропускаю")
            return

        try:
            db_positions = self.db.load_all_open_positions()
            if not db_positions:
                log.info("[TRACKER] load_positions_from_db: в БД нет открытых позиций")
                return

            for pos in db_positions:
                symbol = pos.get("symbol")
                if not symbol:
                    continue

                # [ИСПРАВЛЕНО] Восстанавливаем runtime-поля, которых нет в БД
                if not pos.get("highest"):
                    pos["highest"] = pos.get("entry_price", 0.0)
                if pos.get("mfe") is None:
                    pos["mfe"] = 0.0
                if pos.get("mae") is None:
                    pos["mae"] = 0.0
                if not pos.get("last_watch_price"):
                    pos["last_watch_price"] = pos.get("entry_price", 0.0)
                pos["closing"] = False
                pos.setdefault("trail_active", False)
                pos.setdefault("breakeven_set", False)
                pos.setdefault("tp1_done", False)
                pos.setdefault("tp2_done", False)
                pos.setdefault("tp1_closed_qty", 0.0)
                pos.setdefault("realized_pnl", 0.0)
                pos.setdefault("closed_qty", 0.0)

                # Восстанавливаем позицию в self.positions
                self.positions[symbol] = pos
                log.info(
                    f"[TRACKER] Восстановлена позиция из БД: {symbol} "
                    f"[{pos.get('side', 'LONG')}] "
                    f"qty={pos.get('quantity', 0):.6f} "
                    f"SL={fmt_price(pos.get('sl_price', 0))}"
                )

            log.info(f"[TRACKER] Восстановлено {len(db_positions)} позиций из БД")

        except Exception as e:
            log.error(f"[TRACKER] Ошибка загрузки позиций из БД: {e}")


    async def _check_volume_decay(self, symbol: str) -> bool:
        """Проверяет угасание объёма."""
        w = Config.VOL_DECAY_WINDOW_MIN
        p = Config.VOL_DECAY_PRIOR_WINDOW_MIN
        limit = w + p + 1
        try:
            klines = await self.exchange.get_klines(symbol, "1m", limit)
        except Exception:
            return False
        if not klines or len(klines) < limit:
            return False
        volumes = [float(k[1]) for k in klines]
        recent = volumes[-w:]
        prior = volumes[-(w + p):-w]
        if not prior:
            return False
        prior_avg = sum(prior) / len(prior)
        if prior_avg <= 0:
            return False
        recent_avg = sum(recent) / len(recent)
        ratio = recent_avg / prior_avg
        if ratio < Config.VOL_DECAY_RATIO:
            log.info(
                f"{symbol}: объём угас "
                f"(recent/prior={ratio:.2f} < {Config.VOL_DECAY_RATIO}) "
                f"за последние {w} мин — закрываю по VOL_DECAY"
            )
            return True
        return False

    async def handle_order_update(self, symbol: str, order_id: str, client_order_id: str, status: str, filled_qty: float):
        """
        [НОВОЕ]
        Обрабатывает обновление ордера из User Data Stream.
        Вызывается напрямую из ws_client при получении ORDER_TRADE_UPDATE.
        """
        pos = self.positions.get(symbol)
        if not pos:
            log.debug(f"[TRACKER] {symbol}: позиция не найдена для обработки обновления ордера")
            return
        
        # Проверяем, является ли это SL/TP ордером
        is_sl = (pos.get("sl_order_id") == order_id or 
                pos.get("sl_client_id") == client_order_id)
        is_tp = (pos.get("tp_order_id") == order_id or 
                pos.get("tp_client_id") == client_order_id)
        
        # Обработка статусов
        if status == "FILLED":
            log.info(f"{symbol}: ордер {order_id} полностью исполнен")
            
            if is_sl:
                log.info(f"{symbol}: SL сработал, закрываем позицию")
                # Вызываем обработку SL
                await self._close_position(symbol, pos["sl_price"], "SL")
            
            elif is_tp:
                log.info(f"{symbol}: TP сработал")
                # Проверяем, какой TP (TP1 или TP2)
                if pos.get("tp1_done", False):
                    # Это TP2
                    await self._close_position(symbol, pos["tp2_price"], "TP2")
                else:
                    # Это TP1
                    await self._handle_tp1_fill(symbol, filled_qty)
        
        elif status == "CANCELED":
            log.info(f"{symbol}: ордер {order_id} отменен")
            
            # Если это был SL, возможно, нужно восстановить
            if is_sl:
                log.warning(f"{symbol}: SL отменен, запускаем аварийную ветку")
                await self._emergency_restore_sl(symbol, pos)
            
            # Если это был TP, возможно, нужно восстановить
            elif is_tp:
                log.warning(f"{symbol}: TP отменен, запускаем восстановление")
                # Здесь можно добавить логику восстановления TP