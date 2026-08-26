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
"""
import time
from typing import Dict, List, Optional, Any
from config import Config
from exchange_adapter import ExchangeAdapter
from logger import log, fmt_price


class PositionTracker:
    """
    Управляет открытыми позициями.
    Работает через ExchangeAdapter.
    """
    
    def __init__(self, exchange: ExchangeAdapter):
        self.exchange = exchange
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
                symbol, qty, price=entry_price, sl_price=sl_price, tp_price=tp1_price
            )
        else:
            order = await self.exchange.place_market_sell(
                symbol, qty, price=entry_price, sl_price=sl_price, tp_price=tp1_price
            )
        
        if not order or order.get("filled_amount", 0) <= 0:
            log.error(f"{symbol}: не удалось открыть позицию через exchange")
            return False
        
        # Используем реальные значения из ответа exchange
        actual_qty = order.get("filled_amount", qty)
        actual_price = order.get("avg_price", entry_price)
        if actual_price <= 0:
            actual_price = entry_price
        
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
        }

        
        
        self.positions[symbol] = pos
        
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
            
            # Проверяем условия закрытия
            event = await self._check_conditions(symbol, pos, price, now)
            if event:
                events.append(event)
            
            # Обновляем последнюю цену
            if symbol in self.positions:
                self.positions[symbol]["last_watch_price"] = price
        
        return events
    
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
            target_tp1_qty = pos["quantity"] * Config.TP1_SIZE_FRAC
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
            return None
        
        if pos.get("closing"):
            return None
        
        final_qty = pos["remaining_qty"]
        if final_qty <= 0:
            self.positions.pop(symbol, None)
            return None
        
        pos["closing"] = True

        try:
            # [НОВОЕ] Отменяем защитные ордера на бирже ПЕРЕД закрытием
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
                        f"{symbol}: защитные ордера отменены "
                        f"(SL={'✓' if cancel_result.get('sl') else '✗'}, "
                        f"TP={'✓' if cancel_result.get('tp') else '✗'})"
                    )
                except Exception as e:
                    log.error(f"{symbol}: ошибка отмены защитных ордеров: {e}")

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
            
            # Позиция полностью закрыта
            self.positions.pop(symbol, None)
            
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