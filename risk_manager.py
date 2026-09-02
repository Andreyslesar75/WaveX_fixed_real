#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# ФАЙЛ: risk_manager.py
# СОХРАНИТЬ КАК: risk_manager.py

Управляет принятием решений (score, кулдауны, лимиты) и делегирует 
исполнение и мониторинг позиций в PositionTracker через ExchangeAdapter.
"""
import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from api import BinanceFuturesRestClient, to_binance_symbol
from calculations import calc_sl_tp, calc_sl_tp_short, calculate_position_size, size_is_valid
from config import Config
from database import Database
from logger import log, fmt_price, play_sound, debug_log

# Импортируем новые компоненты
from exchange_adapter import PaperExchange, RealExchange
from position_tracker import PositionTracker

# Импортируем старый PositionManager как RealPositionManager для real-режима
from position_manager import PositionManager as RealPositionManager



class PositionManager:
    """
    Менеджер рисков и позиций.
    Отвечает за:
    - проверку лимитов и кулдаунов;
    - расчёт размера позиции и уровней SL/TP;
    - статистику и запись в БД.
    
    Делегирует исполнение и мониторинг в PositionTracker.
    """

    def __init__(self, rest_client: BinanceFuturesRestClient, is_real: bool):
        self.rest = rest_client
        self.is_real = is_real

        # 1. Создаём адаптер биржи
        if is_real:
            real_pm = RealPositionManager(rest_client)
            self.exchange = RealExchange(rest_client, real_pm)
        else:
            self.exchange = PaperExchange()

        # 2. Создаём трекер позиций
        self.tracker = PositionTracker(self.exchange)

        # 3. Статистика и капитал
        self.capital = Config.PAPER_BALANCE if not is_real else 0.0
        self.total_pnl = 0.0
        self.total_trades = 0
        self.wins = 0
        self.losses = 0
        self.breakevens = 0

        # 4. Кулдауны и блокировки
        self._stop_history: Dict[str, List[float]] = {}
        self._cooldown_until: Dict[str, float] = {}
        self._repeat_block_until: Dict[str, float] = {}
        self._gap_block_until: Dict[str, float] = {}

        # 5. База данных и lock
        self.db = Database()
        self._update_lock = asyncio.Lock()

        # [НОВОЕ] Менеджер реальных защитных ордеров (SL/TP на бирже).
        # Используется только в real-режиме.
        if self.is_real:
            self.real_pm = RealPositionManager(rest_client)
        else:
            self.real_pm = None

        # 6. Дневная статистика
        self._consecutive_losses = 0
        self._daily_pnl = 0.0
        self._today_date = datetime.now().date()
        self._trades_today = 0
        self._daily_loss_limit = -abs(float(Config.DAILY_MAX_LOSS_USDT))


    # ================================================================
    # БАЗОВЫЕ ФУНКЦИИ
    # ================================================================
    async def refresh_balance(self):
        if self.is_real:
            self.capital = await self.exchange.get_balance("USDT")
            log.debug(f"Баланс обновлён: {self.capital:.2f} USDT")

    def close(self):
        if self.db is not None:
            self.db.close()

    @staticmethod
    def get_adaptive_threshold(side: str, btc_trend: float) -> float:
        if side == "SHORT":
            base = Config.SCORE_TRADE_THRESHOLD_SHORT
            if btc_trend < -2.0:
                return base - 3
            elif btc_trend > 2.0:
                return base + 3
            return base
        return Config.SCORE_TRADE_THRESHOLD

    def _check_daily_limit(self) -> Tuple[bool, str]:
        today = datetime.now().date()
        if today != self._today_date:
            self._daily_pnl = 0.0
            self._trades_today = 0
            self._today_date = today

        if Config.MAX_TRADES_PER_DAY > 0:
            if self._trades_today >= Config.MAX_TRADES_PER_DAY:
                return False, f"max_trades_per_day {self._trades_today}/{Config.MAX_TRADES_PER_DAY}"

        if self._daily_pnl <= self._daily_loss_limit:
            return False, f"daily_loss_limit {self._daily_pnl:.2f} <= {self._daily_loss_limit:.1f}"

        return True, ""

    def _calculate_adaptive_size(self, base_size: float) -> float:
        if base_size <= 0:
            return 0.0
        if self._consecutive_losses >= 3:
            base_size *= 0.5
        elif self._consecutive_losses >= 2:
            base_size *= 0.7
        if self.total_pnl < -50:
            base_size *= 0.6
        elif self.total_pnl < -30:
            base_size *= 0.8
        return max(0.0, base_size)

    # ================================================================
    # ОТКРЫТИЕ ПОЗИЦИИ
    # ================================================================
    async def open_position(
        self,
        symbol: str,
        price: float,
        score: float,
        confidence: str,
        klines_1h: list,
        high24: float,
        low24: float,
        structural_level: Optional[float] = None,
        spread_pct: float = 0.0,
        side: str = "LONG",
        btc_trend: float = 0.0,
    ) -> Tuple[bool, str]:
        if len(self.tracker.get_open_positions()) >= Config.MAX_OPEN_POSITIONS:
            return False, "max_positions"
        if symbol in self.tracker.positions:
            return False, "already_open"
        if side == "SHORT" and not Config.SHORT_TRADING_ENABLED:
            return False, "short_disabled"
        if confidence not in ("HIGH", "MEDIUM"):
            return False, f"low_conf_{confidence}"

        limit_ok, limit_reason = self._check_daily_limit()
        if not limit_ok:
            return False, limit_reason

        threshold = self.get_adaptive_threshold(side, btc_trend)
        if score < threshold:
            return False, f"score_{score:.0f}<{threshold:.0f}"

        now = time.time()
        if symbol in self._repeat_block_until and self._repeat_block_until[symbol] > now:
            return False, f"repeat_block_{int(self._repeat_block_until[symbol] - now)}s"
        if symbol in self._gap_block_until and self._gap_block_until[symbol] > now:
            return False, f"gap_block_{int(self._gap_block_until[symbol] - now)}s"
        if symbol in self._cooldown_until and self._cooldown_until[symbol] > now:
            if score < Config.STRONG_SCORE_FOR_REENTRY:
                return False, f"blocked_until_{int(self._cooldown_until[symbol] - now)}s"
            log.info(f"{symbol}: сильный сигнал (score={score:.0f}) -> игнорируем базовый кулдаун")

        if self.is_real:
            await self.refresh_balance()
        bal = self.capital

        if side == "SHORT":
            sl_price, sl_pct, tp1_price, tp1_pct, tp2_price, tp2_pct, sl_source = calc_sl_tp_short(
                price, klines_1h, structural_level, spread_pct, high24, low24
            )
        else:
            sl_price, sl_pct, tp1_price, tp1_pct, tp2_price, tp2_pct, sl_source = calc_sl_tp(
                price, klines_1h, structural_level, spread_pct, high24, low24
            )

        # Защита от некорректных SL/TP
        if side == "SHORT":
            if sl_price <= price:
                sl_pct = Config.ATR_SL_MIN_PCT
                sl_price = price * (1 + sl_pct / 100)
            if tp1_price >= price:
                tp1_pct = sl_pct * Config.FIRST_TP_MULTIPLIER
                tp1_price = price * (1 - tp1_pct / 100)
                tp2_pct = sl_pct * Config.SECOND_TP_MULTIPLIER
                tp2_price = price * (1 - tp2_pct / 100)
        else:
            if sl_price >= price:
                sl_pct = Config.ATR_SL_MIN_PCT
                sl_price = price * (1 - sl_pct / 100)
            if tp1_price <= price:
                tp1_pct = sl_pct * Config.FIRST_TP_MULTIPLIER
                tp1_price = price * (1 + tp1_pct / 100)
                tp2_pct = sl_pct * Config.SECOND_TP_MULTIPLIER
                tp2_price = price * (1 + tp2_pct / 100)

        # ------------------------------------------------------------
        # Размер позиции
        # ------------------------------------------------------------
        base_size = calculate_position_size(
            self.capital,
            sl_pct,
            Config.RISK_PER_TRADE_PCT,
            Config.MAX_POSITION_PCT,
        )
        size = self._calculate_adaptive_size(base_size)
        
        # [НОВОЕ]
        # Если размер слишком маленький, но включён флаг OVERRIDE
        # и баланс позволяет — используем MIN_POSITION_SIZE_USDT.
        # Это нужно для тестирования на малых балансах.
        if (
            size < Config.MIN_POSITION_SIZE_USDT
            and Config.ALLOW_MIN_POSITION_OVERRIDE
            and self.capital >= Config.MIN_POSITION_SIZE_USDT * 1.1  # запас на комиссию
        ):
            log.info(
                f"{symbol}: размер {size:.2f} USDT < минимума, "
                f"но ALLOW_MIN_POSITION_OVERRIDE=True — "
                f"использую {Config.MIN_POSITION_SIZE_USDT:.2f} USDT"
            )
            size = Config.MIN_POSITION_SIZE_USDT
        
        bsym = to_binance_symbol(symbol)
        info = await self.rest._get_symbol_info(bsym)
        min_notional = info.get("minNotional", 5.0)
        
        # Если размер слишком маленький, сделка отклоняется.
        if not size_is_valid(size, min_notional):
            log.warning(
                f"{symbol}: размер {size:.2f} USDT "
                f"меньше допустимого минимума, отказ"
            )
            return False, (
                f"size_too_small "
                f"({size:.2f} < min {max(min_notional, Config.MIN_POSITION_SIZE_USDT):.2f})"
            )

        if self.is_real and bal < size:
            log.warning(f"{symbol}: баланс {bal:.2f} USDT < требуемого размера {size:.2f} USDT, отказ")
            return False, "insufficient_balance"

        # Конвертируем размер в USDT в количество монет для tracker
        qty = size / price if price > 0 else 0.0

        # Делегируем открытие в PositionTracker
        success = await self.tracker.open_position(
            symbol=symbol,
            side=side,
            entry_price=price,
            qty=qty,
            sl_price=sl_price,
            tp1_price=tp1_price,
            tp2_price=tp2_price,
            sl_pct=sl_pct,
            tp1_pct=tp1_pct,
            tp2_pct=tp2_pct,
            size_usdt=size,
            score=score,
            confidence=confidence,
            sl_source=sl_source,
        )

        if success:
            if not self.is_real:
                self.capital -= size
            play_sound("open")
            # [НОВОЕ] Записываем equity по событию открытия
            self.log_equity_event("open")
            return True, sl_source

        return False, "tracker_open_failed"

    # ================================================================
    # ОБНОВЛЕНИЕ ПОЗИЦИЙ И ОБРАБОТКА СОБЫТИЙ
    # ================================================================
    async def update_positions(self, prices: Dict[str, float]):
        async with self._update_lock:
            events = await self.tracker.update_prices(prices)
            debug_log(f"[DEBUG-RISK] update_positions: received {len(events)} events")
            # [НОВОЕ] Проверяем, изменился ли SL у открытых позиций
            sl_changed = False
            for symbol, pos in self.tracker.positions.items():
                if pos.get("_sl_changed"):
                    sl_changed = True
                    pos["_sl_changed"] = False

            for event in events:
                debug_log(f"[DEBUG-RISK] processing event: {event.get('symbol')} reason={event.get('reason')}")
                await self._handle_position_event(event)

            # [НОВОЕ] Если SL изменился (трейлинг/breakeven), записываем equity
            if sl_changed:
                self.log_equity_event("sl_change")

    async def _handle_position_event(self, event: dict):
        # [НОВОЕ] Отладочный лог
        debug_log(
            f"[DEBUG] _handle_position_event вызван: "
            f"{event.get('symbol')} reason={event.get('reason')} "
            f"pnl={event.get('pnl', 0):+.2f}$"
        )

        symbol = event["symbol"]
        reason = event["reason"]
        pnl = event["pnl"]
        qty = event["qty"]
        exit_price = event["price"]
        entry_price = event.get("entry_price", 0.0)
        entry_time = event.get("entry_time", time.time())
        size_usdt = event.get("size_usdt", 0.0)
        score = event.get("score", 0.0)
        side = event.get("side", "LONG")
        mfe = event.get("mfe", 0.0)
        mae = event.get("mae", 0.0)
        sl_pct = event.get("sl_pct", 0.0)
        tp_pct = event.get("tp_pct", 0.0)

        self.total_pnl += pnl
        self._daily_pnl += pnl
        self.total_trades += 1
        self._trades_today += 1

        if pnl > 0:
            self.wins += 1
            self._consecutive_losses = 0
            if reason == "TP2":
                self._cooldown_until.pop(symbol, None)
                self._stop_history.pop(symbol, None)
                self._repeat_block_until.pop(symbol, None)
        elif pnl < 0:
            self.losses += 1
            self._consecutive_losses += 1
            if reason == "SL":
                base_cd = time.time() + Config.SL_COOLDOWN_HOURS * 3600
                self._cooldown_until[symbol] = max(self._cooldown_until.get(symbol, 0), base_cd)
                self._stop_history.setdefault(symbol, []).append(time.time())
                recent = [t for t in self._stop_history[symbol] if t > time.time() - 86400]
                if len(recent) >= Config.REPEAT_STOP_LIMIT:
                    extended_cd = time.time() + Config.REPEAT_BLOCK_HOURS * 3600
                    self._repeat_block_until[symbol] = max(self._repeat_block_until.get(symbol, 0), extended_cd)
        else:
            self.breakevens += 1
            self._consecutive_losses = 0

        if reason == "GAP_SL":
            gap_cd = time.time() + Config.GAP_BLOCK_HOURS * 3600
            self._gap_block_until[symbol] = max(self._gap_block_until.get(symbol, 0), gap_cd)

        pnl_pct = (pnl / size_usdt * 100) if size_usdt > 0 else 0.0
        
        # Запись в БД
        self.db.log_trade({
            "timestamp": datetime.now().isoformat(),
            "symbol": symbol,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "size_usdt": size_usdt,
            "qty": qty,
            "pnl_pct": pnl_pct,
            "pnl_usdt": pnl,
            "exit_reason": reason,
            "entry_time": datetime.fromtimestamp(entry_time).isoformat() if isinstance(entry_time, (int, float)) else entry_time,
            "exit_time": datetime.now().isoformat(),
            "score": score,
            "sl_pct": sl_pct,
            "tp_pct": tp_pct,
            "side": side,
            "mfe": mfe,
            "mae": mae,
            # [НОВОЕ] Идентификаторы биржи
            "entry_order_id": event.get("entry_order_id"),
            "exit_order_id": event.get("exit_order_id"),
            "sl_order_id": event.get("sl_order_id"),
            "tp_order_id": event.get("tp_order_id"),
            "sl_client_id": event.get("sl_client_id"),
            "tp_client_id": event.get("tp_client_id"),
            "client_order_id": event.get("client_order_id"),
            # [НОВОЕ] Реально выставленные уровни SL/TP
            "sl_price": event.get("sl_price"),
            "tp1_price": event.get("tp1_price"),
            "tp2_price": event.get("tp2_price"),
        })

        # [НОВОЕ] Записываем equity по событию закрытия
        event_type = "tp1" if reason == "TP1" else "close"
        self.log_equity_event(event_type)

        log.info(
            f"[DEBUG] Сделка записана в БД: {symbol} "
            f"pnl={pnl:+.2f}$ reason={reason}"
        )

        if self.is_real:
            await self.refresh_balance()

        log.info(f"CLOSE {symbol} [{side}] @ {fmt_price(exit_price)} PnL={pnl:+.2f}$ ({pnl_pct:+.2f}%) {reason}")

        # [НОВОЕ] Записываем equity по событию закрытия
        event_type = "tp1" if reason == "TP1" else "close"
        self.log_equity_event(event_type)

    # ================================================================
    # СВОЙСТВА ДЛЯ ОБРАТНОЙ СОВМЕСТИМОСТИ
    # ================================================================
    @property
    def positions(self) -> Dict[str, dict]:
        """
        Возвращает открытые позиции из PositionTracker.
        Это свойство нужно, чтобы scanner.py и gui.py могли
        обращаться к pm.positions, как раньше.
        """
        return self.tracker.positions

    # ================================================================
    # СТАТИСТИКА И ГЕТТЕРЫ
    # ================================================================
    def get_stats(self) -> str:
        wr = (self.wins / self.total_trades * 100) if self.total_trades else 0
        return (
            f"Сделок: {self.total_trades} | "
            f"Win: {self.wins} | "
            f"Loss: {self.losses} | "
            f"BE: {self.breakevens} | "
            f"WR: {wr:.0f}% | "
            f"PnL: ${self.total_pnl:+.2f} | "
            f"Капитал: ${self.capital:.2f} | "
            f"Открыто: {len(self.tracker.get_open_positions())}/{Config.MAX_OPEN_POSITIONS}"
        )

    def get_open_positions(self) -> List[dict]:
        return self.tracker.get_open_positions()

    def log_equity_event(self, event_type: str = "periodic"):
        """
        Записывает equity по событию.
        event_type: 'open', 'close', 'tp1', 'sl_change', 'periodic'
        """
        if self.db is not None:
            self.db.log_equity(
                self.capital,
                self.total_pnl,
                len(self.tracker.get_open_positions()),
                event_type=event_type,
            )

    def log_equity(self, event_type: str = "periodic"):
        """Записывает снимок капитала (для обратной совместимости)."""
        self.log_equity_event(event_type)

    def get_trades(self, limit: int = 100):
        return self.db.get_trades(limit)

    def get_win_rate(self):
        return self.db.win_rate()

    def get_max_drawdown(self):
        return self.db.max_drawdown()