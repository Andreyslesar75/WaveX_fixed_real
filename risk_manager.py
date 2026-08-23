#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ФАЙЛ: risk_manager.py
# СОХРАНИТЬ КАК: risk_manager.py

Этот файл управляет открытыми позициями:
- открытие LONG/SHORT;
- расчёт размера позиции;
- TP1 / TP2;
- Stop Loss;
- Breakeven;
- Trailing;
- cooldown / repeat block / gap block;
- дневной лимит убытков;
- статистика сделок.

ЧТО ИСПРАВЛЕНО:
1. TP1 больше не помечается как выполненный, если ордер не исполнился.
2. Частичное исполнение TP1 теперь учитывается через tp1_closed_qty.
3. Полное закрытие позиции больше не удаляет позицию, если ордер
   исполнился только частично.
4. Размер позиции больше не увеличивается принудительно до 5 USDT.
5. Дневной лимит убытков берётся из config.py.
6. Добавлен лимит количества сделок за день.
7. SHORT-порог теперь согласован: в нейтральном рынке нет скрытого +2.
8. Сделки с нулевым PnL больше не считаются убытками.
9. Добавлен флаг closing, чтобы не пытаться закрыть позицию
   несколько раз одновременно.
"""

import asyncio
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from api import BinanceFuturesRestClient, to_binance_symbol
from calculations import (
    calc_sl_tp,
    calc_sl_tp_short,
    calculate_position_size,
    size_is_valid,
)
from config import Config
from database import Database
from logger import log, fmt_price, play_sound


class PositionManager:
    """
    Менеджер позиций.

    Отвечает за:
    - открытие позиций;
    - закрытие позиций;
    - TP1/TP2;
    - SL;
    - breakeven;
    - trailing;
    - статистику.
    """

    def __init__(self, rest_client: BinanceFuturesRestClient, is_real: bool):
        self.rest = rest_client
        self.is_real = is_real

        # Открытые позиции.
        # Ключ — символ, значение — словарь позиции.
        self.positions: Dict[str, dict] = {}

        # Капитал.
        # Для paper mode берём PAPER_BALANCE из config.py.
        # Для real mode баланс будет обновлён с Binance.
        self.capital = Config.PAPER_BALANCE if not is_real else 0.0

        # Общая статистика.
        self.total_pnl = 0.0
        self.total_trades = 0
        self.wins = 0
        self.losses = 0

        # [НОВОЕ]
        # Сделки, которые закрылись примерно в ноль.
        # Раньше они попадали в losses, что искажало win rate.
        self.breakevens = 0

        # Кулдауны после стоп-лоссов.
        self._stop_history: Dict[str, List[float]] = {}
        self._cooldown_until: Dict[str, float] = {}

        # Блокировка после серии стопов.
        # Не обходится даже сильным сигналом.
        self._repeat_block_until: Dict[str, float] = {}

        # Блокировка после аномального гэпа.
        # Не обходится даже сильным сигналом.
        self._gap_block_until: Dict[str, float] = {}

        # База данных для записи сделок и equity.
        self.db = Database()

        # Lock нужен, чтобы обновление позиций не пересекалось
        # между scanner и position_watcher.
        self._update_lock = asyncio.Lock()

        # Шаги трейлинга из config.py.
        self.trail_steps = sorted(Config.TRAILING_STEPS.keys())

        # Серия убытков подряд.
        self._consecutive_losses = 0

        # Дневная статистика.
        self._daily_pnl = 0.0
        self._today_date = datetime.now().date()

        # [НОВОЕ]
        # Количество закрытых сделок за сегодня.
        self._trades_today = 0

        # [ИСПРАВЛЕНО]
        # Дневной лимит убытков теперь берётся из config.py.
        self._daily_loss_limit = -abs(float(Config.DAILY_MAX_LOSS_USDT))

    # ================================================================
    # БАЗОВЫЕ ФУНКЦИИ
    # ================================================================

    async def refresh_balance(self):
        """
        Обновляет баланс USDT с Binance.
        Используется только в real mode.
        """
        if self.is_real:
            self.capital = await self.rest.get_balance("USDT")
            log.debug(f"Баланс обновлён: {self.capital:.2f} USDT")

    def close(self):
        """
        Закрывает базу данных.

        Вызывается при остановке бота.
        """
        if self.db is not None:
            self.db.close()

    @staticmethod
    def get_adaptive_threshold(side: str, btc_trend: float) -> float:
        """
        Возвращает порог score для открытия позиции.

        [ИСПРАВЛЕНО]
        Раньше для SHORT в нейтральном рынке возвращалось:
            base + 2

        Из-за этого SHORT-сигналы со score 53-54 могли проходить
        в scanner.py, но потом отклонялись здесь.

        Теперь в нейтральном рынке SHORT-порог равен base,
        то есть Config.SCORE_TRADE_THRESHOLD_SHORT.
        """
        if side == "SHORT":
            base = Config.SCORE_TRADE_THRESHOLD_SHORT

            if btc_trend < -2.0:
                return base - 3

            elif btc_trend > 2.0:
                return base + 3

            return base

        return Config.SCORE_TRADE_THRESHOLD

    def _check_daily_limit(self) -> Tuple[bool, str]:
        """
        Проверяет дневные лимиты:
        1. лимит убытков;
        2. лимит количества сделок.

        Если лимит нарушен, новые сделки не открываются.
        """
        today = datetime.now().date()

        # Если начался новый день, сбрасываем дневные счётчики.
        if today != self._today_date:
            self._daily_pnl = 0.0
            self._trades_today = 0
            self._today_date = today

        # [НОВОЕ]
        # Лимит количества сделок за день.
        if Config.MAX_TRADES_PER_DAY > 0:
            if self._trades_today >= Config.MAX_TRADES_PER_DAY:
                return False, (
                    f"max_trades_per_day "
                    f"{self._trades_today}/{Config.MAX_TRADES_PER_DAY}"
                )

        # Лимит убытков за день.
        if self._daily_pnl <= self._daily_loss_limit:
            return False, (
                f"daily_loss_limit "
                f"{self._daily_pnl:.2f} <= {self._daily_loss_limit:.1f}"
            )

        return True, ""

    def _calculate_adaptive_size(self, base_size: float) -> float:
        """
        Адаптивно уменьшает размер позиции после убытков.

        [ИСПРАВЛЕНО]
        Раньше здесь было:
            return max(5.0, base_size)

        Это могло перечеркнуть уменьшение риска.

        Теперь, если расчётный размер стал маленьким,
        он остаётся маленьким.

        Проверка минимального размера делается отдельно
        через size_is_valid().
        """
        if base_size <= 0:
            return 0.0

        # Если есть серия убытков, уменьшаем размер.
        if self._consecutive_losses >= 3:
            base_size *= 0.5

        elif self._consecutive_losses >= 2:
            base_size *= 0.7

        # Если общий PnL ушёл в минус, тоже уменьшаем размер.
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
        """
        Открывает позицию.

        Возвращает:
        (True, причина) — если позиция открыта.
        (False, причина) — если открытие отклонено.
        """

        # Максимум открытых позиций.
        if len(self.positions) >= Config.MAX_OPEN_POSITIONS:
            return False, "max_positions"

        # По одному символу только одна позиция.
        if symbol in self.positions:
            return False, "already_open"

        # [НОВОЕ]
        # Если SHORT выключен в config.py, не открываем SHORT.
        if side == "SHORT" and not Config.SHORT_TRADING_ENABLED:
            return False, "short_disabled"

        # Открываем только HIGH/MEDIUM уверенность.
        if confidence not in ("HIGH", "MEDIUM"):
            return False, f"low_conf_{confidence}"

        # Дневные лимиты.
        limit_ok, limit_reason = self._check_daily_limit()

        if not limit_ok:
            return False, limit_reason

        # Порог score.
        threshold = self.get_adaptive_threshold(side, btc_trend)

        if score < threshold:
            return False, f"score_{score:.0f}<{threshold:.0f}"

        now = time.time()

        # ------------------------------------------------------------
        # Блокировки символа
        # ------------------------------------------------------------

        # Блок после серии стопов.
        # Не обходится сильным сигналом.
        if symbol in self._repeat_block_until and self._repeat_block_until[symbol] > now:
            return False, (
                f"repeat_block_"
                f"{int(self._repeat_block_until[symbol] - now)}s"
            )

        # Блок после гэпа.
        # Не обходится сильным сигналом.
        if symbol in self._gap_block_until and self._gap_block_until[symbol] > now:
            return False, (
                f"gap_block_"
                f"{int(self._gap_block_until[symbol] - now)}s"
            )

        # Обычный кулдаун после SL.
        # Может обходиться очень сильным сигналом.
        if symbol in self._cooldown_until and self._cooldown_until[symbol] > now:
            if score < Config.STRONG_SCORE_FOR_REENTRY:
                return False, (
                    f"blocked_until_"
                    f"{int(self._cooldown_until[symbol] - now)}s"
                )

            log.info(
                f"{symbol}: сильный сигнал (score={score:.0f}) "
                f"-> игнорируем базовый кулдаун"
            )

        # ------------------------------------------------------------
        # Баланс
        # ------------------------------------------------------------

        if self.is_real:
            await self.refresh_balance()

        bal = self.capital

        # ------------------------------------------------------------
        # Расчёт SL/TP
        # ------------------------------------------------------------

        if side == "SHORT":
            (
                sl_price,
                sl_pct,
                tp1_price,
                tp1_pct,
                tp2_price,
                tp2_pct,
                sl_source,
            ) = calc_sl_tp_short(
                price,
                klines_1h,
                structural_level,
                spread_pct,
                high24,
                low24,
            )

        else:
            (
                sl_price,
                sl_pct,
                tp1_price,
                tp1_pct,
                tp2_price,
                tp2_pct,
                sl_source,
            ) = calc_sl_tp(
                price,
                klines_1h,
                structural_level,
                spread_pct,
                high24,
                low24,
            )

        # ------------------------------------------------------------
        # Защита от некорректных SL/TP
        # ------------------------------------------------------------

        if side == "SHORT":
            if sl_price <= price:
                sl_pct = Config.ATR_SL_MIN_PCT
                sl_price = price * (1 + sl_pct / 100)

                log.warning(
                    f"{symbol}: SL ({sl_price:.6f}) <= entry (SHORT), "
                    f"принудительно установлен {sl_pct}%"
                )

            if tp1_price >= price:
                tp1_pct = sl_pct * Config.FIRST_TP_MULTIPLIER
                tp1_price = price * (1 - tp1_pct / 100)

                tp2_pct = sl_pct * Config.SECOND_TP_MULTIPLIER
                tp2_price = price * (1 - tp2_pct / 100)

                log.warning(
                    f"{symbol}: TP1 ({tp1_price:.6f}) >= entry (SHORT), "
                    f"пересчитан"
                )

        else:
            if sl_price >= price:
                sl_pct = Config.ATR_SL_MIN_PCT
                sl_price = price * (1 - sl_pct / 100)

                log.warning(
                    f"{symbol}: SL ({sl_price:.6f}) >= entry, "
                    f"принудительно установлен {sl_pct}%"
                )

            if tp1_price <= price:
                tp1_pct = sl_pct * Config.FIRST_TP_MULTIPLIER
                tp1_price = price * (1 + tp1_pct / 100)

                tp2_pct = sl_pct * Config.SECOND_TP_MULTIPLIER
                tp2_price = price * (1 + tp2_pct / 100)

                log.warning(
                    f"{symbol}: TP1 ({tp1_price:.6f}) <= entry, "
                    f"пересчитан"
                )

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

        bsym = to_binance_symbol(symbol)

        info = await self.rest._get_symbol_info(bsym)

        min_notional = info.get("minNotional", 5.0)

        # [ИСПРАВЛЕНО]
        # Если размер слишком маленький, сделка отклоняется.
        # Раньше размер мог принудительно подниматься до 5 USDT.
        if not size_is_valid(size, min_notional):
            log.warning(
                f"{symbol}: размер {size:.2f} USDT "
                f"меньше допустимого минимума, отказ"
            )
            return False, (
                f"size_too_small "
                f"({size:.2f} < min {max(min_notional, Config.MIN_POSITION_SIZE_USDT):.2f})"
            )

        # ------------------------------------------------------------
        # Проверка баланса
        # ------------------------------------------------------------

        if self.is_real and bal < size:
            log.warning(
                f"{symbol}: баланс {bal:.2f} USDT "
                f"< требуемого размера {size:.2f} USDT, отказ"
            )
            return False, "insufficient_balance"

        # ------------------------------------------------------------
        # Отправка ордера
        # ------------------------------------------------------------

        if self.is_real:
            if side == "SHORT":
                order = await self.rest.place_market_sell_open(symbol, size)
            else:
                order = await self.rest.place_market_buy(symbol, size)

            if not order or order.get("status") not in ("filled", "partially_filled"):
                return False, "order_failed"

            executed_qty = float(order.get("filled_amount", 0))
            avg_price = float(order.get("avg_price", price))

            if executed_qty <= 0 or avg_price <= 0:
                log.error(
                    f"{symbol}: ордер исполнен некорректно "
                    f"(qty={executed_qty}, avg_price={avg_price})"
                )
                return False, "invalid_fill"

        else:
            avg_price = price
            executed_qty = size / avg_price

        size_usdt_actual = (executed_qty * avg_price) if self.is_real else size

        # ------------------------------------------------------------
        # Создание локальной позиции
        # ------------------------------------------------------------

        pos = {
            "symbol": symbol,
            "side": side,
            "entry_price": avg_price,
            "entry_time": now,
            "quantity": executed_qty,
            "remaining_qty": executed_qty,
            "sl_price": sl_price,
            "sl_pct": sl_pct,
            "tp1_price": tp1_price,
            "tp1_pct": tp1_pct,
            "tp2_price": tp2_price,
            "tp2_pct": tp2_pct,
            "score": score,
            "confidence": confidence,
            "size_usdt": size_usdt_actual,
            "highest": avg_price,
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

            # [НОВОЕ]
            # Сколько уже закрыто по TP1.
            "tp1_closed_qty": 0.0,

            # [НОВОЕ]
            # Флаг, что позиция сейчас в процессе закрытия.
            "closing": False,

            # [НОВОЕ]
            # Последняя цена, которую видел position watcher.
            "last_watch_price": avg_price,
        }

        self.positions[symbol] = pos

        # В paper mode сразу уменьшаем доступный капитал.
        if not self.is_real:
            self.capital -= size

        log.info(
            f"OPEN {symbol} [{side}] @ {fmt_price(avg_price)} "
            f"size=${size:.1f} "
            f"SL={fmt_price(sl_price)} ({sl_pct:.1f}%) "
            f"TP1={fmt_price(tp1_price)} ({tp1_pct:.1f}%, 60%) "
            f"TP2={fmt_price(tp2_price)} ({tp2_pct:.1f}%, 40%) "
            f"[{sl_source}]"
        )

        play_sound("open")

        return True, sl_source

    # ================================================================
    # РАСЧЁТ PNL
    # ================================================================

    @staticmethod
    def _close_pnl(
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

        [ИСПРАВЛЕНО]
        Теперь функция возвращает реально закрытое количество.

        Если ордер не исполнился, возвращает 0.0.
        Это нужно, чтобы TP1 не помечался как выполненный
        при неудачном ордере.
        """
        pos = self.positions.get(symbol)

        if not pos or qty <= 0:
            return 0.0

        # Если позиция уже в процессе закрытия, не трогаем её.
        if pos.get("closing"):
            return 0.0

        side = pos.get("side", "LONG")

        if self.is_real:
            if side == "SHORT":
                order = await self.rest.place_market_buy_close(symbol, qty)
            else:
                order = await self.rest.place_market_sell(symbol, qty)

            if not order or order.get("status") not in ("filled", "partially_filled"):
                log.error(
                    f"{symbol}: частичное закрытие {reason} "
                    f"не исполнилось, ордер {order}"
                )
                return 0.0

            actual_qty = float(order.get("filled_amount", 0))

            if actual_qty <= 0:
                log.error(
                    f"{symbol}: частичное закрытие {reason} исполнило 0"
                )
                return 0.0

            # Защита от некорректно большого filled_amount.
            actual_qty = min(actual_qty, qty, pos["remaining_qty"])

        else:
            actual_qty = min(qty, pos["remaining_qty"])

        if actual_qty <= 0:
            return 0.0

        pnl = self._close_pnl(pos["entry_price"], price, actual_qty, side)

        pos["realized_pnl"] += pnl
        pos["closed_qty"] += actual_qty
        pos["remaining_qty"] -= actual_qty

        # [ИСПРАВЛЕНО]
        # Частичная прибыль/убыток сразу попадают в дневную статистику.
        self.total_pnl += pnl
        self._daily_pnl += pnl

        # В paper mode возвращаем часть капитала.
        if not self.is_real:
            released = actual_qty * pos["entry_price"]
            self.capital += released + pnl

        if pos["remaining_qty"] < 1e-12:
            pos["remaining_qty"] = 0.0

        log.info(
            f"{reason} PARTIAL {symbol} qty={actual_qty:.6f} "
            f"@ {fmt_price(price)} pnl={pnl:+.2f}$ "
            f"remaining={pos['remaining_qty']:.6f}"
        )

        return actual_qty

    # ================================================================
    # ПРОЦЕНТ ОБЪЁМА
    # ================================================================

    async def _check_volume_decay(self, symbol: str) -> bool:
        """
        Проверяет угасание объёма.

        Если объём сильно упал по сравнению с предыдущим периодом,
        позиция может быть закрыта по причине VOL_DECAY.
        """
        w = Config.VOL_DECAY_WINDOW_MIN
        p = Config.VOL_DECAY_PRIOR_WINDOW_MIN

        limit = w + p + 1

        try:
            klines = await self.rest.get_klines(symbol, "1m", limit)

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

    # ================================================================
    # ТРЕЙЛИНГ
    # ================================================================

    def _calc_trailing_sl(
        self,
        profit_pct: float,
        side: str,
        current_price: float,
    ) -> float:
        """
        Считает новый Stop Loss для трейлинга.
        """
        step = self.trail_steps[0]

        for s in self.trail_steps:
            if profit_pct >= s:
                step = s

        offset_pct = Config.TRAILING_STEPS[step]

        if side == "LONG":
            return current_price * (1 - offset_pct / 100.0)

        return current_price * (1 + offset_pct / 100.0)

    # ================================================================
    # ОБНОВЛЕНИЕ ПОЗИЦИЙ
    # ================================================================

    async def update_positions(self, prices: Dict[str, float]):
        """
        Публичный метод обновления позиций.

        Используется scanner и position_watcher.
        """
        async with self._update_lock:
            await self._update_positions_locked(prices)

    async def _update_positions_locked(self, prices: Dict[str, float]):
        """
        Внутренний метод обновления позиций.
        """
        now = time.time()

        for symbol, pos in list(self.positions.items()):
            price = prices.get(symbol)

            if not price:
                continue

            # ------------------------------------------------------------
            # Подготовка
            # ------------------------------------------------------------

            prev_watch_price = pos.get("last_watch_price", pos["entry_price"])

            jump_pct = (
                abs(price - prev_watch_price) / prev_watch_price * 100.0
                if prev_watch_price > 0
                else 0.0
            )

            side = pos.get("side", "LONG")
            is_short = side == "SHORT"

            # Для SHORT "highest" означает лучшую минимальную цену.
            if is_short:
                if price < pos["highest"]:
                    pos["highest"] = price

            else:
                if price > pos["highest"]:
                    pos["highest"] = price

            # Текущая прибыль в процентах.
            if is_short:
                profit = (pos["entry_price"] - price) / pos["entry_price"] * 100.0
            else:
                profit = (price - pos["entry_price"]) / pos["entry_price"] * 100.0

            # MFE — максимальная прибыль за время сделки.
            if profit > pos.get("mfe", 0.0):
                pos["mfe"] = profit

            # MAE — максимальный минус за время сделки.
            adverse = -profit

            if adverse > pos.get("mae", 0.0):
                pos["mae"] = adverse

            hold_minutes = (now - pos["entry_time"]) / 60.0

            # ------------------------------------------------------------
            # Закрытие по угасанию объёма
            # ------------------------------------------------------------

            if hold_minutes >= Config.VOL_DECAY_CHECK_AFTER_MIN:
                if await self._check_volume_decay(symbol):
                    closed = await self._close_position(
                        symbol,
                        price,
                        "VOL_DECAY",
                    )

                    if not closed and symbol in self.positions:
                        self.positions[symbol]["last_watch_price"] = price

                    continue

            # ------------------------------------------------------------
            # Трейлинг
            # ------------------------------------------------------------

            if profit >= Config.TRAILING_ACTIVATION_PCT:
                new_sl = self._calc_trailing_sl(profit, side, price)

                improves = (
                    (side == "LONG" and new_sl > pos["sl_price"])
                    or
                    (side == "SHORT" and new_sl < pos["sl_price"])
                )

                if improves:
                    pos["sl_price"] = new_sl

                    if not pos.get("trail_active", False):
                        pos["trail_active"] = True

                        log.info(
                            f"Trailing activated {symbol} [{side}] "
                            f"profit={profit:.2f}% SL={fmt_price(new_sl)}"
                        )

                    else:
                        log.info(
                            f"Trailing update {symbol} [{side}]: "
                            f"SL -> {fmt_price(new_sl)} profit={profit:.2f}%"
                        )

            # ------------------------------------------------------------
            # TP1
            # ------------------------------------------------------------

            tp1_condition = (
                (is_short and price <= pos["tp1_price"])
                or
                (not is_short and price >= pos["tp1_price"])
            )

            if not pos.get("tp1_done", False) and tp1_condition:
                target_tp1_qty = pos["quantity"] * Config.TP1_SIZE_FRAC
                tp1_closed_qty = pos.get("tp1_closed_qty", 0.0)

                need_qty = target_tp1_qty - tp1_closed_qty

                if need_qty > 0:
                    can_partial = True

                    # [ИСПРАВЛЕНО]
                    # Для real mode проверяем, не получится ли частичное
                    # закрытие меньше minNotional.
                    if self.is_real:
                        info = await self.rest._get_symbol_info(
                            to_binance_symbol(symbol)
                        )

                        min_notional = info.get("minNotional", 5.0)

                        if need_qty * price < min_notional * 1.02:
                            log.warning(
                                f"{symbol}: TP1 часть {need_qty:.6f} "
                                f"может быть меньше minNotional, "
                                f"пропускаем частичное закрытие"
                            )

                            pos["tp1_done"] = True
                            pos["tp1_skipped"] = True

                            can_partial = False

                    if can_partial:
                        filled = await self._execute_partial_close(
                            symbol,
                            need_qty,
                            pos["tp1_price"],
                            "TP1",
                        )

                        if filled > 0:
                            pos["tp1_closed_qty"] = tp1_closed_qty + filled

                            # Если TP1 почти полностью закрыт,
                            # помечаем его как выполненный.
                            if (
                                pos["tp1_closed_qty"] >= target_tp1_qty * 0.999
                                or
                                pos["remaining_qty"] <= target_tp1_qty * 0.01
                            ):
                                pos["tp1_done"] = True

                        else:
                            log.warning(
                                f"{symbol}: TP1 ордер не исполнился, "
                                f"повторим при следующем условии"
                            )

                else:
                    pos["tp1_done"] = True

                # После успешного или пропущенного TP1
                # можно двигать стоп в безубыток.
                if pos.get("tp1_done", False):
                    if is_short:
                        be_price = pos["entry_price"] * (
                            1 - Config.BREAKEVEN_BUFFER_PCT / 100
                        )
                    else:
                        be_price = pos["entry_price"] * (
                            1 + Config.BREAKEVEN_BUFFER_PCT / 100
                        )

                    improves = (
                        (is_short and be_price < pos["sl_price"])
                        or
                        (not is_short and be_price > pos["sl_price"])
                    )

                    if improves:
                        pos["sl_price"] = be_price
                        pos["breakeven_set"] = True
                        pos["trailing_activated"] = True

            # ------------------------------------------------------------
            # Проверяем, существует ли позиция после TP1
            # ------------------------------------------------------------

            pos = self.positions.get(symbol)

            if not pos:
                continue

            # ------------------------------------------------------------
            # TP2
            # ------------------------------------------------------------

            tp2_condition = (
                (is_short and price <= pos["tp2_price"])
                or
                (not is_short and price >= pos["tp2_price"])
            )

            if not pos.get("tp2_done", False) and tp2_condition:
                closed = await self._close_position(symbol, price, "TP2")

                if not closed and symbol in self.positions:
                    self.positions[symbol]["last_watch_price"] = price

                continue

            # ------------------------------------------------------------
            # STOP LOSS
            # ------------------------------------------------------------

            sl_condition = (
                (is_short and price >= pos["sl_price"])
                or
                (not is_short and price <= pos["sl_price"])
            )

            if sl_condition:
                gap_threshold = pos.get("sl_pct", 0.0) * Config.GAP_ANOMALY_MULTIPLIER

                if gap_threshold > 0 and jump_pct > gap_threshold:
                    sl_reason = "GAP_SL"

                elif pos.get("trail_active", False):
                    sl_reason = "TRAIL_SL"

                elif pos.get("breakeven_set", False):
                    sl_reason = "BE_SL"

                else:
                    sl_reason = "SL"

                closed = await self._close_position(symbol, price, sl_reason)

                if not closed and symbol in self.positions:
                    self.positions[symbol]["last_watch_price"] = price

                continue

            # ------------------------------------------------------------
            # TIMEOUT
            # ------------------------------------------------------------

            timeout_sec = Config.POSITION_TIMEOUT_HOURS * 3600

            if now - pos["entry_time"] > timeout_sec:
                closed = await self._close_position(symbol, price, "TIMEOUT")

                if not closed and symbol in self.positions:
                    self.positions[symbol]["last_watch_price"] = price

                continue

            # ------------------------------------------------------------
            # Обычный Breakeven
            # ------------------------------------------------------------

            if (
                not pos.get("breakeven_set", False)
                and profit >= Config.BREAKEVEN_ACTIVATION_PCT
            ):
                if is_short:
                    be = pos["entry_price"] * (
                        1 - Config.BREAKEVEN_BUFFER_PCT / 100
                    )
                else:
                    be = pos["entry_price"] * (
                        1 + Config.BREAKEVEN_BUFFER_PCT / 100
                    )

                improves = (
                    (is_short and be < pos["sl_price"])
                    or
                    (not is_short and be > pos["sl_price"])
                )

                if improves:
                    pos["sl_price"] = be
                    pos["breakeven_set"] = True

                    log.info(
                        f"Breakeven set {symbol} @ {fmt_price(be)}"
                    )

            # ------------------------------------------------------------
            # Обновляем цену последнего тика
            # ------------------------------------------------------------

            if symbol in self.positions:
                self.positions[symbol]["last_watch_price"] = price

    # ================================================================
    # ПОЛНОЕ ЗАКРЫТИЕ ПОЗИЦИИ
    # ================================================================

    async def _close_position(
        self,
        symbol: str,
        exit_price: float,
        reason: str,
    ) -> bool:
        """
        Полностью закрывает позицию.

        [ИСПРАВЛЕНО]
        Возвращает:
        True  — позиция полностью закрыта.
        False — позиция осталась открытой, например partial fill.

        Раньше позиция удалялась даже при частичном исполнении,
        из-за чего бот мог потерять управление оставшейся частью.
        """
        pos = self.positions.get(symbol)

        if not pos:
            return True

        if pos.get("closing"):
            return False

        final_qty = pos["remaining_qty"]

        if final_qty <= 0:
            self.positions.pop(symbol, None)
            return True

        pos["closing"] = True

        try:
            side = pos.get("side", "LONG")

            if self.is_real:
                if side == "SHORT":
                    order = await self.rest.place_market_buy_close(symbol, final_qty)
                else:
                    order = await self.rest.place_market_sell(symbol, final_qty)

                if not order or order.get("status") not in ("filled", "partially_filled"):
                    log.error(
                        f"{symbol}: полное закрытие {reason} "
                        f"не исполнилось, ордер {order}"
                    )
                    return False

                actual_qty = float(order.get("filled_amount", 0))

                if actual_qty <= 0:
                    log.error(
                        f"{symbol}: полное закрытие {reason} исполнило 0"
                    )
                    return False

                actual_qty = min(actual_qty, final_qty)

            else:
                actual_qty = final_qty

            pnl = self._close_pnl(
                pos["entry_price"],
                exit_price,
                actual_qty,
                side,
            )

            pos["realized_pnl"] += pnl
            pos["closed_qty"] += actual_qty
            pos["remaining_qty"] -= actual_qty

            self.total_pnl += pnl
            self._daily_pnl += pnl

            if not self.is_real:
                self.capital += actual_qty * pos["entry_price"] + pnl

            # [ИСПРАВЛЕНО]
            # Если закрыта только часть позиции, не удаляем её.
            if pos["remaining_qty"] > 1e-12:
                log.warning(
                    f"{symbol}: закрытие {reason} частично: "
                    f"filled={actual_qty:.6f}, "
                    f"remaining={pos['remaining_qty']:.6f}"
                )
                return False

            # Позиция полностью закрыта.
            self.positions.pop(symbol, None)

            total_trade_pnl = pos.get("realized_pnl", 0.0)

            pnl_pct = (
                total_trade_pnl / pos["size_usdt"] * 100
                if pos["size_usdt"] > 0
                else 0.0
            )

            self.total_trades += 1
            self._trades_today += 1

            # Серия убытков.
            if total_trade_pnl < 0:
                self._consecutive_losses += 1
            else:
                self._consecutive_losses = 0

            # ------------------------------------------------------------
            # Статистика сделок
            # ------------------------------------------------------------

            if total_trade_pnl > 0:
                self.wins += 1

                if reason == "TP2":
                    self._cooldown_until.pop(symbol, None)
                    self._stop_history.pop(symbol, None)
                    self._repeat_block_until.pop(symbol, None)

                    log.info(
                        f"{symbol}: кулдаун, repeat_block и история стопов "
                        f"сброшены после {reason}"
                    )

            elif total_trade_pnl < 0:
                self.losses += 1

                if reason == "SL":
                    base_cd = time.time() + Config.SL_COOLDOWN_HOURS * 3600

                    self._cooldown_until[symbol] = max(
                        self._cooldown_until.get(symbol, 0),
                        base_cd,
                    )

                    log.warning(
                        f"{symbol}: кулдаун {Config.SL_COOLDOWN_HOURS}ч после SL"
                    )

                    self._stop_history.setdefault(symbol, []).append(time.time())

                    recent = [
                        t
                        for t in self._stop_history[symbol]
                        if t > time.time() - 86400
                    ]

                    if len(recent) >= Config.REPEAT_STOP_LIMIT:
                        extended_cd = time.time() + Config.REPEAT_BLOCK_HOURS * 3600

                        self._repeat_block_until[symbol] = max(
                            self._repeat_block_until.get(symbol, 0),
                            extended_cd,
                        )

                        log.warning(
                            f"Block {symbol} for "
                            f"{Config.REPEAT_BLOCK_HOURS}h "
                            f"due to stops (repeat_block)"
                        )

            else:
                # [ИСПРАВЛЕНО]
                # Раньше сделки с нулевым PnL считались убытками.
                self.breakevens += 1

            # Gap-блокировка ставится независимо от знака PnL.
            if reason == "GAP_SL":
                gap_cd = time.time() + Config.GAP_BLOCK_HOURS * 3600

                self._gap_block_until[symbol] = max(
                    self._gap_block_until.get(symbol, 0),
                    gap_cd,
                )

                log.warning(
                    f"{symbol}: обнаружен аномальный гэп цены -> "
                    f"блок на {Config.GAP_BLOCK_HOURS}ч (gap_block)"
                )

            # ------------------------------------------------------------
            # Запись сделки в БД
            # ------------------------------------------------------------

            self.db.log_trade(
                {
                    "timestamp": datetime.now().isoformat(),
                    "symbol": symbol,
                    "entry_price": pos["entry_price"],
                    "exit_price": exit_price,
                    "size_usdt": pos["size_usdt"],
                    "qty": pos["quantity"],
                    "pnl_pct": pnl_pct,
                    "pnl_usdt": total_trade_pnl,
                    "exit_reason": reason,
                    "entry_time": datetime.fromtimestamp(
                        pos["entry_time"]
                    ).isoformat(),
                    "exit_time": datetime.now().isoformat(),
                    "score": pos["score"],
                    "sl_pct": pos["sl_pct"],
                    "tp_pct": pos["tp1_pct"],
                    "side": side,
                    "mfe": pos.get("mfe", 0.0),
                    "mae": pos.get("mae", 0.0),
                }
            )

            log.info(
                f"CLOSE {symbol} [{side}] @ {fmt_price(exit_price)} "
                f"PnL={total_trade_pnl:+.2f}$ ({pnl_pct:+.2f}%) {reason}"
            )

            if self.is_real:
                await self.refresh_balance()

            return True

        finally:
            if symbol in self.positions:
                self.positions[symbol].pop("closing", None)

    # ================================================================
    # СТАТИСТИКА
    # ================================================================

    def get_stats(self) -> str:
        """
        Короткая статистика для лога и GUI.
        """
        wr = (
            self.wins / self.total_trades * 100
            if self.total_trades
            else 0
        )

        return (
            f"Сделок: {self.total_trades} | "
            f"Win: {self.wins} | "
            f"Loss: {self.losses} | "
            f"BE: {self.breakevens} | "
            f"WR: {wr:.0f}% | "
            f"PnL: ${self.total_pnl:+.2f} | "
            f"Капитал: ${self.capital:.2f} | "
            f"Открыто: {len(self.positions)}/{Config.MAX_OPEN_POSITIONS}"
        )

    def get_open_positions(self) -> List[dict]:
        """
        Возвращает список открытых позиций.
        """
        return list(self.positions.values())

    def log_equity(self):
        """
        Записывает снимок капитала в БД.
        """
        self.db.log_equity(
            self.capital,
            self.total_pnl,
            len(self.positions),
        )

    def get_trades(self, limit: int = 100):
        """
        Возвращает последние сделки из БД.
        """
        return self.db.get_trades(limit)

    def get_win_rate(self):
        """
        Возвращает win rate из БД.
        """
        return self.db.win_rate()

    def get_max_drawdown(self):
        """
        Возвращает максимальную просадку из БД.
        """
        return self.db.max_drawdown()