#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ФАЙЛ: scanner.py
# СОХРАНИТЬ КАК: scanner.py

Это главный сканер рынка.

Он делает:
1. получает список монет с Binance Futures;
2. отбирает кандидатов по объёму и цене;
3. сортирует их по волатильности;
4. анализирует LONG и SHORT сигналы;
5. передаёт подходящие сигналы в risk_manager.py;
6. управляет WebSocket-подписками;
7. обновляет тренд BTC;
8. следит за открытыми позициями через position_watcher.

ЧТО ИСПРАВЛЕНО:
1. high24 / low24 теперь берутся из реального 24h ticker,
   а не из последних 24 минутных свечей.

2. change_pct теперь передаётся как signed change,
   то есть со знаком: +15% или -15%.

3. Используется публичный ws_client.get_subscribed(),
   а не прямой доступ к приватному полю _subscribed.

4. Ликвидность проверяется через Config.MAX_SPREAD_PCT
   и Config.MIN_ORDERBOOK_DEPTH_USDT.

5. Equity пишется не каждый цикл, а примерно раз в 60 секунд,
   чтобы не раздувать базу данных.
"""

import asyncio
import time
from typing import Dict, List, Optional

import aiohttp
import numpy as np

from api import BinanceFuturesRestClient
from config import Config
from logger import log, parse_klines, ema, play_sound
from risk_manager import PositionManager

from signals import (
    check_not_freefall,
    check_pullback_setup,
    check_pushback_setup,
    find_directed_impulse,
    find_directed_impulse_down,
    detect_historical_pump,
    detect_historical_dump,
    detect_pullback,
    detect_pushback,
    detect_consolidation,
    detect_consolidation_short,
    detect_rocket_pullback,
    detect_rocket_pushback_down,
    compute_hybrid_score,
    compute_hybrid_score_short,
)

from ws_client import BinanceWsClient


def _candidate_ok(pair: str) -> bool:
    """
    Проверяет, подходит ли символ для сканирования.
    """
    return pair.endswith("_USDT") and pair not in Config.EXCLUDED


class WaveXScanner:
    """
    Основной класс сканера.
    """

    def __init__(self):
        # HTTP-сессия aiohttp.
        self.session: Optional[aiohttp.ClientSession] = None
        # REST-клиент Binance Futures.
        self.rest_client: Optional[BinanceFuturesRestClient] = None
        # WebSocket-клиент для микроструктуры.
        # [ИСПРАВЛЕНО] Создаётся в init() после rest_client и session
        self.ws_client: Optional[BinanceWsClient] = None
        # Менеджер позиций.
        self.pos_manager: Optional[PositionManager] = None
        # Текущий тренд BTC в процентах относительно EMA50 на 15m.
        self.btc_trend = 0.0
        # Номер цикла сканирования.
        self.cycle = 0
        # Цены кандидатов за последний цикл.
        self.prices: Dict[str, float] = {}
        # История сигналов для GUI.
        self.signals_history: List[dict] = []
        # Флаг остановки.
        self._stop_flag = [False]
        # Статистика пропусков по причинам.
        self.skip_stats: Dict[str, int] = {}
        # Все score за цикл, для диагностики.
        self._cycle_scores: List[float] = []
        # Кулдауны для near-miss звуков.
        self._near_miss_last_alert: Dict[str, float] = {}
        # Список фоновых asyncio-задач.
        self._tasks: List[asyncio.Task] = []
        # Флаг включения/отключения торговли.
        self.trading_enabled = [False]

    # ================================================================
    # СЛУЖЕБНОЕ
    # ================================================================

    def _skip(self, reason: str):
        """
        Увеличивает счётчик пропусков по причине.
        """
        self.skip_stats[reason] = self.skip_stats.get(reason, 0) + 1

    def _track_score(self, score: float):
        """
        Добавляет score в статистику цикла.
        """
        self._cycle_scores.append(score)

    def stop(self):
        """
        Останавливает сканер.
        """
        self._stop_flag[0] = True

    # ================================================================
    # ИНИЦИАЛИЗАЦИЯ И ЗАКРЫТИЕ
    # ================================================================

    async def init(self):
        """
        Инициализирует сканер:
        - HTTP-сессию;
        - REST-клиент;
        - PositionManager;
        - WebSocket-клиент.
        """
        connector = aiohttp.TCPConnector(
            limit=Config.HTTP_CONNECTOR_LIMIT, ttl_dns_cache=300,
        )
        self.session = aiohttp.ClientSession(connector=connector)
        self.rest_client = BinanceFuturesRestClient(
            Config.BINANCE_API_KEY, Config.BINANCE_API_SECRET, self.session,
        )
        
        # [ИСПРАВЛЕНО] Создаём ws_client ПОСЛЕ rest_client и session
        self.ws_client = BinanceWsClient(
            rest_client=self.rest_client,
            session=self.session,
        )
        
        # [НОВОЕ] Блокирующая инициализация кэша фильтров
        try:
            await self.rest_client.filters_cache.initialize()
        except Exception as e:
            log.error(f"Критическая ошибка: не удалось инициализировать кэш фильтров: {e}")
            if self.session and not self.session.closed:
                await self.session.close()
            raise
        
        # [НОВОЕ] Запуск фонового обновления кэша
        self.rest_client.filters_cache.start_background_updater()
        
        # [ИСПРАВЛЕНО] Создаём PositionManager ОДИН раз
        self.pos_manager = PositionManager(self.rest_client, Config.REAL_TRADING)
        
        # [ИСПРАВЛЕНО] Передаём tracker в ws_client для обработки ORDER_TRADE_UPDATE
        self.ws_client.set_tracker(self.pos_manager.tracker)
        
        # Сначала запускаем WebSocket (одна задача, не дублируем)
        ws_task = asyncio.create_task(
            self.ws_client.run(self.session, self._stop_flag)
        )
        self._tasks.append(ws_task)
        
        # Запускаем User Data Stream
        if Config.REAL_TRADING:
            user_data_ok = await self.ws_client.start_user_data_stream()
            if not user_data_ok:
                # [ИСПРАВЛЕНО] Не завершаем бот, а продолжаем без User Data Stream
                # Бот может работать без WS (через REST-опрос), просто медленнее
                log.warning(
                    "User Data Stream не запустился. "
                    "Бот продолжит работу через REST-опрос позиций."
                )
        
        # Ждем, пока WebSocket полностью подключится
        log.info("Ждем подключения WebSocket...")
        try:
            await asyncio.wait_for(
                self.ws_client.wait_until_ready(),
                timeout=10.0,
            )
            log.info("WebSocket готов к работе")
        except Exception:
            log.warning("WS не готов, продолжаем с ограниченной функциональностью")
        
        # Теперь запускаем reconciliation
        if Config.REAL_TRADING:
            await self.pos_manager.refresh_balance()
            try:
                recon_ok = await self.pos_manager.reconcile()
                if not recon_ok:
                    log.error("Reconciliation провалился — бот не может безопасно торговать")
                    self.trading_enabled[0] = False
                else:
                    # Подписываемся на восстановленные позиции
                    if self.pos_manager.positions:
                        open_symbols = list(self.pos_manager.positions.keys())
                        log.info(f"[WS] Подписываемся на {len(open_symbols)} восстановленных позиций")
                        await self.ws_client.subscribe(open_symbols)
            except Exception as e:
                log.error(f"Ошибка reconciliation: {e}")
        
        # [ИСПРАВЛЕНО] Запускаем position_watcher как фоновую задачу
        watcher_task = asyncio.create_task(
            self.position_watcher()
        )
        self._tasks.append(watcher_task)
        
        # Даём WebSocket 5 секунд на первое подключение.
        try:
            await asyncio.wait_for(
                self.ws_client._ws_ready.wait(),
                timeout=10.0,
            )
        except Exception:
            log.warning("WS не готов, продолжаем без него")
        
        # Включаем торговлю по умолчанию
        try:
            can_trade = True
            if Config.REAL_TRADING:
                if not self.trading_enabled[0]:
                    can_trade = False
            if can_trade:
                self.trading_enabled[0] = True
                log.info("Торговля автоматически включена после инициализации")
        except Exception:
            pass


    async def close(self):
        """
        Корректное закрытие сканера:
        1. останавливаем задачи;
        2. закрываем HTTP-сессию;
        3. закрываем базу данных.
        """
        self.stop()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        
        # [НОВОЕ] Остановка фонового обновления кэша
        if self.rest_client:
            try:
                await self.rest_client.filters_cache.stop_background_updater()
            except Exception as e:
                log.debug(f"Ошибка остановки filters_cache updater: {e}")
        
        if self.session and not self.session.closed:
            await self.session.close()
        if self.pos_manager:
            self.pos_manager.close()

    # ================================================================
    # BTC TREND
    # ================================================================

    async def update_btc_trend(self):
        """
        Периодически обновляет тренд BTC.

        Используется EMA50 на 15m.
        Если цена выше EMA — тренд положительный.
        Если ниже — отрицательный.
        """
        while not self._stop_flag[0]:
            try:
                klines = await self.rest_client.get_klines(
                    "BTC_USDT",
                    "15m",
                    100,
                )

                if klines:
                    _, _, _, C, _ = parse_klines(klines)

                    if len(C) > 50:
                        e = ema(C, 50)

                        if e[-1] > 0:
                            self.btc_trend = (C[-1] - e[-1]) / e[-1] * 100

            except Exception:
                pass

            await asyncio.sleep(Config.BTC_UPDATE_SECS)

    # ================================================================
    # POSITION WATCHER
    # ================================================================

    async def position_watcher(self):
        """
        Быстрый наблюдатель позиций.

        Работает каждые POSITION_CHECK_INTERVAL секунд.

        Его задача:
        - получать свежую цену по открытым позициям;
        - передавать цены в PositionManager;
        - PositionManager проверяет SL, TP1, TP2, trailing, timeout.
        """
        while not self._stop_flag[0]:
            t0 = time.time()

            try:
                pm = self.pos_manager

                if pm and pm.positions:
                    symbols = list(pm.positions.keys())

                    price_map: Dict[str, float] = {}

                    for symbol in symbols:
                        # Сначала пробуем взять свежую цену из WebSocket.
                        price = await self.ws_client.get_last_price(
                            symbol,
                            max_age_sec=Config.POSITION_PRICE_MAX_AGE_SEC,
                        )

                        # Если WebSocket-цена старая или отсутствует,
                        # берём цену через REST.
                        if price is None:
                            price = await self.rest_client.get_last_price(symbol)

                        if price is not None and price > 0:
                            price_map[symbol] = price

                    if price_map:
                        await pm.update_positions(price_map)

            except asyncio.CancelledError:
                break

            except Exception as e:
                log.debug(f"position_watcher error: {e}")

            elapsed = time.time() - t0

            await asyncio.sleep(
                max(0.2, Config.POSITION_CHECK_INTERVAL - elapsed)
            )

    # ================================================================
    # ОТКАЗ СИГНАЛА
    # ================================================================

    def _reject(
        self,
        symbol: str,
        price: float,
        reason: str,
        score: float = 0,
        confidence: str = "SKIP",
        reasons=None,
        penalties=None,
        side: str = "LONG",
        high24: float = 0.0,
        low24: float = 0.0,
    ) -> dict:
        """
        Возвращает отклонённый сигнал.

        Это нужно для GUI и статистики.
        """
        self._skip(reason)
        self._track_score(score)

        return {
            "symbol": symbol,
            "price": price,
            "score": score,
            "confidence": confidence,
            "reasons": reasons or [],
            "penalties": penalties or [reason],
            "side": side,
            "high24": high24,
            "low24": low24,
            "structural_level": None,
            "spread_pct": 0.0,
            "rejected": True,
            "reject_reason": reason,
        }

    # ================================================================
    # LONG АНАЛИЗ
    # ================================================================

    async def analyze_symbol(
        self,
        symbol: str,
        price: float,
        volume_24h: float,
        change_signed: float,
        high24: float,
        low24: float,
    ) -> dict:
        """
        Полный анализ символа для LONG.
        """
        try:
            # ------------------------------------------------------------
            # 1. 1m свечи
            # ------------------------------------------------------------

            klines_1m = await self.rest_client.get_klines(symbol, "1m", 350)

            if not klines_1m:
                return self._reject(
                    symbol,
                    price,
                    "нет_klines_1m",
                    high24=high24,
                    low24=low24,
                )

            O, H, L, C, V = parse_klines(klines_1m)

            if len(C) < 60:
                return self._reject(
                    symbol,
                    price,
                    "мало_баров_1m",
                    high24=high24,
                    low24=low24,
                )

            # ------------------------------------------------------------
            # 2. 5m свечи и ракета
            # ------------------------------------------------------------

            klines_5m = await self.rest_client.get_klines(symbol, "5m", 96)

            rocket = None

            if klines_5m:
                candles_5m = [
                    [
                        k[0],
                        float(k[5]),
                        float(k[3]),
                        float(k[4]),
                        float(k[2]),
                        float(k[1]),
                    ]
                    for k in klines_5m
                ]

                # [ИСПРАВЛЕНО]
                # Передаём signed change, а не abs.
                rocket = detect_rocket_pullback(candles_5m, change_signed)

            # ------------------------------------------------------------
            # 3. Импульс
            # ------------------------------------------------------------

            impulse = find_directed_impulse(H, L, C, V)

            if impulse is None and rocket is None:
                return self._reject(
                    symbol,
                    price,
                    "нет_импульса_и_не_ракета",
                    high24=high24,
                    low24=low24,
                )

            # ------------------------------------------------------------
            # 4. Фильтр свободного падения
            # ------------------------------------------------------------

            ff_ok, ff_reason = check_not_freefall(O, C)

            if not ff_ok:
                return self._reject(
                    symbol,
                    price,
                    "свободное_падение",
                    penalties=[ff_reason],
                    high24=high24,
                    low24=low24,
                )

            # ------------------------------------------------------------
            # 5. Откат
            # ------------------------------------------------------------

            pullback_ok, pullback_reason, retrace_pct = (True, "", 0.0)

            if impulse is not None:
                pullback_ok, pullback_reason, retrace_pct = check_pullback_setup(
                    H,
                    L,
                    C,
                    impulse,
                )

                if not pullback_ok and rocket is None:
                    return self._reject(
                        symbol,
                        price,
                        "нет_отката_от_хая",
                        penalties=[pullback_reason],
                        high24=high24,
                        low24=low24,
                    )

            # ------------------------------------------------------------
            # 6. 1h свечи, исторический памп, тренд
            # ------------------------------------------------------------

            klines_1h = await self.rest_client.get_klines(symbol, "1h", 336)

            hist_pump = None
            pullback = None
            consolidation = None

            symbol_trend_dev_pct = 0.0

            if klines_1h and len(klines_1h) > 48:
                candles_1h = [
                    [
                        k[0],
                        float(k[5]),
                        float(k[3]),
                        float(k[4]),
                        float(k[2]),
                        float(k[1]),
                    ]
                    for k in klines_1h
                ]

                hist_pump = detect_historical_pump(candles_1h)

                if hist_pump:
                    pullback = detect_pullback(candles_1h, hist_pump)

                    if pullback:
                        consolidation = detect_consolidation(
                            candles_1h,
                            pullback,
                        )

                closes_1h = np.array([c[4] for c in candles_1h])

                if len(closes_1h) >= Config.TREND_EMA_PERIOD:
                    trend_ema = ema(closes_1h, Config.TREND_EMA_PERIOD)

                    if trend_ema[-1] > 0:
                        symbol_trend_dev_pct = (
                            (closes_1h[-1] - trend_ema[-1])
                            / trend_ema[-1]
                            * 100.0
                        )

            # ------------------------------------------------------------
            # 7. Стакан
            # ------------------------------------------------------------

            orderbook = await self.rest_client.get_orderbook(symbol, limit=20)

            if not orderbook:
                return self._reject(
                    symbol,
                    price,
                    "нет_orderbook",
                    high24=high24,
                    low24=low24,
                )

            bids = orderbook.get("bids", [])
            asks = orderbook.get("asks", [])

            if not bids or not asks:
                return self._reject(
                    symbol,
                    price,
                    "пустой_стакан",
                    high24=high24,
                    low24=low24,
                )

            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])

            spread = (best_ask - best_bid) / best_bid * 100

            if spread > Config.MAX_SPREAD_PCT:
                return self._reject(
                    symbol,
                    price,
                    f"спред>{Config.MAX_SPREAD_PCT}%",
                    high24=high24,
                    low24=low24,
                )

            depth_bid = sum(float(b[0]) * float(b[1]) for b in bids[:10])
            depth_ask = sum(float(a[0]) * float(a[1]) for a in asks[:10])

            depth = min(depth_bid, depth_ask)

            if depth < Config.MIN_ORDERBOOK_DEPTH_USDT:
                return self._reject(
                    symbol,
                    price,
                    f"глубина<${Config.MIN_ORDERBOOK_DEPTH_USDT:.0f}",
                    high24=high24,
                    low24=low24,
                )

            # ------------------------------------------------------------
            # 8. Микроструктура и score
            # ------------------------------------------------------------

            micro = await self.ws_client.get(symbol)

            score, confidence, reasons, penalties = compute_hybrid_score(
                impulse,
                hist_pump,
                pullback,
                consolidation,
                rocket,
                micro,
                volume_24h,
                change_signed,
                self.btc_trend,
                symbol_trend_dev_pct,
            )

            if impulse is not None:
                if pullback_ok:
                    reasons.append(
                        f"откат {retrace_pct:.0f}% (в зоне входа)"
                    )

                elif rocket is not None:
                    penalties.append(
                        f"откат вне зоны ({pullback_reason}), вход по ракете"
                    )

            self._track_score(score)

            result = {
                "symbol": symbol,
                "price": price,
                "score": score,
                "confidence": confidence,
                "reasons": reasons,
                "penalties": penalties,
                "side": "LONG",

                # [ИСПРАВЛЕНО]
                # Реальные 24h high/low из ticker.
                "high24": high24,
                "low24": low24,

                "structural_level": (
                    impulse["impulse_low"]
                    if impulse is not None
                    else (
                        rocket.get("pullback_low")
                        if rocket
                        else None
                    )
                ),

                "spread_pct": micro.get("spread_pct", spread),
                "entry_ref_price": float(C[-1]),
                "klines_1h": klines_1h,
            }

            # ------------------------------------------------------------
            # 9. Порог
            # ------------------------------------------------------------

            threshold = PositionManager.get_adaptive_threshold(
                "LONG",
                self.btc_trend,
            )

            if score >= threshold:
                result["rejected"] = False
                result["reject_reason"] = ""

            else:
                self._skip(f"score<{threshold:.0f}")

                result["rejected"] = True
                result["reject_reason"] = (
                    f"score {score:.0f} < {threshold:.0f}"
                )

            return result

        except Exception as e:
            log.exception(f"Ошибка анализа {symbol}: {e}")

            return self._reject(
                symbol,
                price,
                "ошибка_анализа",
                high24=high24,
                low24=low24,
            )

    # ================================================================
    # SHORT АНАЛИЗ
    # ================================================================

    async def analyze_symbol_short(
        self,
        symbol: str,
        price: float,
        volume_24h: float,
        change_signed: float,
        high24: float,
        low24: float,
    ) -> dict:
        """
        Полный анализ символа для SHORT.
        """
        try:
            # ------------------------------------------------------------
            # 1. 1m свечи
            # ------------------------------------------------------------

            klines_1m = await self.rest_client.get_klines(symbol, "1m", 350)

            if not klines_1m:
                return self._reject(
                    symbol,
                    price,
                    "нет_klines_1m",
                    side="SHORT",
                    high24=high24,
                    low24=low24,
                )

            O, H, L, C, V = parse_klines(klines_1m)

            if len(C) < 60:
                return self._reject(
                    symbol,
                    price,
                    "мало_баров_1m",
                    side="SHORT",
                    high24=high24,
                    low24=low24,
                )

            # ------------------------------------------------------------
            # 2. 5m свечи и обвал
            # ------------------------------------------------------------

            klines_5m = await self.rest_client.get_klines(symbol, "5m", 96)

            rocket_down = None

            if klines_5m:
                candles_5m = [
                    [
                        k[0],
                        float(k[5]),
                        float(k[3]),
                        float(k[4]),
                        float(k[2]),
                        float(k[1]),
                    ]
                    for k in klines_5m
                ]

                # [ИСПРАВЛЕНО]
                # Передаём signed change, а не abs.
                rocket_down = detect_rocket_pushback_down(
                    candles_5m,
                    change_signed,
                )

            # ------------------------------------------------------------
            # 3. Импульс вниз
            # ------------------------------------------------------------

            impulse_down = find_directed_impulse_down(H, L, C, V)

            if impulse_down is None and rocket_down is None:
                return self._reject(
                    symbol,
                    price,
                    "нет_импульса_вниз_и_не_обвал",
                    side="SHORT",
                    high24=high24,
                    low24=low24,
                )

            # ------------------------------------------------------------
            # 4. Отскок
            # ------------------------------------------------------------

            pushback_ok, pushback_reason, retrace_pct = (True, "", 0.0)

            if impulse_down is not None:
                pushback_ok, pushback_reason, retrace_pct = check_pushback_setup(
                    H,
                    L,
                    C,
                    impulse_down,
                )

            # ------------------------------------------------------------
            # 5. 1h свечи, исторический дамп, тренд
            # ------------------------------------------------------------

            klines_1h = await self.rest_client.get_klines(symbol, "1h", 336)

            hist_dump = None
            pushback = None
            consolidation = None

            symbol_trend_dev_pct = 0.0

            if klines_1h and len(klines_1h) > 48:
                candles_1h = [
                    [
                        k[0],
                        float(k[5]),
                        float(k[3]),
                        float(k[4]),
                        float(k[2]),
                        float(k[1]),
                    ]
                    for k in klines_1h
                ]

                hist_dump = detect_historical_dump(candles_1h)

                if hist_dump:
                    pushback = detect_pushback(candles_1h, hist_dump)

                    if pushback:
                        consolidation = detect_consolidation_short(
                            candles_1h,
                            pushback,
                        )

                closes_1h = np.array([c[4] for c in candles_1h])

                if len(closes_1h) >= Config.TREND_EMA_PERIOD:
                    trend_ema = ema(closes_1h, Config.TREND_EMA_PERIOD)

                    if trend_ema[-1] > 0:
                        symbol_trend_dev_pct = (
                            (closes_1h[-1] - trend_ema[-1])
                            / trend_ema[-1]
                            * 100.0
                        )

            # ------------------------------------------------------------
            # 6. Стакан
            # ------------------------------------------------------------

            orderbook = await self.rest_client.get_orderbook(symbol, limit=20)

            if not orderbook:
                return self._reject(
                    symbol,
                    price,
                    "нет_orderbook",
                    side="SHORT",
                    high24=high24,
                    low24=low24,
                )

            bids = orderbook.get("bids", [])
            asks = orderbook.get("asks", [])

            if not bids or not asks:
                return self._reject(
                    symbol,
                    price,
                    "пустой_стакан",
                    side="SHORT",
                    high24=high24,
                    low24=low24,
                )

            best_bid = float(bids[0][0])
            best_ask = float(asks[0][0])

            spread = (best_ask - best_bid) / best_bid * 100

            if spread > Config.MAX_SPREAD_PCT:
                return self._reject(
                    symbol,
                    price,
                    f"спред>{Config.MAX_SPREAD_PCT}%",
                    side="SHORT",
                    high24=high24,
                    low24=low24,
                )

            depth_bid = sum(float(b[0]) * float(b[1]) for b in bids[:10])
            depth_ask = sum(float(a[0]) * float(a[1]) for a in asks[:10])

            depth = min(depth_bid, depth_ask)

            if depth < Config.MIN_ORDERBOOK_DEPTH_USDT:
                return self._reject(
                    symbol,
                    price,
                    f"глубина<${Config.MIN_ORDERBOOK_DEPTH_USDT:.0f}",
                    side="SHORT",
                    high24=high24,
                    low24=low24,
                )

            # ------------------------------------------------------------
            # 7. Микроструктура и score
            # ------------------------------------------------------------

            micro = await self.ws_client.get(symbol)

            score, confidence, reasons, penalties = compute_hybrid_score_short(
                impulse_down,
                hist_dump,
                pushback,
                consolidation,
                rocket_down,
                micro,
                volume_24h,
                change_signed,
                self.btc_trend,
                symbol_trend_dev_pct,
            )

            if impulse_down is not None:
                if pushback_ok:
                    reasons.append(
                        f"отскок {retrace_pct:.0f}% (в зоне входа)"
                    )

                elif rocket_down is not None:
                    penalties.append(
                        f"отскок вне зоны ({pushback_reason}), вход по обвалу"
                    )

            else:
                penalties.append(
                    f"первая волна падения, без отскока ({pushback_reason})"
                )

            self._track_score(score)

            result = {
                "symbol": symbol,
                "price": price,
                "side": "SHORT",
                "score": score,
                "confidence": confidence,
                "reasons": reasons,
                "penalties": penalties,

                # [ИСПРАВЛЕНО]
                # Реальные 24h high/low из ticker.
                "high24": high24,
                "low24": low24,

                "structural_level": (
                    impulse_down["impulse_high"]
                    if impulse_down is not None
                    else (
                        rocket_down.get("pushback_high")
                        if rocket_down
                        else None
                    )
                ),

                "spread_pct": micro.get("spread_pct", spread),
                "entry_ref_price": float(C[-1]),
                "klines_1h": klines_1h,
            }

            # ------------------------------------------------------------
            # 8. Порог
            # ------------------------------------------------------------

            threshold = PositionManager.get_adaptive_threshold(
                "SHORT",
                self.btc_trend,
            )

            if score >= threshold:
                result["rejected"] = False
                result["reject_reason"] = ""

            else:
                self._skip(f"score<{threshold:.0f}")

                result["rejected"] = True
                result["reject_reason"] = (
                    f"score {score:.0f} < {threshold:.0f}"
                )

            return result

        except Exception as e:
            log.exception(f"Ошибка анализа {symbol} (SHORT): {e}")

            return self._reject(
                symbol,
                price,
                "ошибка_анализа",
                side="SHORT",
                high24=high24,
                low24=low24,
            )

    # ================================================================
    # ОСНОВНОЙ ЦИКЛ
    # ================================================================

    async def scan(self):
        """
        Главный цикл сканирования рынка.
        """
        while not self._stop_flag[0]:
            try:
                self.cycle += 1

                log.info(f"=== Cycle {self.cycle} ===")

                # ------------------------------------------------------------
                # 1. Получаем тикеры
                # ------------------------------------------------------------

                tickers = await self.rest_client.get_tickers()

                if not tickers:
                    await asyncio.sleep(Config.SCAN_INTERVAL)
                    continue

                candidates = []

                for t in tickers:
                    pair = t.get("currency_pair", "")

                    if not _candidate_ok(pair):
                        continue

                    try:
                        vol = float(t.get("quote_volume", 0))
                        price = float(t.get("last", 0))

                        # [ИСПРАВЛЕНО]
                        # Берём signed change.
                        # Плюс или минус важен для SHORT/LONG.
                        change_signed = float(t.get("change_percentage", 0))

                        # [ИСПРАВЛЕНО]
                        # Реальные 24h high/low.
                        high24 = float(t.get("high_24h", 0))
                        low24 = float(t.get("low_24h", 0))

                    except Exception:
                        continue

                    if vol < Config.MIN_VOL_24H or price < Config.MIN_PRICE:
                        continue

                    candidates.append(
                        (
                            pair,
                            vol,
                            price,
                            change_signed,
                            high24,
                            low24,
                        )
                    )

                # Сортируем по абсолютному изменению цены.
                candidates.sort(
                    key=lambda x: abs(x[3]),
                    reverse=True,
                )

                candidates = candidates[:Config.TOP_VOLATILE_N]

                log.info(f"Найдено {len(candidates)} кандидатов")

                # ------------------------------------------------------------
                # 2. Обновление позиций и equity
                # ------------------------------------------------------------

                self.prices = {p[0]: p[2] for p in candidates}

                await self.pos_manager.update_positions(self.prices)

                # ------------------------------------------------------------
                # 3. WebSocket подписки
                # ------------------------------------------------------------

                desired = {c[0] for c in candidates}

                # [ИСПРАВЛЕНО]
                # Используем публичный метод, а не приватный _subscribed.
                current = await self.ws_client.get_subscribed()

                to_subscribe = list(desired - current)
                to_unsubscribe = list(current - desired)

                if to_subscribe:
                    await self.ws_client.subscribe(to_subscribe)

                if to_unsubscribe:
                    await self.ws_client.unsubscribe(to_unsubscribe)

                # ------------------------------------------------------------
                # 4. Анализ
                # ------------------------------------------------------------

                self.skip_stats = {}
                self._cycle_scores = []

                sem = asyncio.Semaphore(Config.ANALYZE_CONCURRENCY)

                async def _bounded_long(item):
                    symbol, vol, price, chg, h24, l24 = item

                    async with sem:
                        return await self.analyze_symbol(
                            symbol,
                            price,
                            vol,
                            chg,
                            h24,
                            l24,
                        )

                async def _bounded_short(item):
                    symbol, vol, price, chg, h24, l24 = item

                    async with sem:
                        return await self.analyze_symbol_short(
                            symbol,
                            price,
                            vol,
                            chg,
                            h24,
                            l24,
                        )

                long_results = list(
                    await asyncio.gather(
                        *[
                            _bounded_long(item)
                            for item in candidates
                        ]
                    )
                )

                short_results = []

                if Config.SHORT_TRADING_ENABLED:
                    short_results = list(
                        await asyncio.gather(
                            *[
                                _bounded_short(item)
                                for item in candidates
                            ]
                        )
                    )

                all_results = long_results + short_results

                all_results.sort(
                    key=lambda x: x["score"],
                    reverse=True,
                )

                self.signals_history = all_results[:40]

                # ------------------------------------------------------------
                # 5. Отбор сигналов для открытия
                # ------------------------------------------------------------

                tradeable = []

                for r in all_results:
                    if r.get("rejected", True):
                        continue

                    side = r.get("side", "LONG")

                    threshold = PositionManager.get_adaptive_threshold(
                        side,
                        self.btc_trend,
                    )

                    if r["score"] >= threshold:
                        tradeable.append(r)

                # ------------------------------------------------------------
                # 6. Near miss
                # ------------------------------------------------------------

                if Config.NEAR_MISS_SOUND_ENABLED:
                    now_nm = time.time()

                    for r in all_results:
                        if not r.get("rejected", True):
                            continue

                        side = r.get("side", "LONG")

                        base_threshold = (
                            Config.SCORE_TRADE_THRESHOLD_SHORT
                            if side == "SHORT"
                            else Config.SCORE_TRADE_THRESHOLD
                        )

                        near_miss_floor = (
                            base_threshold - Config.NEAR_MISS_MARGIN
                        )

                        if near_miss_floor <= r["score"] < base_threshold:
                            sym = r["symbol"]

                            last_alert = self._near_miss_last_alert.get(
                                sym,
                                0.0,
                            )

                            if now_nm - last_alert >= Config.NEAR_MISS_ALERT_COOLDOWN_SEC:
                                self._near_miss_last_alert[sym] = now_nm

                                play_sound("near_miss")

                                log.info(
                                    f"🔔 NEAR MISS {sym} [{side}] "
                                    f"score={r['score']:.0f}"
                                )

                # ------------------------------------------------------------
                # 7. Попытки открыть позиции
                # ------------------------------------------------------------

                opened = 0

                open_fail_stats: Dict[str, int] = {}

                # [НОВОЕ] Проверяем, включена ли торговля
                if not self.trading_enabled[0]:
                    log.info("Торговля отключена — новые позиции не открываются")
                else:
                    for sig in tradeable:
                        if len(self.pos_manager.positions) >= Config.MAX_OPEN_POSITIONS:
                            break

                        ok, reason = await self.pos_manager.open_position(
                            sig["symbol"],
                            sig.get("entry_ref_price", sig["price"]),
                            sig["score"],
                            sig["confidence"],
                            sig.get("klines_1h"),
                            sig.get("high24", 0),
                            sig.get("low24", 0),
                            sig.get("structural_level"),
                            sig.get("spread_pct", 0.0),
                            side=sig.get("side", "LONG"),
                            btc_trend=self.btc_trend,
                        )

                        if ok:
                            opened += 1

                        else:
                            open_fail_stats[reason] = (
                                open_fail_stats.get(reason, 0) + 1
                            )

                            log.info(
                                f"  ✗ {sig['symbol']} "
                                f"[{sig.get('side', 'LONG')}] "
                                f"score={sig['score']:.0f} "
                                f"не открыта: {reason}"
                            )

                # ------------------------------------------------------------
                # 8. Итоги цикла
                # ------------------------------------------------------------

                skip_summary = ", ".join(
                    f"{k}={v}"
                    for k, v in sorted(
                        self.skip_stats.items(),
                        key=lambda x: -x[1],
                    )
                )

                log.info(
                    f"📊 Инспектор цикл {self.cycle}: "
                    f"кандидатов={len(candidates)} | "
                    f"проходных={len(tradeable)} | "
                    f"открыто={opened} | "
                    f"BTC={self.btc_trend:+.2f}%"
                )

                if skip_summary:
                    log.info(f"   пропущено: {skip_summary}")

                if open_fail_stats:
                    log.info(
                        "   отказ при попытке входа: "
                        + ", ".join(
                            f"{k}={v}"
                            for k, v in open_fail_stats.items()
                        )
                    )

                log.info(self.pos_manager.get_stats())

                await asyncio.sleep(Config.SCAN_INTERVAL)

            except asyncio.CancelledError:
                break

            except Exception as e:
                log.exception(f"Ошибка в цикле: {e}")

                await asyncio.sleep(5)