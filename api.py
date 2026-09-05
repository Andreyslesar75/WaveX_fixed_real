#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ФАЙЛ: api.py
# СОХРАНИТЬ КАК: api.py

Этот файл отвечает за общение с Binance Futures через REST API.

ЧТО ИСПРАВЛЕНО:
1. Добавлена синхронизация времени с сервером Binance.
   Раньше timestamp брался только с локального компьютера.
   Если время на компьютере немного спешит или отстаёт,
   Binance мог отклонять signed-запросы с ошибкой -1021.

2. Добавлен newClientOrderId для ордеров.
   Это уникальный ID ордера, который позволяет проверить,
   был ли ордер реально создан, даже если ответ от Binance потерялся.

3. Market-ордера больше НЕ повторяются слепо при таймауте.
   Раньше при сетевой ошибке бот мог отправить ордер повторно,
   даже если первый ордер уже был принят биржей.
   Теперь при сомнительной ситуации бот сначала проверяет статус ордера.

4. Если market-ордер вернулся со статусом NEW, бот несколько раз
   проверяет его статус, прежде чем считать сделку не исполненной.

5. Добавлено округление цены для limit-ордеров.
   Раньше цена могла быть с слишком большим числом знаков,
   из-за чего Binance мог отклонить ордер.

6. Добавлена проверка minNotional перед открытием позиции.

7. Добавлены high_24h и low_24h в get_tickers().
   Это нужно, чтобы scanner позже мог брать реальный 24-часовой диапазон,
   а не считать его случайно по последним 24 минутам.
"""

import asyncio
import hashlib
import hmac
import math
import time
import urllib.parse
import uuid
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from typing import Optional, List, Dict, Any

import aiohttp

from config import Config
from logger import log


def to_binance_symbol(internal_symbol: str) -> str:
    """
    Преобразует внутренний символ бота в символ Binance.

    Пример:
    SOL_USDT -> SOLUSDT
    """
    return internal_symbol.replace("_", "")


def to_internal_symbol(binance_symbol: str) -> str:
    """
    Преобразует символ Binance в внутренний символ бота.

    Пример:
    SOLUSDT -> SOL_USDT
    """
    s = binance_symbol.upper()

    if s.endswith("USDT"):
        return s[:-4] + "_USDT"

    return s


def _safe_float(value: Any, default: float = 0.0) -> float:
    """
    Безопасно превращает значение в float.
    Если не получается, возвращает default.
    """
    try:
        return float(value)
    except Exception:
        return default


class TTLCache:
    """
    Простой кэш с временем жизни.

    Например:
    - свечи можно хранить 45 секунд;
    - стакан можно хранить 30 секунд.

    Это уменьшает количество запросов к Binance.
    """

    def __init__(self, ttl_seconds: int = 30, max_size: int = 2000):
        self.ttl = ttl_seconds
        self.max_size = max_size
        self._store: Dict[str, tuple] = {}

    def get(self, key: str):
        now = time.time()

        if key in self._store:
            ts, val = self._store[key]

            if now - ts < self.ttl:
                return val

            del self._store[key]

        return None

    def set(self, key: str, value):
        # Если кэш сильно разросся, чистим старые ключи.
        if len(self._store) > self.max_size:
            self._cleanup()

        self._store[key] = (time.time(), value)

    def _cleanup(self):
        now = time.time()

        self._store = {
            k: v
            for k, v in self._store.items()
            if now - v[0] < self.ttl
        }


class FilterFailureError(Exception):
    """
    [НОВОЕ]
    Исключение для ошибки Binance -1013 Filter failure.
    Бросается в _request() только для ордерных запросов (is_order=True).
    Ловится в методах размещения ордеров для реактивного обновления кэша.
    """
    def __init__(self, symbol: str):
        self.symbol = symbol
        super().__init__(f"Filter failure for {symbol}")


class ExchangeFiltersCache:
    """
    [НОВОЕ]
    Кэш exchangeInfo фильтров.
    
    Архитектура (по документу АЛГОРИТМ, раздел 3):
    - Инициализируется блокирующе до старта торгового цикла
    - Фоновое обновление раз в 2-4 часа (атомарная замена всего снапшота)
    - Реактивное обновление при -1013 Filter failure
    
    Хранит по символу:
    - stepSize, tickSize, minQty, maxQty, minNotional
    - pricePrecision, quantityPrecision
    - status, MARKET_LOT_SIZE
    - leverageBracket (отдельно)
    """
    
    def __init__(self, api_client):
        self.api = api_client
        # bsym -> dict с фильтрами
        self._symbols: Dict[str, dict] = {}
        # bsym -> leverage bracket
        self._leverage_brackets: Dict[str, dict] = {}
        self._lock = asyncio.Lock()
        self._initialized = False
        self._last_update = 0.0
        self._updater_task: Optional[asyncio.Task] = None
        self._stop_flag = False
    
    # ------------------------------------------------------------
    # ИНИЦИАЛИЗАЦИЯ
    # ------------------------------------------------------------
    async def initialize(self):
        """
        Блокирующая инициализация.
        Должна вызываться до старта торгового цикла.
        При неудаче — retry с backoff.
        Если все попытки провалились — бросает исключение,
        и торговый цикл не стартует.
        """
        delays = Config.FILTERS_CACHE_INIT_RETRY_DELAYS
        retries = Config.FILTERS_CACHE_INIT_RETRIES
        
        for attempt in range(retries + 1):
            try:
                await self._load_all()
                self._initialized = True
                self._last_update = time.time()
                log.info(
                    f"ExchangeFiltersCache инициализирован: "
                    f"{len(self._symbols)} символов, "
                    f"{len(self._leverage_brackets)} leverage brackets"
                )
                return
            except Exception as e:
                if attempt < retries:
                    delay = delays[min(attempt, len(delays) - 1)]
                    log.warning(
                        f"ExchangeFiltersCache: ошибка инициализации "
                        f"(попытка {attempt + 1}/{retries + 1}): {e}. "
                        f"Retry через {delay}с"
                    )
                    await asyncio.sleep(delay)
                else:
                    log.error(
                        f"ExchangeFiltersCache: не удалось инициализировать "
                        f"после {retries + 1} попыток: {e}"
                    )
                    raise
    
    async def _load_all(self):
        """
        Загружает все exchangeInfo и leverageBracket.
        Атомарная замена всего снапшота целиком.
        """
        # 1. exchangeInfo (весь список)
        data = await self.api._request("GET", "/fapi/v1/exchangeInfo")
        if not data or not isinstance(data, dict):
            raise RuntimeError("Не удалось загрузить exchangeInfo")
        
        new_symbols = {}
        for s in data.get("symbols", []):
            sname = s.get("symbol", "")
            if not sname.endswith("USDT"):
                continue
            new_symbols[sname] = self._parse_symbol_info(s)
        
        if not new_symbols:
            raise RuntimeError("exchangeInfo вернул 0 USDT-символов")
        
        # 2. leverageBracket (приватный эндпоинт, требует подписи)
        new_brackets = {}
        try:
            brackets_data = await self.api._request(
                "GET",
                "/fapi/v1/leverageBracket",
                signed=True,   # [ИСПРАВЛЕНО] эндпоинт приватный, требует timestamp+signature
            )
            if brackets_data and isinstance(brackets_data, list):
                for item in brackets_data:
                    sname = item.get("symbol", "")
                    new_brackets[sname] = item
        except Exception as e:
            log.debug(f"leverageBracket не загрузился (не критично): {e}")
        
        # 3. Атомарная замена
        async with self._lock:
            self._symbols = new_symbols
            self._leverage_brackets = new_brackets
    
    def _parse_symbol_info(self, s: dict) -> dict:
        """Парсит информацию о символе из exchangeInfo."""
        info = {
            "stepSize": 0.001,
            "quantityPrecision": s.get("quantityPrecision", 3),
            "minQty": 0.0,
            "maxQty": 0.0,
            "minNotional": 5.0,
            "tickSize": 0.00000001,
            "pricePrecision": s.get("pricePrecision", 8),
            "status": s.get("status", "TRADING"),
            "market_lot_size": None,
        }
        
        for f in s.get("filters", []):
            ft = f.get("filterType")
            if ft == "LOT_SIZE":
                info["stepSize"] = _safe_float(f.get("stepSize"), 0.001)
                info["minQty"] = _safe_float(f.get("minQty"), 0.0)
                info["maxQty"] = _safe_float(f.get("maxQty"), 0.0)
            elif ft == "PRICE_FILTER":
                info["tickSize"] = _safe_float(f.get("tickSize"), 0.00000001)
            elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                info["minNotional"] = _safe_float(
                    f.get("notional", f.get("minNotional")), 5.0
                )
            elif ft == "MARKET_LOT_SIZE":
                info["market_lot_size"] = {
                    "stepSize": _safe_float(f.get("stepSize"), 0.001),
                    "minQty": _safe_float(f.get("minQty"), 0.0),
                    "maxQty": _safe_float(f.get("maxQty"), 0.0),
                }
        
        return info
    
    # ------------------------------------------------------------
    # РЕАКТИВНОЕ ОБНОВЛЕНИЕ
    # ------------------------------------------------------------
    async def refresh_symbol(self, bsym: str) -> bool:
        """
        Реактивное обновление по символу при -1013.
        Делает точечный запрос exchangeInfo?symbol=XXX.
        Возвращает True, если обновление успешно.
        """
        try:
            data = await self.api._request(
                "GET",
                "/fapi/v1/exchangeInfo",
                params={"symbol": bsym},
            )
            if not data or not isinstance(data, dict):
                return False
            
            symbols = data.get("symbols", [])
            if not symbols:
                return False
            
            s = symbols[0]
            info = self._parse_symbol_info(s)
            
            async with self._lock:
                self._symbols[bsym] = info
            
            log.info(
                f"ExchangeFiltersCache: реактивное обновление {bsym} "
                f"(stepSize={info['stepSize']}, tickSize={info['tickSize']}, "
                f"minNotional={info['minNotional']})"
            )
            return True
        except Exception as e:
            log.error(f"ExchangeFiltersCache: ошибка обновления {bsym}: {e}")
            return False
    
    # ------------------------------------------------------------
    # ГЕТТЕРЫ
    # ------------------------------------------------------------
    def get(self, bsym: str) -> Optional[dict]:
        """Получить информацию о символе."""
        return self._symbols.get(bsym)
    
    def get_leverage_bracket(self, bsym: str) -> Optional[dict]:
        """Получить leverage bracket."""
        return self._leverage_brackets.get(bsym)
    
    def is_initialized(self) -> bool:
        """Проверка инициализации."""
        return self._initialized
    
    def get_stats(self) -> dict:
        """Статистика кэша для диагностики."""
        return {
            "initialized": self._initialized,
            "symbols_count": len(self._symbols),
            "brackets_count": len(self._leverage_brackets),
            "last_update": self._last_update,
            "age_sec": time.time() - self._last_update if self._last_update else 0,
        }
    
    # ------------------------------------------------------------
    # ФОНОВОЕ ОБНОВЛЕНИЕ
    # ------------------------------------------------------------
    def start_background_updater(self):
        """Запускает фоновое обновление раз в FILTERS_CACHE_UPDATE_INTERVAL."""
        if self._updater_task is not None:
            return
        self._stop_flag = False
        self._updater_task = asyncio.create_task(self._background_updater())
        log.info(
            f"ExchangeFiltersCache: фоновое обновление запущено "
            f"(интервал {Config.FILTERS_CACHE_UPDATE_INTERVAL}с)"
        )
    
    async def stop_background_updater(self):
        """Останавливает фоновое обновление."""
        self._stop_flag = True
        if self._updater_task is not None:
            self._updater_task.cancel()
            try:
                await self._updater_task
            except asyncio.CancelledError:
                pass
            self._updater_task = None
            log.info("ExchangeFiltersCache: фоновое обновление остановлено")
    
    async def _background_updater(self):
        """Фоновое обновление раз в FILTERS_CACHE_UPDATE_INTERVAL."""
        interval = Config.FILTERS_CACHE_UPDATE_INTERVAL
        while not self._stop_flag:
            try:
                await asyncio.sleep(interval)
                if self._stop_flag:
                    break
                
                log.debug("ExchangeFiltersCache: фоновое обновление...")
                await self._load_all()
                self._last_update = time.time()
                log.debug(
                    f"ExchangeFiltersCache: обновлено "
                    f"({len(self._symbols)} символов)"
                )
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.warning(f"ExchangeFiltersCache: ошибка фонового обновления: {e}")
                # При ошибке ждём меньше и пробуем снова
                await asyncio.sleep(60)


# Кэш свечей.
klines_cache = TTLCache(ttl_seconds=45)

# Кэш стакана.
orderbook_cache = TTLCache(ttl_seconds=30)


class BinanceFuturesRestClient:
    """
    REST-клиент для Binance USDT-M Futures.
    """

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        session: aiohttp.ClientSession,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = session
        self.base = Config.BINANCE_BASE

        # Кэш информации о символах: stepSize, minQty, minNotional и т.д.
        # self._symbol_info_cache: Dict[str, dict] = {}
        self.filters_cache = ExchangeFiltersCache(self)

        # Символы, для которых уже пытались выставить плечо.
        self._leverage_set: set = set()

        # [НОВОЕ] Смещение времени между компьютером и сервером Binance.
        self._time_offset_ms = 0.0

        # [НОВОЕ] Флаг, что время синхронизировано.
        self._time_synced = False

        # [НОВОЕ] Локи, чтобы параллельные задачи не ломали друг друга.
        self._time_lock = asyncio.Lock()
        # self._exchange_info_lock = asyncio.Lock()
        self._leverage_lock = asyncio.Lock()

    # ================================================================
    # СЛУЖЕБНЫЕ ФУНКЦИИ
    # ================================================================

    async def _request_raw(self, method: str, path: str, params: dict = None):
        """
        Простой запрос без подписи и без сложной логики повторов.
        Используется, например, для получения времени сервера.
        """
        url = f"{self.base}{path}"

        try:
            async with self.session.request(
                method,
                url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
        except Exception:
            pass

        return None

    async def _ensure_time_synced(self):
        """
        [НОВОЕ]
        Синхронизирует время с сервером Binance.

        Если локальное время компьютера немного неправильное,
        signed-запросы могут отклоняться с ошибкой -1021.

        Здесь мы считаем разницу:
        server_time - local_time.
        """
        if self._time_synced:
            return

        async with self._time_lock:
            if self._time_synced:
                return

            try:
                local_before = time.time() * 1000.0

                data = await self._request_raw("GET", "/fapi/v1/time")

                local_after = time.time() * 1000.0

                if data and "serverTime" in data:
                    server_time = _safe_float(data.get("serverTime"))

                    # Примерная задержка сети в одну сторону.
                    latency = (local_after - local_before) / 2.0

                    self._time_offset_ms = server_time - local_before - latency
                    self._time_synced = True

            except Exception as e:
                log.debug(f"Не удалось синхронизировать время: {e}")

    def _sign(self, params: dict) -> dict:
        """
        Подписывает запрос для Binance.

        [ИСПРАВЛЕНО]
        Раньше timestamp был только локальный:
            int(time.time() * 1000)

        Теперь используется локальное время + поправка на сервер Binance:
            int(time.time() * 1000 + self._time_offset_ms)
        """
        params = dict(params)

        params["timestamp"] = int(time.time() * 1000 + self._time_offset_ms)
        params.setdefault("recvWindow", 5000)

        query = urllib.parse.urlencode(params)

        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

        params["signature"] = signature

        return params

    async def _request(
        self,
        method: str,
        path: str,
        params: dict = None,
        signed: bool = False,
        is_order: bool = False,
    ):
        """
        Универсальный запрос к Binance.

        [ИСПРАВЛЕНО]
        is_order=True используется для отправки ордеров.

        Для ордеров мы НЕ делаем слепые повторы при таймауте.
        Это защищает от ситуации:
        1. Бот отправил market-ордер.
        2. Binance принял ордер.
        3. Ответ не дошёл из-за сети.
        4. Старый бот отправил бы ордер повторно.

        Теперь при сетевой ошибке ордера нужно отдельно проверять
        статус по newClientOrderId.
        """

        # Для signed-запросов сначала проверяем время.
        if signed:
            await self._ensure_time_synced()

        url = f"{self.base}{path}"

        headers = {}
        if self.api_key:
            headers["X-MBX-APIKEY"] = self.api_key

        if signed:
            params = self._sign(params or {})

        # Для ордеров делаем меньше попыток и не повторяем при таймауте.
        attempts = 2 if is_order else 3

        for attempt in range(attempts):
            try:
                async with self.session.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:

                    if resp.status == 200:
                        return await resp.json(content_type=None)

                    # Rate limit.
                    if resp.status in (429, 418):
                        delay = Config.HTTP_RETRY_DELAYS[
                            min(attempt, len(Config.HTTP_RETRY_DELAYS) - 1)
                        ]

                        log.warning(
                            f"Binance rate limit {resp.status} — ждём {delay}с"
                        )

                        await asyncio.sleep(delay)
                        continue

                    text = await resp.text()

                    # Ордер не найден при проверке по clientOrderId.
                    # Это не всегда ошибка, иногда ордер просто ещё не создан.
                    if "-2013" in text:
                        log.debug(f"Order not found: {path}")
                        return None

                    if "-2013" in text:
                        log.debug(f"Order not found: {path}")
                        return None

                    # [ИЗМЕНЕНО] Filter failure — реактивное обновление кэша
                    if "-1013" in text:
                        bsym = (params or {}).get("symbol")
                        if is_order and bsym:
                            # Бросаем исключение — его поймает метод размещения ордера
                            raise FilterFailureError(bsym)
                        log.warning(f"Filter failure (non-order): {path} {text}")
                        return None

                    # Ошибка времени.
                    if "-1021" in text:
                        log.warning(
                            "Binance -1021: проблема времени, "
                            "пробуем пересинхронизировать"
                        )
                        self._time_synced = False

                    log.error(f"Binance API error {resp.status} {path}: {text}")

                    return None

            except asyncio.TimeoutError:
                # [ИСПРАВЛЕНО]
                # Для ордеров не повторяем запрос автоматически.
                if is_order:
                    log.error(
                        f"Таймаут ордера {path}. "
                        f"Повторная отправка без проверки запрещена."
                    )
                    return None

                log.warning(
                    f"Таймаут запроса {path}, попытка {attempt + 1}/{attempts}"
                )

                await asyncio.sleep(2 ** attempt)

            except Exception as e:
                # [ИСПРАВЛЕНО]
                # Для ордеров не повторяем запрос автоматически.
                if is_order:
                    log.error(f"Ошибка ордера {path}: {e}")
                    return None

                log.debug(f"Request error {path}: {e}")

                await asyncio.sleep(2 ** attempt)

        return None

    # ================================================================
    # ПУБЛИЧНЫЕ ДАННЫЕ
    # ================================================================

    async def get_tickers(self) -> List[dict]:
        """
        Возвращает список всех USDT-пар с Binance Futures.

        [ИСПРАВЛЕНО]
        Добавлены high_24h и low_24h.
        Раньше scanner считал "24h" high/low по последним 24 минутам,
        что было неправильно.
        """
        data = await self._request("GET", "/fapi/v1/ticker/24hr")

        if not data or not isinstance(data, list):
            return []

        result = []

        for t in data:
            sym = t.get("symbol", "")

            if not sym.endswith("USDT"):
                continue

            result.append(
                {
                    "currency_pair": to_internal_symbol(sym),
                    "quote_volume": _safe_float(t.get("quoteVolume")),
                    "last": _safe_float(t.get("lastPrice")),
                    "change_percentage": _safe_float(t.get("priceChangePercent")),

                    # [НОВОЕ]
                    # Реальные 24-часовые high и low.
                    "high_24h": _safe_float(t.get("highPrice")),
                    "low_24h": _safe_float(t.get("lowPrice")),
                }
            )

        return result

    async def get_klines(
        self,
        symbol: str,
        interval: str = "1m",
        limit: int = 350,
    ) -> Optional[list]:
        """
        Возвращает свечи.

        Формат после обработки:
        [ts, volume, close, high, low, open, quote_vol]

        Этот формат используется в logger.parse_klines().
        """
        cached = klines_cache.get(f"{symbol}:{interval}:{limit}")

        if cached is not None:
            return cached

        bsym = to_binance_symbol(symbol)

        data = await self._request(
            "GET",
            "/fapi/v1/klines",
            params={
                "symbol": bsym,
                "interval": interval,
                "limit": limit,
            },
        )

        if not data or not isinstance(data, list):
            return None

        # Binance формат:
        # [
        #   open_time,
        #   open,
        #   high,
        #   low,
        #   close,
        #   volume,
        #   close_time,
        #   quote_volume,
        #   ...
        # ]
        #
        # Делаем формат:
        # [ts, volume, close, high, low, open, quote_vol]
        remapped = []

        for k in data:
            try:
                remapped.append(
                    [
                        k[0],
                        _safe_float(k[5]),
                        _safe_float(k[4]),
                        _safe_float(k[2]),
                        _safe_float(k[3]),
                        _safe_float(k[1]),
                        _safe_float(k[7]),
                    ]
                )
            except Exception:
                pass

        klines_cache.set(f"{symbol}:{interval}:{limit}", remapped)

        return remapped

    async def get_orderbook(self, symbol: str, limit: int = 20) -> Optional[dict]:
        """
        Возвращает стакан: bids и asks.
        """
        cached = orderbook_cache.get(f"{symbol}:{limit}")

        if cached is not None:
            return cached

        bsym = to_binance_symbol(symbol)

        allowed = [5, 10, 20, 50, 100, 500, 1000]

        api_limit = min((l for l in allowed if l >= limit), default=20)

        data = await self._request(
            "GET",
            "/fapi/v1/depth",
            params={
                "symbol": bsym,
                "limit": api_limit,
            },
        )

        if not data:
            return None

        result = {
            "bids": data.get("bids", []),
            "asks": data.get("asks", []),
        }

        orderbook_cache.set(f"{symbol}:{limit}", result)

        return result

    async def get_last_price(self, symbol: str) -> Optional[float]:
        """
        Возвращает последнюю цену символа.
        """
        bsym = to_binance_symbol(symbol)

        data = await self._request(
            "GET",
            "/fapi/v1/ticker/price",
            params={"symbol": bsym},
        )

        if data and "price" in data:
            price = _safe_float(data.get("price"))

            if price > 0:
                return price

        return None

    async def get_balance(self, asset: str = "USDT") -> float:
        """
        Возвращает доступный баланс по активу.
        """
        data = await self._request(
            "GET",
            "/fapi/v2/balance",
            signed=True,
        )

        if data and isinstance(data, list):
            for acc in data:
                if acc.get("asset") == asset:
                    return _safe_float(acc.get("availableBalance"))

        return 0.0

    # ================================================================
    # ИНФОРМАЦИЯ О СИМВОЛАХ
    # ================================================================

    async def _get_symbol_info(self, bsym: str) -> dict:
        """
        Возвращает информацию о символе Binance.
        [ИЗМЕНЕНО] Теперь использует filters_cache.
        Если символ не в кэше (например, в тестах без инициализации),
        делает точечную загрузку.
        """
        info = self.filters_cache.get(bsym)
        if info is not None:
            return info
        
        # Fallback: точечная загрузка (не должно происходить в продакшене)
        log.warning(f"{bsym}: символ не в кэше filters, точечная загрузка")
        ok = await self.filters_cache.refresh_symbol(bsym)
        if ok:
            info = self.filters_cache.get(bsym)
            if info is not None:
                return info
        
        # Последний fallback — дефолтные значения
        log.warning(f"{bsym}: использую дефолтные значения фильтров")
        return {
            "stepSize": 0.001,
            "quantityPrecision": 3,
            "minQty": 0.0,
            "maxQty": 0.0,
            "minNotional": 5.0,
            "tickSize": 0.00000001,
            "pricePrecision": 8,
            "status": "TRADING",
            "market_lot_size": None,
        }

    # ================================================================
    # ОКРУГЛЕНИЕ КОЛИЧЕСТВА И ЦЕНЫ
    # ================================================================

    async def _round_qty(self, bsym: str, raw_qty: float) -> float:
        """
        Округляет количество монет под stepSize и minQty Binance.

        [ИСПРАВЛЕНО]
        Используется Decimal, чтобы уменьшить ошибки float.
        Например, чтобы не получалось 0.30000000000000004.
        """
        info = await self._get_symbol_info(bsym)

        # print('Info = ', info)

        step = info.get("stepSize", 0.001)
        precision = info.get("quantityPrecision", 3)
        min_qty = info.get("minQty", 0.0)

        # print('Step = ', step, 'Precision = ', precision, 'Min_qty = ', min_qty)

        if raw_qty <= 0 or step <= 0:
            return 0.0

        try:
            step_d = Decimal(str(step))
            raw_d = Decimal(str(raw_qty))

            # print('Step_d = ', step_d, 'Raw_d = ', raw_d)

            qty_d = (raw_d / step_d).to_integral_value(rounding=ROUND_DOWN)
            # print('Qty_d1 = ', qty_d)
            qty_d = qty_d * step_d

            # print('Qty_d2 = ', qty_d)

            qty = float(qty_d)
            # print('Qty = ', qty)

        except Exception:
            qty = math.floor(raw_qty / step) * step

        qty = round(qty, precision)

        if qty < min_qty:
            return 0.0
        # print('Qty OUT = ', qty)
        return qty

    async def _round_price(self, bsym: str, raw_price: float) -> float:
        """
        [НОВОЕ]
        Округляет цену под tickSize и pricePrecision Binance.

        Это нужно для limit/stop ордеров.
        Раньше цена могла быть с лишними знаками,
        и Binance мог отклонить ордер.
        """
        info = await self._get_symbol_info(bsym)

        tick = info.get("tickSize", 0.0)
        precision = info.get("pricePrecision", 8)

        if raw_price <= 0:
            return 0.0

        if tick <= 0:
            return round(raw_price, precision)

        try:
            tick_d = Decimal(str(tick))
            price_d = Decimal(str(raw_price))

            price_d = (price_d / tick_d).quantize(
                Decimal("1"),
                rounding=ROUND_HALF_UP,
            )

            price_d = price_d * tick_d

            price = float(price_d)

        except Exception:
            price = round(raw_price / tick) * tick

        return round(price, precision)

    # ================================================================
    # ПЛЕЧО
    # ================================================================

    async def ensure_leverage(self, bsym: str, leverage: int = 1) -> bool:
        """
        Выставляет плечо для символа.

        [ИСПРАВЛЕНО]
        Раньше бот всегда считал, что плечо выставлено,
        даже если Binance вернул ошибку.

        Теперь проверяем ответ.
        """
        async with self._leverage_lock:
            if bsym in self._leverage_set:
                return True

            resp = await self._request(
                "POST",
                "/fapi/v1/leverage",
                params={
                    "symbol": bsym,
                    "leverage": leverage,
                },
                signed=True,
            )

            if resp:
                try:
                    resp_symbol = str(resp.get("symbol", ""))
                    resp_leverage = float(resp.get("leverage", 0))
                except Exception:
                    resp_symbol = ""
                    resp_leverage = 0.0

                if resp_symbol == bsym or resp_leverage == float(leverage):
                    self._leverage_set.add(bsym)
                    log.info(f"{bsym}: плечо выставлено x{leverage}")
                    return True

            log.error(f"{bsym}: не удалось выставить плечо x{leverage}")

            return False

    # ================================================================
    # НОРМАЛИЗАЦИЯ ОРДЕРОВ
    # ================================================================

    def _normalize_order(self, resp: Optional[dict]) -> Optional[dict]:
        """
        Приводит ответ Binance к удобному виду.
        Поддерживает и обычные ордера, и алгоритмические (SL/TP).
        """
        if not resp:
            return None
        status_map = {
            "FILLED": "filled",
            "PARTIALLY_FILLED": "partially_filled",
            "NEW": "new",
            "CANCELED": "canceled",
            "EXPIRED": "expired",
            "REJECTED": "rejected",
        }
        executed_qty = _safe_float(resp.get("executedQty"))
        cum_quote = _safe_float(resp.get("cumQuote"))
        avg_price = _safe_float(resp.get("avgPrice"))
        if executed_qty > 0 and cum_quote > 0:
            avg_price = cum_quote / executed_qty
        
        # Для алгоритмических ордеров Binance возвращает clientAlgoId и algoId
        client_id = resp.get("clientAlgoId") or resp.get("clientOrderId")
        order_id = resp.get("algoId") or resp.get("orderId")

        return {
            "status": status_map.get(
                resp.get("status", ""),
                str(resp.get("status", "")).lower(),
            ),
            "filled_amount": executed_qty,
            "avg_price": avg_price,
            "order_id": order_id,
            "client_order_id": client_id,
        }

    async def get_order_by_client_id(
        self,
        symbol: str,
        client_order_id: str,
    ) -> Optional[dict]:
        """
        [НОВОЕ]
        Получает статус ордера по нашему clientOrderId.

        Это нужно, если ответ на отправку ордера потерялся,
        но сам ордер мог быть создан на бирже.
        """
        bsym = to_binance_symbol(symbol)

        resp = await self._request(
            "GET",
            "/fapi/v1/order",
            params={
                "symbol": bsym,
                "origClientOrderId": client_order_id,
            },
            signed=True,
        )

        return self._normalize_order(resp)

    async def _wait_order_fill(
        self,
        symbol: str,
        client_order_id: str,
        initial_order: Optional[dict],
    ) -> Optional[dict]:
        """
        [НОВОЕ]
        Если market-ордер вернулся как NEW или без filled_amount,
        несколько раз проверяем его статус.

        Это уменьшает риск ситуации, когда ордер исполнился,
        но бот посчитал его ошибкой.
        """
        if initial_order:
            status = initial_order.get("status")
            filled = initial_order.get("filled_amount", 0.0)

            if status in ("filled", "partially_filled") and filled > 0:
                return initial_order

        for _ in range(5):
            await asyncio.sleep(0.2)

            order = await self.get_order_by_client_id(symbol, client_order_id)

            if order:
                status = order.get("status")
                filled = order.get("filled_amount", 0.0)

                if status in ("filled", "partially_filled") and filled > 0:
                    return order

        return initial_order

    # ================================================================
    # ОТКРЫТИЕ ПОЗИЦИЙ
    # ================================================================

    async def place_market_buy(self, symbol: str, quote_qty: float) -> Optional[dict]:
        """
        Открывает LONG рыночным ордером.

        quote_qty — это сумма в USDT.
        Например, quote_qty=100 означает купить примерно на 100 USDT.
        """
        bsym = to_binance_symbol(symbol)

        await self.ensure_leverage(bsym, Config.LEVERAGE)

        ticker = await self._request(
            "GET",
            "/fapi/v1/ticker/price",
            params={"symbol": bsym},
        )

        if not ticker:
            return None

        price = _safe_float(ticker.get("price"))

        if price <= 0:
            return None

        raw_qty = quote_qty / price

        qty = await self._round_qty(bsym, raw_qty)

        if qty <= 0:
            log.warning(
                f"{symbol}: qty {raw_qty:.8f} округлился до 0 "
                f"(stepSize/minQty)"
            )
            return None

        info = await self._get_symbol_info(bsym)

        min_notional = info.get("minNotional", 5.0)

        estimated_notional = qty * price

        # [ИСПРАВЛЕНО]
        # Проверяем, что размер ордера проходит minNotional.
        if estimated_notional < min_notional * 1.01:
            log.warning(
                f"{symbol}: notional {estimated_notional:.2f} "
                f"< minNotional {min_notional:.2f}"
            )
            return None

        # [НОВОЕ]
        # Уникальный ID ордера.
        client_order_id = f"wavex{uuid.uuid4().hex[:20]}"

        params = {
            "symbol": bsym,
            "side": "BUY",
            "type": "MARKET",
            "quantity": qty,
            "newClientOrderId": client_order_id,
        }

        # [НОВОЕ] Retry при -1013 Filter failure
        resp = None
        for attempt in range(2):
            try:
                resp = await self._request(
                    "POST", "/fapi/v1/order", params=params,
                    signed=True, is_order=True,
                )
                break
            except FilterFailureError as e:
                if attempt == 0:
                    log.warning(
                        f"{symbol}: -1013 Filter failure, "
                        f"реактивное обновление фильтра..."
                    )
                    ok = await self.filters_cache.refresh_symbol(e.symbol)
                    if not ok:
                        log.error(f"{symbol}: не удалось обновить фильтр")
                        return None
                    # Пересчитываем qty с новыми фильтрами
                    qty = await self._round_qty(bsym, raw_qty)
                    if qty <= 0:
                        return None
                    # Проверка minNotional после пересчёта
                    info = await self._get_symbol_info(bsym)
                    min_notional = info.get("minNotional", 5.0)
                    if qty * price < min_notional * 1.01:
                        return None
                    # Новый client_order_id для повторной попытки
                    client_order_id = f"wavex{uuid.uuid4().hex[:20]}"
                    params = {
                        "symbol": bsym, "side": "BUY", "type": "MARKET",
                        "quantity": qty, "newClientOrderId": client_order_id,
                    }
                else:
                    log.error(f"{symbol}: повторный -1013 после обновления, реджект")
                    return None

        order = self._normalize_order(resp)

        # [НОВОЕ]
        # Если ответ потерялся или ордер ещё NEW, проверяем статус.
        order = await self._wait_order_fill(symbol, client_order_id, order)

        # Если биржа не вернула среднюю цену, используем цену тикера.
        if order and order.get("filled_amount", 0) > 0:
            if order.get("avg_price", 0) <= 0:
                order["avg_price"] = price

        return order

    async def place_market_sell_open(self, symbol: str, quote_qty: float) -> Optional[dict]:
        """
        Открывает SHORT рыночным ордером.

        quote_qty — это сумма в USDT.
        """
        bsym = to_binance_symbol(symbol)

        await self.ensure_leverage(bsym, Config.LEVERAGE)

        ticker = await self._request(
            "GET",
            "/fapi/v1/ticker/price",
            params={"symbol": bsym},
        )

        if not ticker:
            return None

        price = _safe_float(ticker.get("price"))

        if price <= 0:
            return None

        raw_qty = quote_qty / price

        qty = await self._round_qty(bsym, raw_qty)

        if qty <= 0:
            log.warning(
                f"{symbol}: qty {raw_qty:.8f} округлился до 0 "
                f"(stepSize/minQty)"
            )
            return None

        info = await self._get_symbol_info(bsym)

        min_notional = info.get("minNotional", 5.0)

        estimated_notional = qty * price

        if estimated_notional < min_notional * 1.01:
            log.warning(
                f"{symbol}: notional {estimated_notional:.2f} "
                f"< minNotional {min_notional:.2f}"
            )
            return None

        client_order_id = f"wavex{uuid.uuid4().hex[:20]}"

        params = {
            "symbol": bsym,
            "side": "SELL",
            "type": "MARKET",
            "quantity": qty,
            "newClientOrderId": client_order_id,
        }

        # [НОВОЕ] Retry при -1013 Filter failure
        resp = None
        for attempt in range(2):
            try:
                resp = await self._request(
                    "POST", "/fapi/v1/order", params=params,
                    signed=True, is_order=True,
                )
                break
            except FilterFailureError as e:
                if attempt == 0:
                    log.warning(
                        f"{symbol}: -1013 Filter failure, "
                        f"реактивное обновление фильтра..."
                    )
                    ok = await self.filters_cache.refresh_symbol(e.symbol)
                    if not ok:
                        log.error(f"{symbol}: не удалось обновить фильтр")
                        return None
                    # Пересчитываем qty с новыми фильтрами
                    qty = await self._round_qty(bsym, raw_qty)
                    if qty <= 0:
                        return None
                    # Проверка minNotional после пересчёта
                    info = await self._get_symbol_info(bsym)
                    min_notional = info.get("minNotional", 5.0)
                    if qty * price < min_notional * 1.01:
                        return None
                    # Новый client_order_id для повторной попытки
                    client_order_id = f"wavex{uuid.uuid4().hex[:20]}"
                    params = {
                        "symbol": bsym, "side": "BUY", "type": "MARKET",
                        "quantity": qty, "newClientOrderId": client_order_id,
                    }
                else:
                    log.error(f"{symbol}: повторный -1013 после обновления, реджект")
                    return None

        order = self._normalize_order(resp)
        order = await self._wait_order_fill(symbol, client_order_id, order)

        if order and order.get("filled_amount", 0) > 0:
            if order.get("avg_price", 0) <= 0:
                order["avg_price"] = price

        return order

    # ================================================================
    # ЗАКРЫТИЕ ПОЗИЦИЙ
    # ================================================================

    async def place_market_buy_close(self, symbol: str, quantity: float) -> Optional[dict]:
        """
        Закрывает SHORT рыночным ордером.

        Используется reduceOnly=true, чтобы случайно не открыть новый SHORT.
        """
        bsym = to_binance_symbol(symbol)

        qty = await self._round_qty(bsym, quantity)

        if qty <= 0:
            return None

        client_order_id = f"wavex{uuid.uuid4().hex[:20]}"

        params = {
            "symbol": bsym,
            "side": "BUY",
            "type": "MARKET",
            "quantity": qty,
            "reduceOnly": "true",
            "newClientOrderId": client_order_id,
        }

        resp = await self._request(
            "POST",
            "/fapi/v1/order",
            params=params,
            signed=True,
            is_order=True,
        )

        order = self._normalize_order(resp)
        order = await self._wait_order_fill(symbol, client_order_id, order)

        return order

    async def place_market_sell(self, symbol: str, quantity: float) -> Optional[dict]:
        """
        Закрывает LONG рыночным ордером.

        Используется reduceOnly=true, чтобы случайно не открыть новый SHORT.
        """
        bsym = to_binance_symbol(symbol)

        qty = await self._round_qty(bsym, quantity)

        if qty <= 0:
            return None

        client_order_id = f"wavex{uuid.uuid4().hex[:20]}"

        params = {
            "symbol": bsym,
            "side": "SELL",
            "type": "MARKET",
            "quantity": qty,
            "reduceOnly": "true",
            "newClientOrderId": client_order_id,
        }

        resp = await self._request(
            "POST",
            "/fapi/v1/order",
            params=params,
            signed=True,
            is_order=True,
        )

        order = self._normalize_order(resp)
        order = await self._wait_order_fill(symbol, client_order_id, order)

        return order

    async def place_limit_sell(
        self,
        symbol: str,
        quantity: float,
        price: float,
    ) -> Optional[dict]:
        """
        Закрывает LONG лимитным ордером.

        [ИСПРАВЛЕНО]
        Теперь цена округляется до tickSize.
        Раньше Binance мог отклонить ордер из-за неправильной точности цены.
        """
        bsym = to_binance_symbol(symbol)

        qty = await self._round_qty(bsym, quantity)

        if qty <= 0:
            return None

        price = await self._round_price(bsym, price)

        if price <= 0:
            return None

        client_order_id = f"wavex{uuid.uuid4().hex[:20]}"

        params = {
            "symbol": bsym,
            "side": "SELL",
            "type": "LIMIT",
            "timeInForce": "GTC",
            "quantity": qty,
            "price": price,
            "reduceOnly": "true",
            "newClientOrderId": client_order_id,
        }

        # [НОВОЕ] Retry при -1013 Filter failure
        resp = None
        for attempt in range(2):
            try:
                resp = await self._request(
                    "POST",
                    "/fapi/v1/order",
                    params=params,
                    signed=True,
                    is_order=True,
                )
                break
            except FilterFailureError as e:
                if attempt == 0:
                    log.warning(
                        f"{symbol}: -1013 Filter failure на limit-ордере, "
                        f"реактивное обновление фильтра..."
                    )
                    ok = await self.filters_cache.refresh_symbol(e.symbol)
                    if not ok:
                        log.error(f"{symbol}: не удалось обновить фильтр, реджект")
                        return None
                    
                    # Пересчитываем qty и price с новыми фильтрами
                    qty = await self._round_qty(bsym, quantity)
                    if qty <= 0:
                        log.warning(
                            f"{symbol}: qty {quantity:.8f} округлился до 0 "
                            f"после обновления фильтров"
                        )
                        return None
                    
                    price = await self._round_price(bsym, price)
                    if price <= 0:
                        return None
                    
                    # Новый client_order_id для повторной попытки
                    client_order_id = f"wavex{uuid.uuid4().hex[:20]}"
                    params = {
                        "symbol": bsym,
                        "side": "SELL",
                        "type": "LIMIT",
                        "timeInForce": "GTC",
                        "quantity": qty,
                        "price": price,
                        "reduceOnly": "true",
                        "newClientOrderId": client_order_id,
                    }
                    log.info(
                        f"{symbol}: повтор limit-ордера с новыми фильтрами "
                        f"(qty={qty}, price={price})"
                    )
                else:
                    log.error(
                        f"{symbol}: повторный -1013 после обновления фильтров, "
                        f"реджект limit-ордера"
                    )
                    return None

        order = self._normalize_order(resp)
        order = await self._wait_order_fill(symbol, client_order_id, order)

        return order

    async def get_algo_order_status(
        self,
        symbol: str,
        algo_id: Optional[int] = None,
        client_algo_id: Optional[str] = None,
    ) -> Optional[dict]:
        """
        Проверяет статус алгоритмического (условного) ордера.
        Требует либо algo_id, либо client_algo_id.
        """
        bsym = to_binance_symbol(symbol)
        params = {"symbol": bsym}
        
        if algo_id is not None:
            params["algoId"] = algo_id
        elif client_algo_id is not None:
            params["clientAlgoId"] = client_algo_id
        else:
            log.error("Для проверки algo-ордера нужен либо algo_id, либо client_algo_id")
            return None

        resp = await self._request(
            "GET",
            "/fapi/v1/algoOrder",
            params=params,
            signed=True,
        )
        
        if resp:
            return {
                "algo_id": resp.get("algoId"),
                "client_algo_id": resp.get("clientAlgoId"),
                "status": resp.get("algoStatus"),  # NEW, CANCELED, FILLED, EXPIRED
                "trigger_price": _safe_float(resp.get("triggerPrice")),
                "actual_price": _safe_float(resp.get("actualPrice")),
                "close_position": resp.get("closePosition", False),
            }
        return None

    async def cancel_order(
        self,
        symbol: str,
        order_id: int,
        client_order_id: Optional[str] = None,
        is_algo: bool = False,
    ) -> bool:
        """
        Отменяет ордер.
        Если is_algo=True и передан client_order_id, использует эндпоинт
        для алгоритмических ордеров (SL/TP, созданные через /fapi/v1/algoOrder).
        Иначе использует стандартный эндпоинт для обычных ордеров.
        """
        bsym = to_binance_symbol(symbol)

        if is_algo and client_order_id:
            # Для алгоритмических ордеров (STOP_MARKET, TAKE_PROFIT_MARKET)
            result = await self._request(
                "DELETE",
                "/fapi/v1/algoOrder",
                params={"symbol": bsym, "clientAlgoId": client_order_id},
                signed=True,
                is_order=True,
            )
        else:
            # Для обычных ордеров (MARKET, LIMIT)
            result = await self._request(
                "DELETE",
                "/fapi/v1/order",
                params={"symbol": bsym, "orderId": order_id},
                signed=True,
                is_order=True,
            )

        return result is not None

    async def cancel_all_algo_orders(self, symbol: str) -> bool:
        """
        Отменяет все алгоритмические (условные) ордера по символу.
        Используется как подстраховка при форс-закрытии позиции.
        """
        bsym = to_binance_symbol(symbol)
        result = await self._request(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            params={"symbol": bsym},
            signed=True,
            is_order=True,
        )
        # Binance возвращает код 200 и список отменённых ордеров,
        # либо пустой список, если их не было.
        return result is not None

    # ================================================================
    # STOP / TAKE-PROFIT ОРДЕРА
    # ================================================================
    async def place_stop_market(
        self,
        symbol: str,
        side: str,
        stop_price: float,
        quantity: Optional[float] = None,
        close_position: bool = False,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> Optional[dict]:
        """Размещает STOP_MARKET ордер через Algo Order API."""
        bsym = to_binance_symbol(symbol)
        rounded_price = await self._round_price(bsym, stop_price)
        if rounded_price <= 0:
            log.error(f"{symbol}: некорректная stop_price={stop_price}")
            return None

        params = {
            "algoType": "CONDITIONAL",          # [НОВОЕ] Обязательно для условных ордеров
            "symbol": bsym,
            "side": side.upper(),
            "type": "STOP_MARKET",
            "triggerPrice": str(rounded_price), # [ИСПРАВЛЕНО] triggerPrice вместо stopPrice
            "workingType": "MARK_PRICE",
        }

        if close_position:
            params["closePosition"] = "true"
        else:
            if quantity is None or quantity <= 0:
                log.error(f"{symbol}: для STOP_MARKET без closePosition нужно указать quantity")
                return None
            qty = await self._round_qty(bsym, quantity)
            if qty <= 0:
                log.warning(f"{symbol}: quantity округлился до 0")
                return None
            params["quantity"] = str(qty)
            if reduce_only:
                params["reduceOnly"] = "true"

        if client_order_id is None:
            client_order_id = f"wavex{uuid.uuid4().hex[:20]}"
        params["clientAlgoId"] = client_order_id  # [ИСПРАВЛЕНО] clientAlgoId вместо newClientOrderId

        # [НОВОЕ] Retry при -1013
        resp = None
        for attempt in range(2):
            try:
                resp = await self._request(
                    "POST", "/fapi/v1/algoOrder", params=params,
                    signed=True, is_order=True,
                )
                break
            except FilterFailureError as e:
                if attempt == 0:
                    log.warning(f"{symbol}: -1013, реактивное обновление...")
                    ok = await self.filters_cache.refresh_symbol(e.symbol)
                    if not ok:
                        return None
                    # Пересчитываем цену и qty
                    rounded_price = await self._round_price(bsym, stop_price)
                    params["triggerPrice"] = str(rounded_price)
                    if not close_position and quantity is not None:
                        qty = await self._round_qty(bsym, quantity)
                        if qty <= 0:
                            return None
                        params["quantity"] = str(qty)
                    client_order_id = f"wavex{uuid.uuid4().hex[:20]}"
                    params["clientAlgoId"] = client_order_id
                else:
                    log.error(f"{symbol}: повторный -1013, реджект")
                    return None
        return self._normalize_order(resp)

    async def place_take_profit_market(
        self,
        symbol: str,
        side: str,
        stop_price: float,
        quantity: Optional[float] = None,
        close_position: bool = False,
        reduce_only: bool = False,
        client_order_id: Optional[str] = None,
    ) -> Optional[dict]:
        """Размещает TAKE_PROFIT_MARKET ордер через Algo Order API."""
        bsym = to_binance_symbol(symbol)
        rounded_price = await self._round_price(bsym, stop_price)
        if rounded_price <= 0:
            log.error(f"{symbol}: некорректная stop_price={stop_price}")
            return None

        params = {
            "algoType": "CONDITIONAL",          # [НОВОЕ]
            "symbol": bsym,
            "side": side.upper(),
            "type": "TAKE_PROFIT_MARKET",
            "triggerPrice": str(rounded_price), # [ИСПРАВЛЕНО]
            "workingType": "MARK_PRICE",
        }

        if close_position:
            params["closePosition"] = "true"
        else:
            if quantity is None or quantity <= 0:
                log.error(f"{symbol}: для TAKE_PROFIT_MARKET без closePosition нужно указать quantity")
                return None
            qty = await self._round_qty(bsym, quantity)
            if qty <= 0:
                log.warning(f"{symbol}: quantity округлился до 0")
                return None
            params["quantity"] = str(qty)
            if reduce_only:
                params["reduceOnly"] = "true"

        if client_order_id is None:
            client_order_id = f"wavex{uuid.uuid4().hex[:20]}"
        params["clientAlgoId"] = client_order_id  # [ИСПРАВЛЕНО]

        # [НОВОЕ] Retry при -1013
        resp = None
        for attempt in range(2):
            try:
                resp = await self._request(
                    "POST", "/fapi/v1/algoOrder", params=params,
                    signed=True, is_order=True,
                )
                break
            except FilterFailureError as e:
                if attempt == 0:
                    log.warning(f"{symbol}: -1013, реактивное обновление...")
                    ok = await self.filters_cache.refresh_symbol(e.symbol)
                    if not ok:
                        return None
                    # Пересчитываем цену и qty
                    rounded_price = await self._round_price(bsym, stop_price)
                    params["triggerPrice"] = str(rounded_price)
                    if not close_position and quantity is not None:
                        qty = await self._round_qty(bsym, quantity)
                        if qty <= 0:
                            return None
                        params["quantity"] = str(qty)
                    client_order_id = f"wavex{uuid.uuid4().hex[:20]}"
                    params["clientAlgoId"] = client_order_id
                else:
                    log.error(f"{symbol}: повторный -1013, реджект")
                    return None
        return self._normalize_order(resp)

    # ================================================================
    # ПОЛУЧЕНИЕ ИНФОРМАЦИИ О ПОЗИЦИЯХ И ОРДЕРАХ
    # ================================================================
    async def get_position_risk(self, symbol: Optional[str] = None) -> List[dict]:
        """
        Возвращает информацию об открытых позициях с данными о риске.
        Если symbol=None — по всем символам.
        Возвращает только позиции с ненулевым объёмом.
        """
        params = {}
        if symbol:
            params["symbol"] = to_binance_symbol(symbol)

        resp = await self._request(
            "GET",
            "/fapi/v2/positionRisk",
            params=params,
            signed=True,
        )

        if not resp or not isinstance(resp, list):
            return []

        result = []
        for p in resp:
            position_amt = _safe_float(p.get("positionAmt"))
            if position_amt == 0:
                continue
            result.append(
                {
                    "symbol": to_internal_symbol(p.get("symbol", "")),
                    "position_amt": position_amt,
                    "entry_price": _safe_float(p.get("entryPrice")),
                    "mark_price": _safe_float(p.get("markPrice")),
                    "unrealized_pnl": _safe_float(p.get("unRealizedProfit")),
                    "liquidation_price": _safe_float(p.get("liquidationPrice")),
                    "leverage": _safe_float(p.get("leverage")),
                    "margin_type": p.get("marginType"),
                    "position_side": p.get("positionSide"),
                    "notional": _safe_float(p.get("notional")),
                }
            )
        return result

    async def get_open_orders(self, symbol: Optional[str] = None) -> List[dict]:
        """
        Возвращает список открытых ордеров.
        Если symbol=None — по всем символам.
        """
        params = {}
        if symbol:
            params["symbol"] = to_binance_symbol(symbol)

        resp = await self._request(
            "GET",
            "/fapi/v1/openOrders",
            params=params,
            signed=True,
        )

        if not resp or not isinstance(resp, list):
            return []

        result = []
        for o in resp:
            result.append(
                {
                    "symbol": to_internal_symbol(o.get("symbol", "")),
                    "order_id": o.get("orderId"),
                    "client_order_id": o.get("clientOrderId"),
                    "type": o.get("type"),
                    "side": o.get("side"),
                    "status": o.get("status"),
                    "price": _safe_float(o.get("price")),
                    "stop_price": _safe_float(o.get("stopPrice")),
                    "quantity": _safe_float(o.get("origQty")),
                    "executed_qty": _safe_float(o.get("executedQty")),
                    "reduce_only": o.get("reduceOnly", False),
                    "close_position": o.get("closePosition", False),
                    "update_time": o.get("updateTime"),
                }
            )
        return result

    async def get_open_algo_orders(self, symbol: Optional[str] = None) -> List[dict]:
        """
        [НОВОЕ]
        Возвращает список открытых алгоритмических (условных) ордеров.
        Это SL/TP, созданные через /fapi/v1/algoOrder.
        Если symbol=None — по всем символам.
        """
        params = {}
        if symbol:
            params["symbol"] = to_binance_symbol(symbol)
        resp = await self._request(
            "GET",
            "/fapi/v1/openAlgoOrders",
            params=params,
            signed=True,
        )
        if not resp or not isinstance(resp, list):
            return []
        result = []
        for o in resp:
            result.append(
                {
                    "symbol": to_internal_symbol(o.get("symbol", "")),
                    "algo_id": _safe_float(o.get("algoId")),
                    "client_algo_id": o.get("clientAlgoId"),
                    "type": o.get("orderType"),
                    "side": o.get("side"),
                    "status": o.get("status"),
                    "trigger_price": _safe_float(o.get("triggerPrice")),
                    "quantity": _safe_float(o.get("origQty")),
                    "close_position": o.get("closePosition", False),
                }
            )
        return result

    async def cancel_all_orders(self, symbol: str) -> bool:
        """
        Отменяет все открытые ордера по символу.
        Используется при форс-закрытии позиции.
        """
        bsym = to_binance_symbol(symbol)
        resp = await self._request(
            "DELETE",
            "/fapi/v1/allOpenOrders",
            params={"symbol": bsym},
            signed=True,
            is_order=True,
        )
        return resp is not None

    async def get_user_trades(
        self,
        symbol: str,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
        limit: int = 50,
    ) -> List[dict]:
        """
        Возвращает историю сделок (fills) по символу.
        Нужно для reconciliation и учёта комиссий.
        """
        bsym = to_binance_symbol(symbol)
        params = {"symbol": bsym, "limit": limit}
        if start_time is not None:
            params["startTime"] = start_time
        if end_time is not None:
            params["endTime"] = end_time

        resp = await self._request(
            "GET",
            "/fapi/v1/userTrades",
            params=params,
            signed=True,
        )

        if not resp or not isinstance(resp, list):
            return []

        result = []
        for t in resp:
            result.append(
                {
                    "symbol": to_internal_symbol(t.get("symbol", "")),
                    "trade_id": t.get("id"),
                    "order_id": t.get("orderId"),
                    "price": _safe_float(t.get("price")),
                    "quantity": _safe_float(t.get("qty")),
                    "commission": _safe_float(t.get("commission")),
                    "commission_asset": t.get("commissionAsset"),
                    "time": t.get("time"),
                    "buyer": t.get("buyer"),
                    "maker": t.get("maker"),
                    "realized_pnl": _safe_float(t.get("realizedPnl", 0)),
                }
            )
        return result

    async def create_listen_key(self) -> Optional[str]:
        """
        Создаёт listenKey для User Data Stream (Futures).
        Нужен для подписки на события ORDER_TRADE_UPDATE и ACCOUNT_UPDATE.
        """
        resp = await self._request(
            "POST",
            "/fapi/v1/listenKey",
            signed=False,  # listenKey не требует подписи, только API-ключ
        )
        if resp and "listenKey" in resp:
            return resp["listenKey"]
        return None