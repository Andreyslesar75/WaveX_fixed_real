#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ФАЙЛ: ws_client.py
# СОХРАНИТЬ КАК: ws_client.py

Этот файл отвечает за WebSocket-подключение к Binance Futures.

Он получает:
1. стакан (depth20);
2. ленту сделок (aggTrade);
3. последнюю цену;
4. микроструктуру: спред, buy/sell ratio, агрессию, скорость ленты.

ЧТО ИСПРАВЛЕНО:
1. _pending_subs и _pending_unsubs больше не перезаписываются.
   Раньше новая подписка могла затереть старую, если она ещё не
   успела отправиться на сервер.

2. Добавлена периодическая отправка отложенных подписок/отписок.
   Раньше они отправлялись только после получения нового сообщения.
   Если сообщений не было, подписка могла задержаться.

3. Добавлен флаг connected.
   Позже его можно использовать в GUI, чтобы показывать реальный
   статус WebSocket, а не просто зелёный кружок.

4. Добавлен публичный метод get_subscribed().
   Он нужен, чтобы scanner не обращался напрямую к приватному
   полю _subscribed.
"""

import asyncio
import json
import time
from typing import Dict, Optional

import aiohttp

from config import Config
from logger import log
from api import to_binance_symbol, to_internal_symbol


class BinanceWsClient:
    """
    WebSocket-клиент Binance Futures.
    """

    def __init__(self):
        # Хранилище микроструктуры по каждому символу.
        self._data: Dict[str, dict] = {}

        # Lock нужен, потому что данные читаются и пишутся
        # из разных asyncio-задач.
        self._lock = asyncio.Lock()

        # Символы, на которые бот уже подписан.
        self._subscribed: set = set()

        # Событие, которое говорит, что WS хотя бы один раз подключился.
        self._ws_ready = asyncio.Event()

        # ID запросов SUBSCRIBE/UNSUBSCRIBE.
        self._req_id = 1

        # Отложенные подписки и отписки.
        self._pending_subs = []
        self._pending_unsubs = []

        # [НОВОЕ]
        # Флаг реального состояния WebSocket.
        self.connected = False

    def _default(self) -> dict:
        """
        Значения по умолчанию для микроструктуры.

        Если данных ещё нет или они устарели,
        возвращается этот нейтральный набор.
        """
        return {
            "spread_pct": 0.5,
            "buy_sell_ratio": 1.0,
            "tape_speed": 0.0,
            "aggression_pct": 50.0,
            "ts": 0.0,
        }

    async def get(self, symbol: str) -> dict:
        """
        Возвращает свежую микроструктуру по символу.

        Если данные старше 60 секунд, возвращаем значения по умолчанию.
        """
        async with self._lock:
            d = dict(self._data.get(symbol, self._default()))

        if time.time() - d.get("ts", 0) > 60:
            return self._default()

        return d

    async def get_subscribed(self) -> set:
        """
        [НОВОЕ]
        Публичный метод для получения списка подписок.

        Раньше scanner обращался напрямую к self._subscribed,
        что было небезопасно.
        """
        async with self._lock:
            return set(self._subscribed)

    async def subscribe(self, symbols: list):
        """
        Добавляет символы в подписку.

        [ИСПРАВЛЕНО]
        Раньше:
            self._pending_subs = list(new_symbols)

        Это могло перезаписать старые подписки, которые ещё
        не успели отправиться на сервер.

        Теперь новые символы аккуратно добавляются в конец списка.
        """
        async with self._lock:
            new_symbols = set(symbols) - self._subscribed

            if new_symbols:
                self._subscribed.update(new_symbols)

                for s in new_symbols:
                    if s not in self._pending_subs:
                        self._pending_subs.append(s)

                # Если символ раньше стоял в отписке,
                # но теперь снова нужен — убираем его из отписки.
                for s in new_symbols:
                    if s in self._pending_unsubs:
                        self._pending_unsubs.remove(s)

    async def unsubscribe(self, symbols: list):
        """
        Отписывает символы.

        [ИСПРАВЛЕНО]
        Раньше:
            self._pending_unsubs = list(to_remove)

        Теперь отписка добавляется безопасно, без перезаписи.
        """
        async with self._lock:
            to_remove = set(symbols) & self._subscribed

            if to_remove:
                self._subscribed.difference_update(to_remove)

                for s in to_remove:
                    if s not in self._pending_unsubs:
                        self._pending_unsubs.append(s)

                # Если символ стоял в отложенных подписках,
                # но теперь он больше не нужен — убираем его.
                for s in to_remove:
                    if s in self._pending_subs:
                        self._pending_subs.remove(s)

    def _streams_for(self, internal_symbols: list) -> list:
        """
        Возвращает список Binance-стримов для внутренних символов.

        Для каждого символа подписываемся на:
        - depth20@100ms — стакан;
        - aggTrade — лента сделок.
        """
        streams = []

        for s in internal_symbols:
            bsym = to_binance_symbol(s).lower()

            streams.append(f"{bsym}@depth20@100ms")
            streams.append(f"{bsym}@aggTrade")

        return streams

    async def run(self, session: aiohttp.ClientSession, stop_flag: list):
        """
        Главный цикл WebSocket.

        Если соединение падает, клиент автоматически переподключается
        с растущей задержкой: 2с, 4с, 8с и т.д., максимум 60с.
        """
        reconnect_delay = 2

        while not stop_flag[0]:
            try:
                await self._ws_loop(session)
                reconnect_delay = 2

            except asyncio.CancelledError:
                break

            except Exception as e:
                log.warning(
                    f"WS обрыв: {e}, реконнект через {reconnect_delay}s"
                )

            finally:
                self.connected = False

            if stop_flag[0]:
                break

            await asyncio.sleep(reconnect_delay)

            reconnect_delay = min(reconnect_delay * 2, 60)

    async def _ws_loop(self, session: aiohttp.ClientSession):
        """
        Внутри одного подключения.
        """
        async with session.ws_connect(
            Config.BINANCE_WS_URL,
            heartbeat=20,
        ) as ws:

            log.info("WS подключён (Binance Futures)")

            self.connected = True
            self._ws_ready.set()

            # Сразу подписываемся на то, что уже было в _subscribed.
            async with self._lock:
                init_subs = list(self._subscribed)

            if init_subs:
                await self._send_subscribe(ws, init_subs)

            # Основной цикл приёма сообщений.
            while True:
                try:
                    # Ждём сообщение максимум 1 секунду.
                    # Это нужно, чтобы даже при тишине в WS
                    # отправлять отложенные подписки/отписки.
                    msg = await asyncio.wait_for(
                        ws.receive(),
                        timeout=1.0,
                    )

                except asyncio.TimeoutError:
                    # Сообщений не было, но нужно отправить
                    # отложенные подписки/отписки.
                    await self._flush_pending(ws)
                    continue

                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = json.loads(msg.data)
                        await self._handle(data)

                    except Exception:
                        pass

                elif msg.type in (
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.CLOSING,
                    aiohttp.WSMsgType.ERROR,
                ):
                    break

                # После каждого сообщения тоже проверяем,
                # есть ли отложенные подписки/отписки.
                await self._flush_pending(ws)

    async def _flush_pending(self, ws):
        """
        [ИСПРАВЛЕНО]
        Отправляет отложенные подписки и отписки.

        Раньше отправка происходила только после получения сообщения.
        Теперь отправка также происходит по таймауту раз в секунду.
        """
        # Забираем списки под lock, но отправляем без lock,
        # чтобы не блокировать обработку данных на время отправки.
        async with self._lock:
            pending_subs = list(self._pending_subs)
            pending_unsubs = list(self._pending_unsubs)

        if pending_subs:
            await self._send_subscribe(ws, pending_subs)

            async with self._lock:
                # Удаляем только те, которые реально отправили.
                for s in pending_subs:
                    if s in self._pending_subs:
                        self._pending_subs.remove(s)

        if pending_unsubs:
            await self._send_unsubscribe(ws, pending_unsubs)

            async with self._lock:
                for s in pending_unsubs:
                    if s in self._pending_unsubs:
                        self._pending_unsubs.remove(s)

    async def _send_subscribe(self, ws, internal_symbols: list):
        """
        Отправляет SUBSCRIBE на сервер Binance.

        Отправляем пачками не больше 40 стримов за раз.
        """
        if not internal_symbols:
            return

        streams = self._streams_for(internal_symbols)

        for i in range(0, len(streams), 40):
            chunk = streams[i:i + 40]

            self._req_id += 1

            await ws.send_json(
                {
                    "method": "SUBSCRIBE",
                    "params": chunk,
                    "id": self._req_id,
                }
            )

    async def _send_unsubscribe(self, ws, internal_symbols: list):
        """
        Отправляет UNSUBSCRIBE на сервер Binance.
        """
        if not internal_symbols:
            return

        streams = self._streams_for(internal_symbols)

        for i in range(0, len(streams), 40):
            chunk = streams[i:i + 40]

            self._req_id += 1

            await ws.send_json(
                {
                    "method": "UNSUBSCRIBE",
                    "params": chunk,
                    "id": self._req_id,
                }
            )

    async def _handle(self, data: dict):
        """
        Разбирает входящее сообщение WebSocket.
        """
        stream_name = data.get("stream", "")
        payload = data.get("data")

        if not stream_name or not payload:
            return

        if "@depth" in stream_name:
            await self._handle_book(payload, stream_name)

        elif "@aggTrade" in stream_name:
            await self._handle_trade(payload, stream_name)

    async def _handle_book(self, payload: dict, stream_name: str):
        """
        Обработка стакана.

        Считаем:
        - spread_pct;
        - buy_sell_ratio по глубине топ-10 уровней.
        """
        symbol_b = stream_name.split("@")[0].upper()

        bids = payload.get("bids", [])
        asks = payload.get("asks", [])

        if not symbol_b or not bids or not asks:
            return

        symbol = to_internal_symbol(symbol_b)

        try:
            bb = float(bids[0][0])
            ba = float(asks[0][0])

            mid = (bb + ba) / 2

            spread_pct = (ba - bb) / mid * 100 if mid > 0 else 0.5

            # Глубина в USDT по топ-10 уровням.
            bd = sum(float(b[0]) * float(b[1]) for b in bids[:10])
            ad = sum(float(a[0]) * float(a[1]) for a in asks[:10])

            bsr = bd / ad if ad > 0 else 1.0

            async with self._lock:
                d = self._data.setdefault(symbol, self._default())

                d["spread_pct"] = spread_pct
                d["buy_sell_ratio"] = round(bsr, 2)
                d["ts"] = time.time()

        except Exception:
            pass

    async def _handle_trade(self, payload: dict, stream_name: str):
        """
        Обработка ленты сделок.

        Считаем:
        - aggression_pct;
        - tape_speed;
        - last_price.
        """
        symbol_b = stream_name.split("@")[0].upper()

        if not symbol_b:
            return

        symbol = to_internal_symbol(symbol_b)

        try:
            price = float(payload.get("p", 0))
            amount = float(payload.get("q", 0))

            q = price * amount

            # m = True означает, что покупатель — maker.
            # Значит агрессором был продавец.
            is_buyer_maker = bool(payload.get("m", False))

            ts_ms = payload.get("T", payload.get("E", time.time() * 1000))

            # Покупатель-тейкер -> бычий объём.
            bv = q if not is_buyer_maker else 0.0

            # Продавец-тейкер -> медвежий объём.
            sv = q if is_buyer_maker else 0.0

            async with self._lock:
                d = self._data.setdefault(symbol, self._default())

                # Экспоненциальное сглаживание объёмов.
                d["_bv"] = d.get("_bv", 0.0) * 0.9 + bv
                d["_sv"] = d.get("_sv", 0.0) * 0.9 + sv

                total = d["_bv"] + d["_sv"]

                if total > 0:
                    d["aggression_pct"] = round(
                        d["_bv"] / total * 100,
                        1,
                    )

                # Скорость ленты сделок.
                ts_list = d.get("_ts_list", [])
                ts_list.append(float(ts_ms) / 1000.0)
                d["_ts_list"] = ts_list[-50:]

                if len(d["_ts_list"]) >= 2:
                    span = (max(d["_ts_list"]) - min(d["_ts_list"])) / 60.0

                    if span > 0:
                        d["tape_speed"] = round(
                            len(d["_ts_list"]) / span,
                            0,
                        )

                # Последняя цена для position_watcher.
                if price > 0:
                    d["last_price"] = price
                    d["last_price_ts"] = time.time()

                d["ts"] = time.time()

        except Exception:
            pass

    async def get_last_price(
        self,
        symbol: str,
        max_age_sec: float = 5.0,
    ) -> Optional[float]:
        """
        Возвращает последнюю цену из WebSocket, если она свежая.

        Если цена старше max_age_sec, возвращает None.
        """
        async with self._lock:
            d = self._data.get(symbol)

            if not d:
                return None

            ts = d.get("last_price_ts", 0.0)

            if time.time() - ts > max_age_sec:
                return None

            price = d.get("last_price", 0.0)

            return price if price > 0 else None
        
    async def start_user_data_stream(self):
        """Создает и начинает слушать user data stream."""
        try:
            # Создаем listenKey
            resp = await self.rest_client.post("/fapi/v1/listenKey")
            listen_key = resp["listenKey"]
            log.info(f"User Data Stream: listenKey получен: {listen_key[:10]}...")
            
            # Сохраняем listenKey
            self._listen_key = listen_key
            
            # Подписываемся на события
            self._user_data_stream_task = asyncio.create_task(
                self._user_data_stream_loop(listen_key)
            )
            
            # Запускаем keepalive
            self._keepalive_task = asyncio.create_task(
                self._keepalive_loop(listen_key)
            )
            
            return True
        except Exception as e:
            log.error(f"Не удалось запустить User Data Stream: {e}")
            return False

    async def _user_data_stream_loop(self, listen_key: str):
        """Цикл обработки событий из User Data Stream."""
        url = f"{Config.BINANCE_WS_URL}/ws/{listen_key}"
        
        while not self._stop_flag[0]:
            try:
                async with self.session.ws_connect(url, heartbeat=20) as ws:
                    log.info("User Data Stream: подключен")
                    self._user_data_connected = True
                    
                    while not self._stop_flag[0]:
                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=60.0)
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = json.loads(msg.data)
                                await self._handle_user_data(data)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, 
                                            aiohttp.WSMsgType.CLOSING,
                                            aiohttp.WSMsgType.ERROR):
                                break
                        except asyncio.TimeoutError:
                            # Проверяем активность
                            if time.time() - self._last_user_data_time > 120:
                                log.warning("User Data Stream: тишина > 120с, реконнект")
                                break
                    self._user_data_connected = False
                await asyncio.sleep(2)
            except Exception as e:
                log.error(f"User Data Stream ошибка: {e}")
                await asyncio.sleep(5)

    async def _keepalive_loop(self, listen_key: str):
        """Отправляет keepalive каждые 30 минут."""
        while not self._stop_flag[0]:
            try:
                await asyncio.sleep(1800)  # 30 минут
                if self._stop_flag[0]:
                    break
                log.debug("User Data Stream: отправка keepalive")
                await self.rest_client.put("/fapi/v1/listenKey")
            except Exception as e:
                log.error(f"User Data Stream keepalive ошибка: {e}")

    async def _handle_user_data(self, data: dict):
        """Обрабатывает события из User Data Stream."""
        event_type = data.get("e")
        if not event_type:
            return
        
        self._last_user_data_time = time.time()
        
        if event_type == "ORDER_TRADE_UPDATE":
            # Обработка обновления ордера
            order = data["o"]
            symbol = to_internal_symbol(order["s"])
            
            # Проверяем, есть ли эта позиция в tracker
            if symbol in self._tracker.positions:
                await self._process_order_update(symbol, order)
                
        elif event_type == "ACCOUNT_UPDATE":
            # Обработка обновления аккаунта
            update_data = data["a"]
            # Можно обновлять баланс и позиции
            await self._process_account_update(update_data)

    async def _process_order_update(self, symbol: str, order: dict):
        """Обрабатывает обновление ордера из User Data Stream."""
        order_id = order.get("i")
        client_order_id = order.get("c")
        status = order.get("X")
        qty = float(order.get("q", 0))
        filled_qty = float(order.get("z", 0))
        
        log.info(
            f"User Data Stream: обновление ордера {symbol} "
            f"id={order_id} client_id={client_order_id} "
            f"status={status} filled={filled_qty}/{qty}"
        )
        
        # Передаем событие в tracker
        if self._tracker:
            await self._tracker.handle_order_update(
                symbol, 
                order_id, 
                client_order_id, 
                status, 
                filled_qty
            )