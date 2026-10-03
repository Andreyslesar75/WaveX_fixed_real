# trading/binance/user_stream.py
"""User Data Stream: listenKey lifecycle + WS + парсинг событий.

Надёжность (§8 черновика):
- keepalive каждые 30 мин [фактический TTL ключа — V-API-8];
- 2 неудачных keepalive подряд -> полный пересбор сессии;
- тишина > WS_SILENCE_SEC -> реконнект, не дожидаясь TCP-таймаута;
- событие listenKeyExpired от биржи -> немедленный пересбор;
- после КАЖДОГО реконнекта (не первого) в очередь кладётся
  VenueReconnected — движок обязан сделать точечный reconciliation.

Модель конкурентности: одна задача на стрим; парсинг синхронный,
без await-точек между чтением и put_nowait — порядок событий
сохраняется (важно для ORDER_TRADE_UPDATE над TP/SL).
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from decimal import Decimal
from typing import Any, Mapping, NamedTuple, Protocol

from ..types import OrderState, OrderUpdateEvent
from ..venue import (
    ExchangePosition, VenueAccountUpdate, VenueEvent, VenueOrderUpdate,
    VenueReconnected,
)

logger = logging.getLogger(__name__)


class WsMessage(NamedTuple):
    """Одно сообщение WS: текст либо признак закрытия."""

    closed: bool
    data: str


class WsConnection(Protocol):
    """Абстракция WS-соединения (тестируется фейками)."""

    def receive(self, timeout_s: float) -> Any: ...  # Awaitable[WsMessage]
    async def close(self) -> None: ...


class WsFactory(Protocol):
    """Фабрика соединений по URL (тестируется фейками)."""

    def connect(self, url: str) -> Any: ...  # Awaitable[WsConnection]


class ListenKeyApi(Protocol):
    """Узкий интерфейс к REST, нужный стриму (не весь клиент)."""

    async def create_listen_key(self) -> str: ...
    async def keepalive_listen_key(self) -> None: ...


def _dec(raw: Any) -> Decimal | None:
    """Decimal из поля события или None (отсутствует/мусор)."""
    if raw is None or isinstance(raw, bool):
        return None
    if not isinstance(raw, (str, int, float)):
        return None
    try:
        value = Decimal(str(raw))
    except Exception:
        return None
    return value if value.is_finite() else None


def _str(raw: Any) -> str | None:
    """str из поля события или None."""
    return raw if isinstance(raw, str) else None


def is_listen_key_expired(payload: Mapping[str, Any]) -> bool:
    """Проверить флаг истечения listenKey (событие от биржи)."""
    return payload.get("e") == "listenKeyExpired"


def parse_stream_message(payload: Mapping[str, Any]) -> VenueEvent | None:
    """Разобрать сообщение user-stream в VenueEvent.

    Returns:
        VenueOrderUpdate / VenueAccountUpdate / None (чужие типы событий
        игнорируются осознанно; мусор логируется вызывающим).

    Raises:
        ValueError: обязательные поля ORDER_TRADE_UPDATE отсутствуют —
        лучше потерять одно событие с алертом, чем уронить стрим.
    """
    event_type = payload.get("e")
    if event_type == "ORDER_TRADE_UPDATE":
        return _parse_order_update(payload)
    if event_type == "ACCOUNT_UPDATE":
        return _parse_account_update(payload)
    return None


def _parse_order_update(payload: Mapping[str, Any]) -> VenueOrderUpdate:
    """ORDER_TRADE_UPDATE -> VenueOrderUpdate (формат полей — V-API-5).

    Raises:
        ValueError: нет s/c/X — событие не матчится ни с чем.
    """
    order = payload.get("o")
    if not isinstance(order, Mapping):
        raise ValueError(f"ORDER_TRADE_UPDATE без o: {payload!r}")
    symbol = _str(order.get("s"))
    client_id = _str(order.get("c"))
    raw_status = _str(order.get("X"))
    if symbol is None or client_id is None or raw_status is None:
        raise ValueError(f"ORDER_TRADE_UPDATE без s/c/X: {order!r}")

    exchange_id = order.get("i")
    ts_raw = payload.get("E")
    avg_price = _dec(order.get("ap"))
    if avg_price is not None and avg_price <= 0:
        avg_price = None  # "0" у неисполненных ордеров — не цена
    last_qty = _dec(order.get("l"))
    if last_qty is not None and last_qty <= 0:
        last_qty = None
    accumulated = _dec(order.get("z"))
    if accumulated is not None and accumulated <= 0:
        accumulated = None
    maker = order.get("m")

    event = OrderUpdateEvent(
        ts_ms=ts_raw if isinstance(ts_raw, int) else 0,
        symbol=symbol,
        client_order_id=client_id,
        exchange_order_id=exchange_id if isinstance(exchange_id, int) else None,
        raw_status=raw_status,
        state=OrderState.from_exchange(raw_status),
        avg_price=avg_price,
        last_filled_qty=last_qty,
        accumulated_qty=accumulated,
        commission=_dec(order.get("n")),
        commission_asset=_str(order.get("N")),
        realized_pnl=_dec(order.get("rp")),
        is_maker=maker if isinstance(maker, bool) else None,
    )
    return VenueOrderUpdate(event=event, raw=payload)


def _parse_account_update(payload: Mapping[str, Any]) -> VenueAccountUpdate:
    """ACCOUNT_UPDATE -> VenueAccountUpdate (балансы + позиции)."""
    account = payload.get("a")
    balances: dict[str, Decimal] = {}
    positions: dict[str, ExchangePosition] = {}
    if isinstance(account, Mapping):
        raw_balances = account.get("B")
        if isinstance(raw_balances, list):
            for item in raw_balances:
                if not isinstance(item, Mapping):
                    continue
                asset = _str(item.get("a"))
                wallet = _dec(item.get("wb"))
                if asset is not None and wallet is not None:
                    balances[asset] = wallet
        raw_positions = account.get("P")
        if isinstance(raw_positions, list):
            for item in raw_positions:
                if not isinstance(item, Mapping):
                    continue
                symbol = _str(item.get("s"))
                amount = _dec(item.get("pa"))
                entry = _dec(item.get("ep"))
                if symbol is None or amount is None or entry is None or amount == 0:
                    continue
                positions[symbol] = ExchangePosition(
                    symbol=symbol,
                    side="LONG" if amount > 0 else "SHORT",
                    qty=abs(amount),
                    entry_price=entry,
                    unrealized_pnl=_dec(item.get("up")),
                )
    return VenueAccountUpdate(balances=balances, positions=positions, raw=payload)


class UserStream:
    """Жизненный цикл user-stream: ключ, WS, keepalive, тишина, реконнекты.

    Инвариант: событие VenueReconnected кладётся только после успешного
    подключения, и только если это не первое подключение процесса —
    «до первого подключения» пропущенных событий быть не может.
    """

    def __init__(
        self,
        api: ListenKeyApi,
        factory: WsFactory,
        ws_base_url: str,
        events: "asyncio.Queue[VenueEvent]",
        silence_timeout_s: float = 60.0,
        keepalive_interval_s: float = 1800.0,
        reconnect_delay_s: float = 1.0,
        now_ms: Any = None,
    ) -> None:
        """now_ms — инъекция времени для тестов."""
        self._api = api
        self._factory = factory
        self._ws_base = ws_base_url.rstrip("/")
        self._events = events
        self._silence = silence_timeout_s
        self._keepalive_interval = keepalive_interval_s
        self._reconnect_delay = reconnect_delay_s
        self._now_ms = now_ms or (lambda: int(time.time() * 1000))
        self._ever_connected = False

    async def run(self, stop: asyncio.Event | None = None) -> None:
        """Бесконечный цикл сессий; единственная точка выхода — stop/cancel.

        Ошибки сессии не гасят стрим: лог + задержка + новая сессия.
        """
        while stop is None or not stop.is_set():
            try:
                await self._session()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("user-stream: сессия упала: %s", exc)
            await asyncio.sleep(self._reconnect_delay)

    async def _session(self) -> None:
        """Одна сессия: ключ -> WS -> чтение до обрыва/тишины."""
        listen_key = await self._api.create_listen_key()
        connection = await self._factory.connect(f"{self._ws_base}/ws/{listen_key}")
        if self._ever_connected:
            # реконнект: события могли быть пропущены — сигнал движку
            self._events.put_nowait(VenueReconnected(reason="ws reconnect"))
        self._ever_connected = True
        keepalive = asyncio.create_task(self._keepalive_loop(connection))
        try:
            while True:
                try:
                    message = await connection.receive(self._silence)
                except TimeoutError:
                    logger.warning(
                        "user-stream: тишина > %.0fs — реконнект", self._silence
                    )
                    break
                if message.closed:
                    logger.warning("user-stream: соединение закрыто — реконнект")
                    break
                if not message.data:
                    continue
                try:
                    payload = json.loads(message.data)
                except ValueError:
                    logger.warning("user-stream: не-JSON сообщение пропущено")
                    continue
                if not isinstance(payload, Mapping):
                    continue
                if is_listen_key_expired(payload):
                    logger.warning("user-stream: listenKeyExpired — пересбор")
                    break
                try:
                    event = parse_stream_message(payload)
                except ValueError as exc:
                    logger.error("user-stream: событие отброшено: %s", exc)
                    continue
                if event is not None:
                    self._events.put_nowait(event)
        finally:
            keepalive.cancel()
            try:
                await keepalive
            except (asyncio.CancelledError, Exception):
                pass
            await connection.close()

    async def _keepalive_loop(self, connection: WsConnection) -> None:
        """Продление ключа; 2 неудачи подряд -> разрыв сессии (§8)."""
        failures = 0
        while True:
            await asyncio.sleep(self._keepalive_interval)
            try:
                await self._api.keepalive_listen_key()
                failures = 0
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                failures += 1
                logger.warning(
                    "user-stream: keepalive не прошёл (%d/2): %s", failures, exc
                )
                if failures >= 2:
                    await connection.close()
                    return