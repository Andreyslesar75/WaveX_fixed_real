# trading/venue.py
# trading/venue.py
"""Контракт ExecutionVenue — единая поверхность «биржи» для движка.

Два провайдера (RealVenue поверх REST/WS Binance, PaperVenue —
симулятор) реализуют один интерфейс; движок не делает isinstance-
ветвлений в торговой логике (единственный осознанный isinstance —
в reconciliation для paper, решение владельца по П16).

Принцип «venue не решает»: провайдер исполняет команды и сообщает
факты. Вся политика (ретраи, восстановление SL, пересчёт уровней) —
в движке. Исключение — resolve-цикл ордера после таймаута: это
свойство транспорта, живёт в RealVenue.

События: venue.events — asyncio.Queue[VenueEvent]. Реал кладёт туда
разобранный User Data Stream, paper генерирует события сам при
исполнении. put_nowait безопасен в модели одного asyncio-цикла
движка: у put нет await-точек, блокировки не возникает.
"""
from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict

from .types import Fill, OrderAck, OrderRequest, OrderUpdateEvent


class ExchangePosition(BaseModel):
    """Позиция на «бирже» (positionRisk real / состояние paper).

    qty всегда положительная; направление — в side (One-way модель,
    решение зафиксировано в батче 1).
    """

    model_config = ConfigDict(strict=True)

    symbol: str
    side: str  # 'LONG' | 'SHORT'
    qty: Decimal
    entry_price: Decimal
    unrealized_pnl: Decimal | None = None


class VenueOrderUpdate(BaseModel):
    """Обёртка ORDER_TRADE_UPDATE: типизированное событие + raw для аудита."""

    model_config = ConfigDict(strict=True)

    event: OrderUpdateEvent
    raw: Mapping[str, object]


class VenueAccountUpdate(BaseModel):
    """Обёртка ACCOUNT_UPDATE: балансы (asset -> amount) и позиции."""

    model_config = ConfigDict(strict=True)

    balances: Mapping[str, Decimal]
    positions: Mapping[str, ExchangePosition]
    raw: Mapping[str, object]


class VenueReconnected(BaseModel):
    """Сигнал реконнекта user-stream.

    Движок ОБЯЗАН запустить точечный reconciliation: за время обрыва
    события могли быть пропущены (§8 черновика).
    """

    model_config = ConfigDict(strict=True)

    reason: str


VenueEvent = VenueOrderUpdate | VenueAccountUpdate | VenueReconnected
"""Все события venue: кладёт venue, потребляет один потребитель-движок."""


class ExecutionVenue(ABC):
    """Единый контракт «биржи»: real и paper неотличимы для движка.

    Контрактные исключения (общие для провайдеров — паритет веток):
    - UnknownOrderError: -2011, ордер уже исполнен/отменён — норма при
      гонке cancel/fill; движок выясняет фактический статус через
      query_order (§9 черновика), а не трактует как ошибку;
    - InsufficientFundsError: -2010/-2019, нет средств/маржи —
      реджект сигнала без ретраев.

    Инвариант: методы venue не содержат торговой политики — только
    исполнение и факты. feed_price у real — no-op (истина в WS),
    у paper — триггер условных ордеров.
    """

    @property
    @abstractmethod
    def events(self) -> asyncio.Queue[VenueEvent]:
        """Очередь событий venue (реал — user stream, paper — генерация)."""

    @abstractmethod
    async def execute_order(self, request: OrderRequest) -> OrderAck:
        """Отправить ордер.

        Слепые ретраи запрещены: RealVenue при потере ответа сам
        выясняет фактический статус (resolve-цикл), PaperVenue
        исполняет детерминированно.

        Raises:
            InsufficientFundsError: средств/маржи нет — без ретраев.
        """

    @abstractmethod
    async def cancel_order(self, symbol: str, client_order_id: str) -> OrderAck:
        """Отменить ордер.

        Raises:
            UnknownOrderError: -2011 — штатная гонка cancel/fill;
            OrderNotFoundError (binance): -2013 — ордера не было.
        """

    @abstractmethod
    async def query_order(self, symbol: str, client_order_id: str) -> OrderAck | None:
        """Статус ордера; None = ордера нет (никогда не вставал)."""

    @abstractmethod
    async def open_orders(self, symbol: str) -> list[OrderAck]:
        """Активные ордера символа (здоровье SL/TP, Часть A защиты)."""

    @abstractmethod
    async def cancel_all_orders(self, symbol: str) -> int:
        """Снять все ордера символа; возвращает число снятий."""

    @abstractmethod
    async def positions(self) -> list[ExchangePosition]:
        """Позиции биржи — источник истины при reconciliation."""

    @abstractmethod
    async def available_balance(self, asset: str = "USDT") -> Decimal:
        """Доступный баланс актива."""

    @abstractmethod
    async def user_trades(self, symbol: str, start_ms: int) -> list[Fill]:
        """Сделки символа с указанного времени (reconciliation закрытий)."""

    @abstractmethod
    async def commission_rate(self, symbol: str) -> tuple[Decimal, Decimal]:
        """Ставки комиссий (maker, taker) символа."""

    @abstractmethod
    def feed_price(self, symbol: str, price: Decimal) -> None:
        """Подать live-цену: paper триггерит ордера, real игнорирует."""


class UnknownOrderError(Exception):
    """-2011: ордер уже исполнен/отменён — штатная гонка cancel/fill.

    Движок обязан воспринимать это как «выяснить фактический статус»,
    а не как ошибку (§9 черновика: отмена SL при успевшем исполнении).
    """

    def __init__(
        self,
        code: int,
        http_status: int,
        message: str,
        path: str,
        params: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(f"[{code}] HTTP {http_status}: {message} ({path})")
        self.code = code
        self.http_status = http_status
        self.message = message
        self.path = path
        self.params = dict(params or {})


class InsufficientFundsError(Exception):
    """-2010/-2019: нет средств/маржи — реджект сигнала без ретраев.

    [НЕУВЕРЕН] у фьючерсов код маржи — -2019 (-2010 — spot-наследие);
    оба кода маппятся сюда; фактический фиксирует V-API-6.
    """

    def __init__(
        self,
        code: int,
        http_status: int,
        message: str,
        path: str,
        params: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(f"[{code}] HTTP {http_status}: {message} ({path})")
        self.code = code
        self.http_status = http_status
        self.message = message
        self.path = path
        self.params = dict(params or {})
