# trading/types.py
"""Доменные модели торговой части — единый словарь терминов проекта.

Все модели pydantic v2 strict=True: данные с границ системы (REST/WS
биржи, БД, конфиг) обязаны пройти валидацию здесь ДО использования
движком. Деньги/цены/объёмы — Decimal; float допустим только в
статистиках (score, pnl_pct, mfe/mae).

Инварианты:
- OrderRequest frozen: параметры фиксируются при создании и
  переиспользуются при повторных отправках — устраняет класс ошибок
  «retry собран из других полей» (баг П1 старого кода);
- closePosition=True исключает qty и reduceOnly (контракт Binance);
- clientOrderId валидируется по правилам Binance: [A-Za-z0-9_-],
  <= 36 символов [НЕУВЕРЕН: точный лимит /fapi — проверит V-API-9].

Исключения: pydantic.ValidationError — на границе системы это штатный
реджект с причиной, а не падение процесса.
"""
from __future__ import annotations

import re
from collections.abc import Awaitable, Callable, Mapping
from decimal import Decimal
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

#: Транспортный контракт: async GET JSON (path, query-params) -> сырой ответ.
#: Возвращает Any осознанно: сырой JSON валидируется парсерами этого пакета.
JsonFetcher = Callable[[str, Mapping[str, str] | None], Awaitable[Any]]

_CLIENT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,36}$")


class Mode(str, Enum):
    """Режим записи данных (колонка mode в каждой таблице)."""

    PAPER = "paper"
    REAL = "real"


class OrderSide(str, Enum):
    """Сторона биржевого ордера."""

    BUY = "BUY"
    SELL = "SELL"


class Side(str, Enum):
    """Направление позиции (не путать со стороной ордера)."""

    LONG = "LONG"
    SHORT = "SHORT"

    @property
    def order_side_entry(self) -> OrderSide:
        """Сторона ордера на вход в позицию."""
        return OrderSide.BUY if self is Side.LONG else OrderSide.SELL

    @property
    def order_side_exit(self) -> OrderSide:
        """Сторона ордера на выход из позиции."""
        return OrderSide.SELL if self is Side.LONG else OrderSide.BUY


class OrderKind(str, Enum):
    """Типы ордеров, реально используемые системой.

    Остальные типы Binance (LIMIT, TRAILING_STOP_MARKET, ...) намеренно
    не включены: неиспользуемая поверхность API не должна попадать
    в валидированный словарь проекта.
    """

    MARKET = "MARKET"
    STOP_MARKET = "STOP_MARKET"
    TAKE_PROFIT_MARKET = "TAKE_PROFIT_MARKET"


class OrderState(str, Enum):
    """Состояние ордера в нашей модели (не сырые статусы биржи).

    TIMEOUT_UNKNOWN / NOT_FOUND — состояния resolve-цикла: при потере
    ответа POST ордер переводится в TIMEOUT_UNKNOWN и выясняется через
    GET /fapi/v1/order; NOT_FOUND (-2013) означает, что ордер никогда
    не был выставлен — повтор с тем же clientOrderId безопасен
    (идемпотентность, защита от задвоения позиции).
    """

    SUBMITTING = "SUBMITTING"
    NEW = "NEW"
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    TIMEOUT_UNKNOWN = "TIMEOUT_UNKNOWN"
    NOT_FOUND = "NOT_FOUND"

    @classmethod
    def from_exchange(cls, raw: str) -> OrderState | None:
        """Отобразить сырой статус (X в WS / status в REST) в OrderState.

        Returns:
            OrderState или None для неизвестных значений — вызывающая
            сторона обязана залогировать инцидент, а не молча
            отбросить событие.

        Пояснения маппинга: EXPIRED -> CANCELED (ордер снят биржей по
        времени); PENDING_CANCEL -> NEW (отмена инициирована, ордер
        ещё активен — важно для проверки здоровья SL).
        """
        mapping: dict[str, OrderState] = {
            "NEW": cls.NEW,
            "PARTIALLY_FILLED": cls.PARTIALLY_FILLED,
            "FILLED": cls.FILLED,
            "CANCELED": cls.CANCELED,
            "REJECTED": cls.REJECTED,
            "EXPIRED": cls.CANCELED,
            "PENDING_CANCEL": cls.NEW,
        }
        return mapping.get(raw)


class ExitReason(str, Enum):
    """Причина закрытия позиции.

    Множество зафиксировано под контракт GUI (gui.py показывает
    exit_reason из trades) — расширение только согласованно с GUI.
    """

    SL = "SL"
    TRAIL_SL = "TRAIL_SL"
    BE_SL = "BE_SL"
    TP1 = "TP1"
    TP2 = "TP2"
    TIMEOUT = "TIMEOUT"
    VOL_DECAY = "VOL_DECAY"
    IRON_SL = "IRON_SL"
    GAP_SL = "GAP_SL"
    EXTERNAL_CLOSE = "EXTERNAL_CLOSE"
    FORCED = "FORCED"
    UNKNOWN_RECONCILE = "UNKNOWN_RECONCILE"


class RejectReason(str, Enum):
    """Точная причина реджекта сигнала/ордера — пишется в БД.

    Требование черновика: не просто «invalid», а какой фильтр/гейт
    не прошёл — для статистики причин реджектов.
    """

    INVALID_SIGNAL = "invalid_signal"
    LOW_CONFIDENCE = "low_confidence"
    SCORE_THRESHOLD = "score_threshold"
    DUPLICATE = "duplicate"
    SHORT_DISABLED = "short_disabled"
    COOLDOWN_SL = "cooldown_sl"
    COOLDOWN_REPEAT = "cooldown_repeat"
    GAP_PROTECTION = "gap_protection"
    DAILY_LIMIT = "daily_limit"
    MAX_POSITIONS = "max_positions"
    SYMBOL_NOT_TRADING = "symbol_not_trading"
    QTY_BELOW_MIN = "qty_below_min"
    QTY_ABOVE_MAX = "qty_above_max"
    NOTIONAL_BELOW_MIN = "notional_below_min"
    INVALID_LEVELS = "invalid_levels"
    INSUFFICIENT_BALANCE = "insufficient_balance"
    INSUFFICIENT_MARGIN = "insufficient_margin"
    RATE_LIMITED = "rate_limited"
    FILTER_FAILURE = "filter_failure"
    ORDER_FAILED = "order_failed"
    PROTECTION_FAILED = "protection_failed"
    UNKNOWN = "unknown_error"


class IncidentType(str, Enum):
    """Типы инцидентов (таблица incidents) — кандидаты на алерты."""

    SL_LOST = "sl_lost"
    SL_RESTORED = "sl_restored"
    FORCE_CLOSE = "force_close"
    IRON_SL = "iron_sl"
    RECON_MISMATCH = "recon_mismatch"
    UNPROTECTED = "unprotected"
    WS_RECONNECT = "ws_reconnect"
    ORDER_TIMEOUT = "order_timeout"
    RATE_LIMIT = "rate_limit"
    EXTERNAL_POSITION = "external_position"
    TP1_SKIP = "tp1_skip"
    FILTER_REFRESH_FAILED = "filter_refresh_failed"
    CLOCK_RESYNC = "clock_resync"
    UNKNOWN_ORDER_STATUS = "unknown_order_status"


class SymbolFilters(BaseModel):
    """Фильтры символа из exchangeInfo, потребляются money.py.

    Соответствие биржевым фильтрам: PRICE_FILTER -> tick_size,
    MARKET_LOT_SIZE -> step_size/min_qty/max_qty,
    MIN_NOTIONAL -> min_notional. Все Decimal — из строк биржи.
    """

    model_config = ConfigDict(strict=True, frozen=True)

    symbol: str
    status: str
    tick_size: Decimal = Field(gt=Decimal("0"))
    step_size: Decimal = Field(gt=Decimal("0"))
    min_qty: Decimal = Field(ge=Decimal("0"))
    max_qty: Decimal = Field(gt=Decimal("0"))
    min_notional: Decimal = Field(ge=Decimal("0"))
    price_precision: int = Field(ge=0)
    quantity_precision: int = Field(ge=0)
    trigger_protect: Decimal | None = None

    @property
    def is_trading(self) -> bool:
        """True, если символ допущен к торговле (гейт входа)."""
        return self.status == "TRADING"

Confidence = Literal["HIGH", "MEDIUM", "LOW", "SKIP"]
"""Метка уверенности сигнала (значения — из части решений)."""

class SignalInput(BaseModel):
    """Валидированный входной сигнал от части решений.

    klines_1h в модель не входят: сырые свечи передаются в
    calculations.py как opaque-аргумент (внутренняя граница проекта,
    не граница системы — двойной валидации не требуется).
    """

    model_config = ConfigDict(strict=True)

    symbol: str = Field(min_length=1, max_length=20)
    side: Side
    price: Decimal = Field(gt=Decimal("0"))  # референс (close 1m-свечи)
    score: float
    confidence: Literal["HIGH", "MEDIUM", "LOW", "SKIP"]
    spread_pct: float
    btc_trend: float
    high24: float
    low24: float
    structural_level: Decimal | None = None


def make_client_id(signal_id: int, code: str, seq: int = 0) -> str:
    """Собрать идемпотентный clientOrderId.

    Формат: wx{signal_id}-{code}{seq}, например wx4212-tp2.
    code in {in, sl, tp1, tp2, fc, rs} (rs = восстановление SL).

    Args:
        signal_id: первичный ключ signals — уникален в рамках сигнала;
        code: семантика ордера; seq: номер повторной постановки уровня.

    Returns:
        Строка, проходящая правила Binance ([A-Za-z0-9_-], <= 36).

    Raises:
        ValueError: результат не проходит правила Binance.
    """
    cid = f"wx{signal_id}-{code}{seq}"
    if not _CLIENT_ID_RE.fullmatch(cid):
        raise ValueError(f"clientOrderId не проходит правила Binance: {cid!r}")
    return cid


class OrderRequest(BaseModel):
    """Неизменяемый запрос ордера — единственный источник параметров
    при отправке и повторных отправках (анти-П1).

    Инварианты (проверяются до любого сетевого вызова):
    - MARKET: qty обязателен; stop_price/close_position/price_protect
      запрещены;
    - STOP_MARKET / TAKE_PROFIT_MARKET: stop_price обязателен;
    - close_position=True: qty и reduce_only запрещены (контракт биржи);
    - без close_position: qty обязателен;
    - price_protect по умолчанию False: включается фабрикой движка
      явно для условных ордеров (см. engine, Д7).
    """

    model_config = ConfigDict(strict=True, frozen=True)

    client_order_id: str
    symbol: str = Field(min_length=1, max_length=20)
    side: OrderSide
    kind: OrderKind
    qty: Decimal | None = Field(default=None, gt=Decimal("0"))
    stop_price: Decimal | None = Field(default=None, gt=Decimal("0"))
    reduce_only: bool = False
    close_position: bool = False
    working_type: Literal["MARK_PRICE", "CONTRACT_PRICE"] = "MARK_PRICE"
    price_protect: bool = False
    position_ref: str | None = None  # clientOrderId входа — связка ордеров
    signal_id: int | None = None
    meta: Mapping[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _check_kind_contract(self) -> OrderRequest:
        """Проверить согласованность полей с типом ордера (см. класс)."""
        if not _CLIENT_ID_RE.fullmatch(self.client_order_id):
            raise ValueError("clientOrderId: [A-Za-z0-9_-], <= 36 символов")
        if self.kind is OrderKind.MARKET:
            if self.qty is None:
                raise ValueError("MARKET требует qty")
            if self.stop_price is not None:
                raise ValueError("MARKET не принимает stop_price")
            if self.close_position:
                raise ValueError("MARKET не принимает close_position")
            if self.price_protect:
                raise ValueError("price_protect не применим к MARKET")
        else:
            if self.stop_price is None:
                raise ValueError(f"{self.kind.value} требует stop_price")
            if self.close_position:
                if self.qty is not None:
                    raise ValueError("close_position=True исключает qty")
                if self.reduce_only:
                    raise ValueError("close_position=True исключает reduce_only")
            elif self.qty is None:
                raise ValueError("условный ордер без close_position требует qty")
        return self


class OrderAck(BaseModel):
    """Подтверждение биржи на запрос ордера.

    avg_price/executed_qty заполняются для MARKET-исполнений (вход,
    форс-закрытие) и запроса статуса; для постановки условных ордеров
    остаются None. raw — исходный JSON для аудита (orders.raw_response).
    """

    model_config = ConfigDict(strict=True)

    client_order_id: str
    exchange_order_id: int | None
    status: OrderState
    avg_price: Decimal | None = None
    executed_qty: Decimal | None = None
    raw: Mapping[str, Any]


class Fill(BaseModel):
    """Исполнение (trade) — источник точных цен и комиссий.

    order_client_id известен из WS-событий; exchange_order_id — из
    REST /fapi/v1/userTrades. Инвариант: заполнен хотя бы один из двух
    идентификаторов (иначе матчинг с orders невозможен).
    """

    model_config = ConfigDict(strict=True, frozen=True)

    order_client_id: str | None = None
    exchange_order_id: int | None = None
    trade_id: int
    ts_ms: int
    price: Decimal = Field(gt=Decimal("0"))
    qty: Decimal = Field(gt=Decimal("0"))
    commission: Decimal = Field(ge=Decimal("0"))
    commission_asset: str

    @model_validator(mode="after")
    def _require_id(self) -> Fill:
        """Гарантия матчинга с orders (см. класс)."""
        if self.order_client_id is None and self.exchange_order_id is None:
            raise ValueError("нужен order_client_id или exchange_order_id")
        return self


class OrderUpdateEvent(BaseModel):
    """Типизированное ORDER_TRADE_UPDATE (User Data Stream).

    Парсинг сырых полей — ответственность user_stream; здесь только
    контракт. state может быть None, если биржа прислала статус вне
    словаря: raw_status сохраняется, инцидент логируется (анти-П2:
    события никогда не теряются молча).
    """

    model_config = ConfigDict(strict=True)

    ts_ms: int
    symbol: str
    client_order_id: str
    exchange_order_id: int | None
    raw_status: str
    state: OrderState | None
    avg_price: Decimal | None
    last_filled_qty: Decimal | None
    accumulated_qty: Decimal | None
    commission: Decimal | None
    commission_asset: str | None
    realized_pnl: Decimal | None
    is_maker: bool | None


class PositionSnapshot(BaseModel):
    """Полное состояние открытой позиции (in-memory + снапшот БД).

    Почему две цены SL: sl_price — биржевой STOP_MARKET (в
    program-режиме статичен), local_sl_price — программный уровень
    BE/трейлинга, который двигает monitor; на входе совпадают.
    iron_sl_price — локальный аварийный контур (§10), переживает
    рестарт через колонку positions.iron_sl_price (чинит П5).
    """

    model_config = ConfigDict(strict=True)

    symbol: str
    side: Side
    signal_id: int
    entry_ts_ms: int
    entry_price: Decimal = Field(gt=Decimal("0"))
    qty: Decimal = Field(gt=Decimal("0"))
    size_usdt: Decimal = Field(gt=Decimal("0"))
    score: float

    sl_price: Decimal = Field(gt=Decimal("0"))
    local_sl_price: Decimal = Field(gt=Decimal("0"))
    tp1_price: Decimal | None = None
    tp2_price: Decimal | None = None
    iron_sl_price: Decimal | None = None

    sl_client_id: str | None = None
    tp1_client_id: str | None = None
    tp2_client_id: str | None = None

    tp1_done: bool = False
    tp2_done: bool = False
    breakeven_done: bool = False
    trail_active: bool = False
    unprotected: bool = False

    mfe_price: Decimal | None = None
    mae_price: Decimal | None = None
    updated_ms: int = 0
