# trading/binance/venue.py
"""RealVenue: исполнение контракта ExecutionVenue поверх REST+WS.

Разделение ответственности:
- venue.execute_order отправляет ровно один POST (без ретраев) и
  парсит ответ в OrderAck;
- при потере ответа (таймаут/сеть) запускается resolve-цикл:
  опрос GET /fapi/v1/order по origClientOrderId, стабильный
  NOT_FOUND (2 подряд) = ордер не вставал -> безопасный повтор
  с тем же clientOrderId (идемпотентность, защита от задвоения);
- политика кодов (-1013 -> refresh фильтров, -2019 -> реджект
  сигнала) остаётся в движке: venue не решает.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from decimal import Decimal
from typing import Any

from ..money import to_api_str
from ..types import Fill, OrderAck, OrderKind, OrderRequest, OrderState
from ..venue import ExchangePosition, ExecutionVenue, VenueEvent
from .rest import (
    BinanceApiError,
    BinanceRestClient,
    FilterFailureError,
    OrderNotFoundError,
    TransportError,
    TransportTimeout,
)

logger = logging.getLogger(__name__)


def build_order_params(request: OrderRequest) -> dict[str, str]:
    """OrderRequest -> параметры POST /fapi/v1/order.

    Boolean-флаги: closePosition/reduceOnly -> "true"/"false";
    priceProtect -> "TRUE"/"FALSE" [НЕУВЕРЕН: регистр — V-API-2/3].
    """
    params: dict[str, str] = {
        "symbol": request.symbol,
        "side": request.side.value,
        "type": request.kind.value,
        "newClientOrderId": request.client_order_id,
    }
    if request.qty is not None:
        params["quantity"] = to_api_str(request.qty)
    if request.stop_price is not None:
        params["stopPrice"] = to_api_str(request.stop_price)
    if request.reduce_only:
        params["reduceOnly"] = "true"
    if request.close_position:
        params["closePosition"] = "true"
    if request.price_protect:
        params["priceProtect"] = "TRUE"
    params["workingType"] = request.working_type
    return params

def build_algo_params(request: OrderRequest) -> dict[str, str]:
    """OrderRequest -> параметры POST /fapi/v1/algoOrder.

    Контракт восстановлен из рабочего кода старого api.py (август,
    аккаунт владельца): algoType=CONDITIONAL; triggerPrice вместо
    stopPrice; clientAlgoId вместо newClientOrderId. priceProtect НЕ
    отправляем: в рабочем коде отсутствовал, поддержка эндпоинтом не
    подтверждена [НЕУВЕРЕН]. Статусная модель совместима: NEW/CANCELED/
    FILLED/EXPIRED покрываются OrderState.from_exchange.
    """
    assert request.stop_price is not None  # инвариант OrderRequest
    params: dict[str, str] = {
        "algoType": "CONDITIONAL",
        "symbol": request.symbol,
        "side": request.side.value,
        "type": request.kind.value,
        "triggerPrice": to_api_str(request.stop_price),
        "workingType": request.working_type,
        "clientAlgoId": request.client_order_id,
    }
    if request.close_position:
        params["closePosition"] = "true"
    else:
        assert request.qty is not None  # инвариант OrderRequest
        params["quantity"] = to_api_str(request.qty)
        if request.reduce_only:
            params["reduceOnly"] = "true"
    return params


def _ack_from_algo_raw(client_order_id: str, raw: Any) -> OrderAck:
    """Ответ Algo API -> OrderAck (валидация на границе).

    Терпимость полей: POST-ответ и GET-статус различаются полнотой
    (полный формат POST [НЕУВЕРЕН] — снимает verify_api); статус ищем
    в algoStatus, потом status; avg — actualPrice/avgPrice.
    """
    if not isinstance(raw, Mapping) or not raw:
        raise ValueError(f"алго-ответ пуст/не объект: {raw!r}")
    raw_status = raw.get("algoStatus") or raw.get("status")
    if not isinstance(raw_status, str):
        raise ValueError(f"алго-ответ без статуса: {raw!r}")
    state = OrderState.from_exchange(raw_status)
    if state is None:
        raise ValueError(f"неизвестный алго-статус: {raw_status!r}")
    algo_id = raw.get("algoId")
    avg = _safe_dec(raw.get("actualPrice")) or _safe_dec(raw.get("avgPrice"))
    if avg is not None and avg <= 0:
        avg = None
    executed = _safe_dec(raw.get("executedQty"))
    if executed is not None and executed <= 0:
        executed = None
    return OrderAck(
        client_order_id=client_order_id,
        exchange_order_id=algo_id if isinstance(algo_id, int) else None,
        status=state, avg_price=avg, executed_qty=executed, raw=raw,
    )


def _algo_empty(raw: Any) -> bool:
    """«Ордера нет» по алго-GET: пустой объект или отсутствие clientAlgoId.

    Рабочий старый код трактовал пустой resp как «нет»; отказ с кодом
    (какой именно — [НЕУВЕРЕН]) приходит как BinanceApiError и
    обрабатывается вызывающим.
    """
    return (not isinstance(raw, Mapping)) or (
        raw.get("clientAlgoId") is None and raw.get("algoId") is None
    )


class RealVenue(ExecutionVenue):
    """Провайдер реальной торговли Binance USDⓈ-M.

    События (ORDER_TRADE_UPDATE/ACCOUNT_UPDATE/реконнекты) прилетают
    в общую очередь из UserStream; venue только исполняет команды.
    """

    def __init__(
        self,
        rest: BinanceRestClient,
        events: asyncio.Queue[VenueEvent],
        resolve_attempts: int = 10,
        resolve_interval_s: float = 0.3,
        not_found_confirm: int = 2,
    ) -> None:
        """resolve_* — параметры цикла выяснения статуса после таймаута."""
        self._rest = rest
        self._events = events
        self._resolve_attempts = resolve_attempts
        self._resolve_interval = resolve_interval_s
        self._not_found_confirm = not_found_confirm

    @property
    def events(self) -> asyncio.Queue[VenueEvent]:
        """Очередь событий user-stream (общая с движком)."""
        return self._events

    def feed_price(self, symbol: str, price: Decimal) -> None:
        """No-op: источник истины real — WS биржи, не локальный фид."""
        del symbol, price

    async def execute_order(self, request: OrderRequest) -> OrderAck:
        """Отправить ордер; таймаут -> resolve-цикл (см. модуль).

        Returns:
            OrderAck: NEW/PARTIALLY_FILLED/FILLED (успех),
            REJECTED (детерминированный отказ биржи, код — в raw),
            NOT_FOUND (стабильно не вставал — безопасный повтор),
            TIMEOUT_UNKNOWN (статус не выяснен — движок решает).

        Raises:
            InsufficientFundsError: -2010/-2019 — реджект без ретрая.
        """
        if request.kind is OrderKind.MARKET:
            params = build_order_params(request)
        else:
            # [МИГРАЦИЯ] условные ордера (SL/TP/RS) — Algo Order API:
            # обычный /fapi/v1/order возвращает -4120 (подтверждено live)
            params = build_algo_params(request)
        try:
            if request.kind is OrderKind.MARKET:
                raw = await self._rest.new_order(params)
            else:
                raw = await self._rest.algo_order_new(params)
        except FilterFailureError as exc:
            return self._rejected(request.client_order_id, exc)
        except BinanceApiError as exc:
            return self._rejected(request.client_order_id, exc)
        except (TransportTimeout, TransportError):
            logger.error(
                "order %s: ответ потерян — resolve по clientOrderId",
                request.client_order_id,
            )
            return await self._resolve(request)
        if request.kind is OrderKind.MARKET:
            return _ack_from_raw(request.client_order_id, raw)
        return _ack_from_algo_raw(request.client_order_id, raw)

    async def cancel_order(self, symbol: str, client_order_id: str) -> OrderAck:
        """Отменить ордер по clientOrderId.

        Алго-first (все наши условные — алго; MARKET не отменяем).
        -2011 на алго-DELETE трактуем как штатную гонку cancel/fill
        (как в обычном пути) — эмпирика кода [НЕУВЕРЕН] снимет проба.

        Raises:
            UnknownOrderError: -2011 (гонка); OrderNotFoundError: -2013
            в обоих путях — ордера нет.
        """
        raw = await self._rest.algo_order_cancel(symbol, client_order_id)
        if isinstance(raw, Mapping) and raw and not _algo_empty(raw):
            return _ack_from_algo_raw(client_order_id, raw)
        return OrderAck(
            client_order_id=client_order_id,
            exchange_order_id=None, status=OrderState.CANCELED,
            raw={"cancelled": "algo", "empty_response": True},
        )

    async def query_order(self, symbol: str, client_order_id: str) -> OrderAck | None:
        """Статус ордера; None = ордера нет.

        Алго-first: условные — через GET /fapi/v1/algoOrder; если биржа
        говорит «нет» (пустой объект) или -2013 — fallback на обычный
        GET /fapi/v1/order (путь resolve входа MARKET после таймаута:
        алго-GET честно ответит «нет», обычный найдёт исполнение).
        """
        try:
            raw = await self._rest.algo_order_query(symbol, client_order_id)
        except OrderNotFoundError:
            raw = None
        if raw is not None and not _algo_empty(raw):
            return _ack_from_algo_raw(client_order_id, raw)
        try:
            raw2 = await self._rest.get_order(
                symbol, orig_client_order_id=client_order_id
            )
        except OrderNotFoundError:
            return None
        return _ack_from_raw(client_order_id, raw2)

    async def open_orders(self, symbol: str) -> list[OrderAck]:
        """Активные условные ордера символа (Часть A: SL жив?).

        Только /fapi/v1/openAlgoOrders: обычный openOrders алго не
        возвращает (-4120, live-подтверждение), а открытых обычных
        ордеров у системы не бывает (все MARKET). Контракт движка
        прежний: client_order_id в ack = clientAlgoId на бирже.
        """
        raw = await self._rest.algo_orders_open(symbol)
        if not isinstance(raw, list):
            raise BinanceApiError(None, 200, "openAlgoOrders вернул не список",
                                  "/fapi/v1/openAlgoOrders")
        acks: list[OrderAck] = []
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            cid = item.get("clientAlgoId")
            if isinstance(cid, str):
                acks.append(_ack_from_algo_raw(cid, item))
        return acks

    async def cancel_all_orders(self, symbol: str) -> int:
        """Убрать все ордера символа (аварийная ветка). Возвращает число снятий."""
        raw = await self._rest.cancel_all_open_orders(symbol)
        return len(raw) if isinstance(raw, list) else 0

    async def positions(self) -> list[ExchangePosition]:
        """positionRisk -> позиции (источник истины reconciliation).

        [НЕУВЕРЕН] имена полей v3 (positionAmt/entryPrice/
        unRealizedProfit) — V-API; fallback-варианты обрабатываются.
        """
        raw = await self._rest.position_risk()
        if not isinstance(raw, list):
            raise BinanceApiError(None, 200, "positionRisk вернул не список",
                                  "/fapi/v3/positionRisk")
        result: list[ExchangePosition] = []
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            symbol = item.get("symbol")
            amount = _safe_dec(item.get("positionAmt"))
            entry = _safe_dec(item.get("entryPrice"))
            if not isinstance(symbol, str) or amount is None or entry is None:
                logger.error("positionRisk: битая запись %s", item)
                continue
            if amount == 0:
                continue
            pnl = _safe_dec(
                item.get("unRealizedProfit", item.get("unrealizedProfit"))
            )
            result.append(
                ExchangePosition(
                    symbol=symbol,
                    side="LONG" if amount > 0 else "SHORT",
                    qty=abs(amount),
                    entry_price=entry,
                    unrealized_pnl=pnl,
                )
            )
        return result

    async def available_balance(self, asset: str = "USDT") -> Decimal:
        """Доступный баланс актива из /fapi/v3/balance.

        [НЕУВЕРЕН] поле availableBalance (fallback: balance) — V-API.
        """
        raw = await self._rest.balance()
        if not isinstance(raw, list):
            raise BinanceApiError(None, 200, "balance вернул не список",
                                  "/fapi/v3/balance")
        for item in raw:
            if isinstance(item, Mapping) and item.get("asset") == asset:
                value = _safe_dec(item.get("availableBalance", item.get("balance")))
                if value is not None:
                    return value
        raise BinanceApiError(None, 200, f"актив {asset} не найден в балансе",
                              "/fapi/v3/balance")

    async def user_trades(self, symbol: str, start_ms: int) -> list[Fill]:
        """Сделки символа с start_ms (reconciliation «позиция закрылась»)."""
        raw = await self._rest.user_trades(symbol, start_ms)
        if not isinstance(raw, list):
            raise BinanceApiError(None, 200, "userTrades вернул не список",
                                  "/fapi/v1/userTrades")
        fills: list[Fill] = []
        for item in raw:
            if not isinstance(item, Mapping):
                continue
            try:
                fills.append(
                    Fill(
                        exchange_order_id=item.get("orderId")
                        if isinstance(item.get("orderId"), int) else None,
                        order_client_id=None,
                        trade_id=item["id"],
                        ts_ms=item["time"],
                        price=Decimal(str(item["price"])),
                        qty=Decimal(str(item["qty"])),
                        commission=Decimal(str(item["commission"])),
                        commission_asset=str(item["commissionAsset"]),
                    )
                )
            except (KeyError, ArithmeticError, Exception):
                logger.error("userTrades: битая запись пропущена: %s", item)
        return fills

    async def commission_rate(self, symbol: str) -> tuple[Decimal, Decimal]:
        """(maker, taker) — реальные ставки символа."""
        return await self._rest.commission_rate(symbol)

    # ---------- внутреннее ----------

    def _rejected(self, client_order_id: str, exc: BinanceApiError) -> OrderAck:
        """OrderAck для детерминированного отказа биржи (код в raw)."""
        return OrderAck(
            client_order_id=client_order_id,
            exchange_order_id=None,
            status=OrderState.REJECTED,
            raw={"code": exc.code, "msg": exc.message, "http": exc.http_status},
        )

    async def _resolve(self, request: OrderRequest) -> OrderAck:
        """Resolve-цикл: фактический статус ордера после потери ответа.

        Стабильный NOT_FOUND (not_found_confirm подряд) = ордер не
        вставал -> повтор с тем же clientOrderId безопасен (Д4).
        """
        not_found_streak = 0
        for _ in range(self._resolve_attempts):
            await asyncio.sleep(self._resolve_interval)
            try:
                ack = await self.query_order(request.symbol, request.client_order_id)
            except (TransportTimeout, TransportError) as exc:
                logger.warning("resolve %s: транспорт: %s",
                               request.client_order_id, exc)
                continue
            if ack is None:
                not_found_streak += 1
                if not_found_streak >= self._not_found_confirm:
                    return OrderAck(
                        client_order_id=request.client_order_id,
                        exchange_order_id=None,
                        status=OrderState.NOT_FOUND,
                        raw={"resolved": "not_found"},
                    )
                continue
            return ack
        return OrderAck(
            client_order_id=request.client_order_id,
            exchange_order_id=None,
            status=OrderState.TIMEOUT_UNKNOWN,
            raw={"resolved": "unresolved"},
        )


def _safe_dec(raw: Any) -> Decimal | None:
    """Decimal из ответа биржи или None (мусор не должен ронять venue)."""
    if raw is None or isinstance(raw, bool):
        return None
    try:
        value = Decimal(str(raw))
    except ArithmeticError:
        return None
    return value if value.is_finite() else None


def _ack_from_raw(client_order_id: str, raw: Any) -> OrderAck:
    """Ответ биржи -> OrderAck (валидация на границе).

    Raises:
        ValueError: статус отсутствует/неизвестен — движок обязан
        залогировать инцидент и разрешить через query_order.
    """
    if not isinstance(raw, Mapping):
        raise ValueError(f"ответ ордера — не объект: {raw!r}")
    raw_status = raw.get("status")
    if not isinstance(raw_status, str):
        raise ValueError(f"ответ без status: {raw!r}")
    state = OrderState.from_exchange(raw_status)
    if state is None:
        raise ValueError(f"неизвестный статус ордера: {raw_status!r}")
    exchange_id = raw.get("orderId")
    avg = _safe_dec(raw.get("avgPrice"))
    if avg is not None and avg <= 0:
        avg = None
    executed = _safe_dec(raw.get("executedQty"))
    if executed is not None and executed <= 0:
        executed = None
    return OrderAck(
        client_order_id=client_order_id,
        exchange_order_id=exchange_id if isinstance(exchange_id, int) else None,
        status=state,
        avg_price=avg,
        executed_qty=executed,
        raw=raw,
    )
