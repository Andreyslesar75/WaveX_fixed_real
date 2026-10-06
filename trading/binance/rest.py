# trading/binance/rest.py
"""REST-слой Binance USDⓈ-M: подпись, rate limit, типизированные ошибки.

Идемпотентность и таймауты (§5/§11 черновика):
- POST/DELETE ордеров НЕ ретраятся слепо: потеря ответа оставляет
  ордер в TIMEOUT_UNKNOWN, и фактический статус выясняется GET-ом
  по origClientOrderId (политика — в RealVenue._resolve);
- GET-эндпоинты ретраятся с экспоненциальным backoff — они безопасны.

Коды ошибок Binance -> типизированные исключения:
-1013 -> FilterFailureError; -2011 -> UnknownOrderError (гонка
cancel/fill — норма); -2013 -> OrderNotFoundError (ордера не было);
-2010/-2019 -> InsufficientFundsError [НЕУВЕРЕН: у фьючерсов код
маржи — -2019, проверит V-API-6]; -1021 -> TimestampSyncError;
429/418 -> TransientError + глобальная пауза limiter.

[НЕУВЕРЕН] Веса запросов (WEIGHT_*) — консервативные, сверяются по
заголовкам X-MBX-USED-WEIGHT-1M в verify_api (V-API-7).
[ПРЕДПОЛОЖЕНИЕ] Параметры POST-запросов fapi передаются в query
string (как в официальном коннекторе), не в body. Проверяется V-API.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import time
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Protocol
from urllib.parse import urlencode

from aiohttp import ClientTimeout

from ..clock import TimeProvider
from ..ratelimit import RateLimiter
from ..venue import InsufficientFundsError, UnknownOrderError

logger = logging.getLogger(__name__)

HttpMethod = str  # "GET" | "POST" | "PUT" | "DELETE"

# --- Веса запросов (консервативные; фактические подтверждает V-API-7) ---
WEIGHT_SERVER_TIME = 1
WEIGHT_EXCHANGE_INFO = 1
WEIGHT_ORDER_ACTION = 1      # фактический вес ордерных POST/DELETE = 0
WEIGHT_QUERY_ORDER = 1
WEIGHT_OPEN_ORDERS = 1       # с параметром symbol
WEIGHT_POSITION_RISK = 5     # v3
WEIGHT_BALANCE = 5           # v3
WEIGHT_USER_TRADES = 5
WEIGHT_COMMISSION_RATE = 5
WEIGHT_LISTEN_KEY = 1


class TransportTimeout(Exception):
    """Таймаут HTTP-запроса; запрос мог дойти до биржи."""


class TransportError(Exception):
    """Сетевая ошибка до/вместо ответа; запрос мог дойти до биржи."""


class TransientError(Exception):
    """429/418 — биржа просит подождать; повтор позже."""

    def __init__(self, status: int, retry_after_s: int) -> None:
        super().__init__(f"HTTP {status}, retry-after {retry_after_s}s")
        self.status = status
        self.retry_after_s = retry_after_s


class BinanceApiError(Exception):
    """Ошибочный ответ биржи с JSON-телом {code, msg}."""

    def __init__(
        self, code: int | None, http_status: int, message: str,
        path: str, params: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(f"[{code}] HTTP {http_status}: {message} ({path})")
        self.code = code
        self.http_status = http_status
        self.message = message
        self.path = path
        self.params = dict(params or {})


class FilterFailureError(BinanceApiError):
    """-1013: фильтр символа; кэш фильтров мог устареть (§3 черновика)."""


class OrderNotFoundError(BinanceApiError):
    """-2013: ордера с таким id нет — либо не вставал, либо исчез."""


class TimestampSyncError(BinanceApiError):
    """-1021: рассинхрон времени; форс-ресинк clock (§13 черновика)."""


class Transport(Protocol):
    """Транспорт HTTP: одна точка, где живёт реальная сеть.

    Возвращает (status, headers, parsed_json). Отсутствие body -> None.
    """

    async def request(
        self, method: HttpMethod, url: str,
        headers: Mapping[str, str], timeout_s: float,
    ) -> tuple[int, dict[str, str], Any]: ...


class AioHttpTransport:
    """Транспорт на aiohttp: persistent-сессия, явный таймаут.

    Почему свой слой, а не python-binance: скорость ордерных операций
    критична (§0 черновика), контроль таймаутов/заголовков — явный.
    """

    def __init__(self, session: Any) -> None:
        """session — aiohttp.ClientSession (Any: aiohttp не типизирован для strict)."""
        self._session = session
        self.last_headers: dict[str, str] = {}

    async def request(
        self, method: HttpMethod, url: str,
        headers: Mapping[str, str], timeout_s: float,
    ) -> tuple[int, dict[str, str], Any]:
        """Выполнить запрос; сетевые ошибки -> TransportTimeout/TransportError."""
        import json as _json

        try:
            async with self._session.request(
                method, url, headers=dict(headers),
                timeout=ClientTimeout(total=timeout_s),
            ) as resp:
                status = resp.status
                hdrs = dict(resp.headers.items())
                self.last_headers = hdrs
                body = await resp.text()
        except TimeoutError as exc:
            raise TransportTimeout(str(exc)) from exc
        except Exception as exc:  # aiohttp.ClientError
            raise TransportError(str(exc)) from exc
        try:
            data: Any = _json.loads(body) if body else None
        except ValueError:
            data = {"raw_body": body}
        return status, hdrs, data


class BinanceRestClient:
    """REST-клиент fapi: подпись HMAC, лимиты, типизированные ошибки.

    Инвариант: clock уже синхронизирован до первого signed-вызова
    (engine гарантирует: clock.wait_synced() до старта торгового цикла).
    """

    def __init__(
        self,
        transport: Transport,
        api_key: str,
        secret_key: str,
        base_url: str,
        limiter: RateLimiter,
        clock: TimeProvider,  # trading.clock.Clock; Any чтобы не тянуть цикл импортов — см. note
        recv_window_ms: int = 5000,
        request_timeout_s: float = 10.0,
        get_retries: int = 2,
        get_backoff_s: float = 0.4,
    ) -> None:
        """clock передаётся duck-typed: достаточно now_ms() -> int.

        [НЕУВЕРЕН] recvWindow 5000 — дефолт Binance; §13 черновика.
        """
        self._transport = transport
        self._api_key = api_key
        self._secret = secret_key.encode()
        self._base_url = base_url.rstrip("/")
        self._limiter = limiter
        self._clock = clock
        self._recv_window = recv_window_ms
        self._timeout = request_timeout_s
        self._get_retries = get_retries
        self._get_backoff = get_backoff_s

    # ---------- публичные GET без подписи (совместимы с JsonFetcher) ----------

    async def public_get(
        self, path: str, params: Mapping[str, str] | None = None
    ) -> Any:
        """GET без подписи, без внутреннего ретрая.

        Ретраи здесь не нужны: FiltersCache.initialize реализует свой
        retry+backoff, Clock — свой.
        """
        weight = (
            WEIGHT_EXCHANGE_INFO
            if path.endswith("exchangeInfo")
            else WEIGHT_SERVER_TIME
        )
        return await self._call(
            "GET", path, dict(params) if params else None,
            signed=False, weight=weight, order_action=False, retries=0,
        )
    # ---------- typed API ----------

    async def exchange_info(self, symbol: str | None = None) -> Any:
        """GET /fapi/v1/exchangeInfo (полный или точечный по -1013)."""
        params = {"symbol": symbol} if symbol else None
        return await self._call("GET", "/fapi/v1/exchangeInfo", params,
                                signed=False, weight=WEIGHT_EXCHANGE_INFO,
                                order_action=False, retries=self._get_retries)

    async def create_listen_key(self) -> str:
        """POST /fapi/v1/listenKey — создать ключ user-stream.

        Returns:
            listenKey.

        Raises:
            BinanceApiError/транспортные — политика ретрая у UserStream.
        """
        data = await self._call("POST", "/fapi/v1/listenKey", None,
                                signed=False, weight=WEIGHT_LISTEN_KEY,
                                order_action=False, retries=0)
        key = data.get("listenKey") if isinstance(data, Mapping) else None
        if not isinstance(key, str) or not key:
            raise BinanceApiError(None, 200, "listenKey отсутствует в ответе",
                                  "/fapi/v1/listenKey")
        return key

    async def keepalive_listen_key(self) -> None:
        """PUT /fapi/v1/listenKey — продление (не подписывается)."""
        await self._call("PUT", "/fapi/v1/listenKey", None,
                         signed=False, weight=WEIGHT_LISTEN_KEY,
                         order_action=False, retries=0)

    async def new_order(self, params: Mapping[str, Any]) -> Any:
        """POST /fapi/v1/order — без слепого ретрая (см. докстринг модуля)."""
        return await self._call("POST", "/fapi/v1/order", dict(params),
                                signed=True, weight=WEIGHT_ORDER_ACTION,
                                order_action=True, retries=0)

    async def get_order(
        self, symbol: str,
        orig_client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> Any:
        """GET /fapi/v1/order по clientOrderId (идемпотентный запрос).

        Raises:
            OrderNotFoundError: -2013 — ордера нет (не вставал/исчез).
        """
        params: dict[str, Any] = {"symbol": symbol}
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        if order_id is not None:
            params["orderId"] = order_id
        return await self._call("GET", "/fapi/v1/order", params,
                                signed=True, weight=WEIGHT_QUERY_ORDER,
                                order_action=False, retries=self._get_retries)

    async def cancel_order(
        self, symbol: str,
        orig_client_order_id: str | None = None,
        order_id: int | None = None,
    ) -> Any:
        """DELETE /fapi/v1/order.

        Raises:
            UnknownOrderError: -2011 — ордер уже исполнен/отменён (норма при
            гонке cancel/fill, §9 черновика); OrderNotFoundError: -2013.
        """
        params: dict[str, Any] = {"symbol": symbol}
        if orig_client_order_id is not None:
            params["origClientOrderId"] = orig_client_order_id
        if order_id is not None:
            params["orderId"] = order_id
        return await self._call("DELETE", "/fapi/v1/order", params,
                                signed=True, weight=WEIGHT_ORDER_ACTION,
                                order_action=True, retries=0)

    async def cancel_all_open_orders(self, symbol: str) -> Any:
        """DELETE /fapi/v1/allOpenOrders?symbol= (аварийная уборка)."""
        return await self._call("DELETE", "/fapi/v1/allOpenOrders",
                                {"symbol": symbol}, signed=True,
                                weight=WEIGHT_ORDER_ACTION, order_action=True,
                                retries=0)

    # ---------- Algo Order API (условные ордера SL/TP; миграция §1) ----------

    async def algo_order_new(self, params: Mapping[str, Any]) -> Any:
        """POST /fapi/v1/algoOrder — постановка условного ордера.

        Без слепых ретраев (как и обычные ордерные POST): при потере
        ответа RealVenue выясняет статус через algo_order_query по
        clientAlgoId.

        Raises:
            FilterFailureError: -1013; BinanceApiError: прочие отказы.
        """
        return await self._call("POST", "/fapi/v1/algoOrder", dict(params),
                               signed=True, weight=WEIGHT_ORDER_ACTION,
                               order_action=True, retries=0)

    async def algo_order_query(
        self, symbol: str, client_algo_id: str
    ) -> Any:
        """GET /fapi/v1/algoOrder по clientAlgoId (resolve/статус).

        Returns:
            Тело ответа биржи (Mapping) — включая ПУСТОЙ объект при
            отсутствии ордера: в рабочем старом коде пустой resp трактовался
            как «ордера нет» (if resp: ...). Различение «пусто» vs «ошибка»
            здесь НЕ делается — политика у вызывающего (venue).

        Raises:
            BinanceApiError: коды биржи (какой именно код даёт биржа на
            несуществующий cid — [НЕУВЕРЕН], снимает проба verify_api).
        """
        return await self._call(
            "GET", "/fapi/v1/algoOrder",
            {"symbol": symbol, "clientAlgoId": client_algo_id},
            signed=True, weight=WEIGHT_QUERY_ORDER,
            order_action=False, retries=self._get_retries,
        )

    async def algo_order_cancel(
        self, symbol: str, client_algo_id: str
    ) -> Any:
        """DELETE /fapi/v1/algoOrder по clientAlgoId."""
        return await self._call(
            "DELETE", "/fapi/v1/algoOrder",
            {"symbol": symbol, "clientAlgoId": client_algo_id},
            signed=True, weight=WEIGHT_ORDER_ACTION,
            order_action=True, retries=0,
        )

    async def algo_orders_open(self, symbol: str) -> Any:
        """GET /fapi/v1/openAlgoOrders?symbol= (здоровье SL/TP, Часть A).

        ВАЖНО: обычный /fapi/v1/openOrders алго-ордера НЕ возвращает
        (подтверждено -4120 в live-прогоне) — для условных только этот
        эндпоинт.
        """
        return await self._call(
            "GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol},
            signed=True, weight=WEIGHT_OPEN_ORDERS,
            order_action=False, retries=self._get_retries,
        )

    async def open_orders(self, symbol: str) -> Any:
        """GET /fapi/v1/openOrders?symbol= (проверка здоровья SL, Часть A)."""
        return await self._call("GET", "/fapi/v1/openOrders",
                                {"symbol": symbol}, signed=True,
                                weight=WEIGHT_OPEN_ORDERS, order_action=False,
                                retries=self._get_retries)

    async def position_risk(self) -> Any:
        """GET /fapi/v3/positionRisk (источник истины при reconciliation)."""
        return await self._call("GET", "/fapi/v3/positionRisk", None,
                                signed=True, weight=WEIGHT_POSITION_RISK,
                                order_action=False, retries=self._get_retries)

    async def balance(self) -> Any:
        """GET /fapi/v3/balance (availableBalance для гейта входа)."""
        return await self._call("GET", "/fapi/v3/balance", None,
                                signed=True, weight=WEIGHT_BALANCE,
                                order_action=False, retries=self._get_retries)

    async def user_trades(self, symbol: str, start_ms: int, limit: int = 100) -> Any:
        """GET /fapi/v1/userTrades (окно <= 7 дней от start_ms, §12 черновика)."""
        return await self._call(
            "GET", "/fapi/v1/userTrades",
            {"symbol": symbol, "startTime": start_ms, "limit": limit},
            signed=True, weight=WEIGHT_USER_TRADES, order_action=False,
            retries=self._get_retries,
        )

    async def commission_rate(self, symbol: str) -> tuple[Decimal, Decimal]:
        """GET /fapi/v1/commissionRate -> (maker, taker).

        [НЕУВЕРЕН] имена полей makerCommissionRate/takerCommissionRate —
        подтверждает V-API. Используется движком для паритета комиссий.
        """
        data = await self._call("GET", "/fapi/v1/commissionRate",
                                {"symbol": symbol}, signed=True,
                                weight=WEIGHT_COMMISSION_RATE,
                                order_action=False, retries=self._get_retries)
        if not isinstance(data, Mapping):
            raise BinanceApiError(None, 200, "commissionRate вернул не объект",
                                  "/fapi/v1/commissionRate")
        maker = Decimal(str(data.get("makerCommissionRate", "0")))
        taker = Decimal(str(data.get("takerCommissionRate", "0")))
        return maker, taker

    async def get_position_mode(self) -> bool:
        """GET /fapi/v1/positionSide/dual -> dualSidePosition.

        True = Hedge — блокирующее расхождение (One-way зафиксирован,
        -4061 в §13 черновика). Проверка на старте, не в цикле.
        """
        data = await self._call("GET", "/fapi/v1/positionSide/dual", None,
                                signed=True, weight=1, order_action=False,
                                retries=self._get_retries)
        if isinstance(data, Mapping):
            return data.get("dualSidePosition") is True
        raise BinanceApiError(None, 200, "positionSide/dual вернул не объект",
                              "/fapi/v1/positionSide/dual")

    # ---------- служебные (утилита setup, Часть 4) ----------

    async def set_leverage(self, symbol: str, leverage: int) -> Any:
        """POST /fapi/v1/leverage — только ручная утилитой (§4 черновика)."""
        return await self._call("POST", "/fapi/v1/leverage",
                                {"symbol": symbol, "leverage": leverage},
                                signed=True, weight=1, order_action=True,
                                retries=0)

    async def set_dual_side(self, dual: bool) -> Any:
        """POST /fapi/v1/positionSide/dual (false = One-way)."""
        return await self._call("POST", "/fapi/v1/positionSide/dual",
                                {"dualSidePosition": str(dual).lower()},
                                signed=True, weight=1, order_action=True,
                                retries=0)

    # ---------- ядро ----------

    def _headers(self) -> dict[str, str]:
        """Заголовок API-ключа (обязателен и для listenKey)."""
        return {"X-MBX-APIKEY": self._api_key}

    def _sign(self, query: str) -> str:
        """HMAC-SHA256 секретом по точной строке запроса (идемпотентно)."""
        return hmac.new(self._secret, query.encode(), hashlib.sha256).hexdigest()

    async def _call(
        self, method: HttpMethod, path: str,
        params: dict[str, Any] | None, *, signed: bool,
        weight: int, order_action: bool, retries: int,
    ) -> Any:
        """Единый путь всех запросов: лимит -> подпись -> вызов -> разбор.

        Raises:
            TransportTimeout/TransportError: сеть (после ретраев GET);
            TransientError: 429/418 (лимитер уже поставлен на паузу);
            типизированные BinanceApiError: по коду биржи;
            BinanceApiError: нераспознанная ошибка.

        Правило ретраев: только GET и только не-ордерные вызовы;
        ордерные действия не ретраятся никогда (идемпотентность
        обеспечивается clientOrderId + resolve, не повтором POST).
        """
        url_base = f"{self._base_url}{path}"
        attempt = 0
        while True:
            if order_action:
                await self._limiter.acquire_order()
            else:
                await self._limiter.acquire_request(weight)
            final: dict[str, str] = {}
            for key, value in (params or {}).items():
                final[str(key)] = str(value)
            if signed:
                final["recvWindow"] = str(self._recv_window)
                final["timestamp"] = str(self._clock.now_ms())
            query = urlencode(final) if final else ""
            if signed and query:
                query = f"{query}&signature={self._sign(query)}"
            url = f"{url_base}?{query}" if query else url_base
            try:
                status, headers, data = await self._transport.request(
                    method, url, self._headers(), self._timeout,
                )
            except TransportTimeout:
                if attempt < retries:
                    attempt += 1
                    await asyncio.sleep(self._get_backoff * (2 ** (attempt - 1)))
                    continue
                raise
            except TransportError:
                if attempt < retries:
                    attempt += 1
                    await asyncio.sleep(self._get_backoff * (2 ** (attempt - 1)))
                    continue
                raise
            self._limiter.update_from_headers(headers)
            if status == 200:
                return data
            if status in (429, 418):
                raw_after = headers.get("Retry-After", "5")
                try:
                    after_s = int(raw_after)
                except ValueError:
                    after_s = 5
                self._limiter.pause(
                    int(time.time() * 1000) + (after_s + 1) * 1000,
                    f"HTTP {status}",
                )
                if attempt < retries:
                    attempt += 1
                    await asyncio.sleep(self._get_backoff * (2 ** (attempt - 1)))
                    continue
                raise TransientError(status, after_s)
            code: int | None = None
            message = "?"
            if isinstance(data, Mapping):
                raw_code = data.get("code")
                if isinstance(raw_code, int):
                    code = raw_code
                raw_msg = data.get("msg")
                if isinstance(raw_msg, str):
                    message = raw_msg
            if code == -1013:
                raise FilterFailureError(code, status, message, path, params)
            if code == -2011:
                raise UnknownOrderError(code, status, message, path, params)
            if code == -2013:
                raise OrderNotFoundError(code, status, message, path, params)
            if code == -1021:
                raise TimestampSyncError(code, status, message, path, params)
            if code in (-2010, -2019):
                raise InsufficientFundsError(code, status, message, path, params)
            raise BinanceApiError(code, status, message, path, params)
