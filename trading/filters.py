# trading/filters.py
"""Кэш биржевых фильтров (exchangeInfo) — Д2/filters.

Модель обновления (почему): блокирующая инициализация до старта
торгового цикла (без фильтров нельзя корректно округлить ни один
ордер); фоновое обновление — атомарной заменой всего снапшота раз в
FILTERS_REFRESH_HOURS (поэлементная замена допускает несогласованное
состояние step/minQty в момент округления ордера); точечный
refresh_symbol — по -1013, с одним повтором ордера (§3 черновика).

Транспорт отвязан (JsonFetcher) — парсинг тестируется без сети.
"""
from __future__ import annotations

import asyncio
import logging
import time
from decimal import Decimal
from typing import Any, Mapping

from .types import JsonFetcher, SymbolFilters

logger = logging.getLogger(__name__)

_EXCHANGE_INFO_PATH = "/fapi/v1/exchangeInfo"


class FiltersError(RuntimeError):
    """Невалидируемый exchangeInfo / исчерпаны попытки инициализации."""


def _now_ms() -> int:
    """Локальное время в мс (метки обновления кэша)."""
    return int(time.time() * 1000)


def _decimal(raw: Any, field: str, symbol: str) -> Decimal:
    """Конвертировать поле биржи в Decimal с валидацией.

    Raises:
        FiltersError: поле не число/строка, либо NaN/Inf.
    """
    if isinstance(raw, bool) or not isinstance(raw, (str, int, float)):
        raise FiltersError(f"{symbol}: {field} ожидается числом/строкой, получено {raw!r}")
    d = Decimal(str(raw))
    if not d.is_finite():
        raise FiltersError(f"{symbol}: {field} = NaN/Inf")
    return d


def parse_symbol_filters(entry: Mapping[str, Any]) -> SymbolFilters:
    """Валидировать одну запись symbols[] в SymbolFilters.

    Требуемые фильтры: PRICE_FILTER, MARKET_LOT_SIZE (fallback
    LOT_SIZE — приоритет у рыночного), MIN_NOTIONAL. Прочие типы
    фильтров игнорируются с debug-логом (биржа расширяет набор
    фильтров независимо от нас).

    Raises:
        FiltersError: обязательное поле/фильтр отсутствует или
        некорректен; вызывающий сборщик пропускает символ с ошибкой
        в лог (один мусорный символ не должен валить систему).
    """
    symbol = entry.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        raise FiltersError(f"запись без symbol: {entry!r}")
    status = entry.get("status")
    if not isinstance(status, str):
        raise FiltersError(f"{symbol}: status отсутствует")

    filters_raw = entry.get("filters")
    if not isinstance(filters_raw, list):
        raise FiltersError(f"{symbol}: filters отсутствует")

    tick_size: Decimal | None = None
    market_lot: Mapping[str, Any] | None = None
    plain_lot: Mapping[str, Any] | None = None
    min_notional: Decimal | None = None
    trigger_protect: Decimal | None = None

    for f in filters_raw:
        if not isinstance(f, Mapping):
            raise FiltersError(f"{symbol}: не-словарь в filters: {f!r}")
        ftype = f.get("filterType")
        if not isinstance(ftype, str):
            raise FiltersError(f"{symbol}: filterType отсутствует: {f!r}")
        if ftype == "PRICE_FILTER":
            tick_size = _decimal(f.get("tickSize"), "tickSize", symbol)
        elif ftype == "MARKET_LOT_SIZE":
            market_lot = f
        elif ftype == "LOT_SIZE":
            plain_lot = f
        elif ftype == "MIN_NOTIONAL":
            # [НЕУВЕРЕН] fapi использует поле "notional", spot —
            # "minNotional"; принимаем оба, фактическое имя фиксирует
            # V-API на реале.
            min_notional = _decimal(
                f.get("notional", f.get("minNotional")), "notional", symbol
            )
        elif ftype == "PRICE_PROTECT":
            trigger_protect = _decimal(f.get("triggerProtect"), "triggerProtect", symbol)
        else:
            logger.debug("ignoring filter %s for %s", ftype, symbol)

    lot = market_lot if market_lot is not None else plain_lot
    if lot is None:
        raise FiltersError(f"{symbol}: нет MARKET_LOT_SIZE/LOT_SIZE")

    step_size = _decimal(lot.get("stepSize"), "stepSize", symbol)
    min_qty = _decimal(lot.get("minQty"), "minQty", symbol)
    max_qty = _decimal(lot.get("maxQty"), "maxQty", symbol)

    if tick_size is None:
        raise FiltersError(f"{symbol}: нет PRICE_FILTER")
    if min_notional is None:
        raise FiltersError(f"{symbol}: нет MIN_NOTIONAL")

    price_precision = entry.get("pricePrecision")
    quantity_precision = entry.get("quantityPrecision")
    if not isinstance(price_precision, int) or not isinstance(quantity_precision, int):
        raise FiltersError(f"{symbol}: pricePrecision/quantityPrecision некорректны")

    return SymbolFilters(
        symbol=symbol,
        status=status,
        tick_size=tick_size,
        step_size=step_size,
        min_qty=min_qty,
        max_qty=max_qty,
        min_notional=min_notional,
        price_precision=price_precision,
        quantity_precision=quantity_precision,
        trigger_protect=trigger_protect,
    )


def parse_exchange_info(payload: Mapping[str, Any]) -> dict[str, SymbolFilters]:
    """Валидировать полный exchangeInfo в снапшот {symbol: filters}.

    Raises:
        FiltersError: ответ вообще не exchangeInfo (нет symbols) —
        ошибка транспорта/границы; либо ни одного валидного символа.
    """
    symbols = payload.get("symbols")
    if not isinstance(symbols, list):
        raise FiltersError(f"ответ не похож на exchangeInfo: keys={list(payload)[:10]}")
    snapshot: dict[str, SymbolFilters] = {}
    skipped = 0
    for entry in symbols:
        if not isinstance(entry, Mapping):
            skipped += 1
            continue
        try:
            sf = parse_symbol_filters(entry)
        except FiltersError as exc:
            logger.error("exchangeInfo: символ пропущен: %s", exc)
            skipped += 1
            continue
        snapshot[sf.symbol] = sf
    if skipped:
        logger.warning("exchangeInfo: пропущено записей: %d", skipped)
    if not snapshot:
        raise FiltersError("exchangeInfo не дал ни одного валидного символа")
    return snapshot


class FiltersCache:
    """Кэш фильтров: блокирующая инициализация + атомарные обновления.

    Модель конкурентности: один asyncio-цикл. get() синхронный —
    чтение self._snapshot атомарно в asyncio-модели (присваивание
    ссылки без await между чтением и записью); _refresh_lock
    защищает только параллельные refresh-задачи.
    """

    def __init__(self, fetch_json: JsonFetcher) -> None:
        self._fetch_json = fetch_json
        self._snapshot: dict[str, SymbolFilters] = {}
        self._ready = asyncio.Event()
        self._refresh_lock = asyncio.Lock()
        self._last_error: str | None = None

    @property
    def last_error(self) -> str | None:
        """Текст последней ошибки обновления (диагностика/REPORT)."""
        return self._last_error

    @property
    def symbols_count(self) -> int:
        """Число символов в снапшоте (health-check)."""
        return len(self._snapshot)

    def get(self, symbol: str) -> SymbolFilters | None:
        """Фильтры символа или None (гейт входа ответит SYMBOL_NOT_TRADING)."""
        return self._snapshot.get(symbol)

    async def initialize(self, attempts: int = 5, backoff_start_s: float = 0.5) -> None:
        """Блокирующая инициализация с retry+backoff (§3: до старта цикла).

        Raises:
            FiltersError: исчерпаны попытки — engine не стартует
            торговый цикл (сигналы не обрабатываются), только retry.
        """
        delay = backoff_start_s
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                await self._refresh_all()
                self._ready.set()
                return
            except (FiltersError, asyncio.TimeoutError, OSError) as exc:
                last_exc = exc
                self._last_error = f"init attempt {attempt}: {exc}"
                logger.warning(
                    "filters init: попытка %d/%d не удалась: %s", attempt, attempts, exc
                )
                if attempt < attempts:
                    await asyncio.sleep(delay)
                    delay = min(delay * 2.0, 30.0)
        raise FiltersError(
            f"инициализация фильтров не удалась за {attempts} попыток: {last_exc}"
        )

    async def wait_ready(self) -> None:
        """Дождаться готовности кэша (стартовый reconciliation)."""
        await self._ready.wait()

    async def _refresh_all(self) -> None:
        """Полное обновление с атомарной заменой снапшота.

        Raises:
            FiltersError/OSError: транспорт или парсинг провалились.
        """
        payload = await self._fetch_json(_EXCHANGE_INFO_PATH, None)
        if not isinstance(payload, Mapping):
            raise FiltersError(f"exchangeInfo вернул не объект: {type(payload)!r}")
        snapshot = parse_exchange_info(payload)
        async with self._refresh_lock:
            self._snapshot = snapshot  # атомарная замена целиком
            self._last_error = None
        logger.info("filters: снапшот обновлён, символов=%d", len(snapshot))

    async def refresh_all(self) -> None:
        """Фоновое полное обновление; ошибка НЕ снимает readiness.

        Почему: торгуем по старому согласованному снапшоту, инцидент
        FILTER_REFRESH_FAILED — виден в логах/БД.
        """
        try:
            await self._refresh_all()
        except (FiltersError, asyncio.TimeoutError, OSError) as exc:
            self._last_error = str(exc)
            logger.error(
                "filters refresh failed: %s (продолжаем на старом снапшоте)", exc
            )

    async def refresh_symbol(self, symbol: str) -> bool:
        """Точечное обновление по -1013: exchangeInfo?symbol=XXX.

        Returns:
            True при успехе. False означает: -1013 вызван не устаревшим
            кэшем (реальная аномалия) — вызывающий делает финальный
            реджект (§3 черновика).
        """
        payload = await self._fetch_json(_EXCHANGE_INFO_PATH, {"symbol": symbol})
        if not isinstance(payload, Mapping):
            return False
        symbols = payload.get("symbols")
        # [ПРЕДПОЛОЖЕНИЕ] точечный ответ имеет ту же структуру {"symbols": [...]};
        # проверяется V-API на реале.
        if not isinstance(symbols, list) or len(symbols) != 1:
            logger.error("filters: точечный ответ по %s неожиданного формата", symbol)
            return False
        try:
            sf = parse_symbol_filters(symbols[0])
        except FiltersError as exc:
            logger.error("filters: точечный парсинг %s провалился: %s", symbol, exc)
            return False
        async with self._refresh_lock:
            self._snapshot[symbol] = sf
        return True

    async def run_background(
        self, interval_h: float, stop: asyncio.Event | None = None
    ) -> None:
        """Периодическое обновление (FILTERS_REFRESH_HOURS из Config)."""
        interval_s = max(interval_h, 0.1) * 3600.0
        while True:
            if stop is not None and stop.is_set():
                return
            await asyncio.sleep(interval_s)
            if stop is not None and stop.is_set():
                return
            await self.refresh_all()