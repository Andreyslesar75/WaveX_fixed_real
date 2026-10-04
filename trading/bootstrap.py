# trading/bootstrap.py
"""Сборка торговой части: одна точка, где решается real/paper и вяжутся
REST/WS/filters/storage/engine/facade.

Безопасность (Д1): REAL_TRADING/TESTNET — только env (П20, дефолт false);
real-prod + SL_MAINTENANCE=exchange без ALLOW_EXCH_SL — отказ старта (В3).
"""
from __future__ import annotations

import asyncio
import logging
import os
from decimal import Decimal
from pathlib import Path
from typing import Any, Awaitable, Callable

import aiohttp

from .binance.rest import AioHttpTransport, BinanceRestClient
from .binance.user_stream import UserStream, WsMessage
from .binance.venue import RealVenue
from .clock import Clock
from .engine import TradingEngine
from .facade import PositionManager
from .filters import FiltersCache
from .levels import CalculationsLevelCalculator
from .notifier import LogNotifier
from .paper.venue import PaperVenue
from .ratelimit import RateLimiter
from .settings import EngineSettings
from .storage import Storage
from .types import Mode
from .venue import VenueEvent

logger = logging.getLogger(__name__)

KlinesProvider = Callable[[str, str, int], Awaitable[Any]]


class _AioWsConnection:
    """aiohttp-WS под протокол WsConnection (user_stream)."""

    def __init__(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        self._ws = ws

    async def receive(self, timeout_s: float) -> Any:
        """Одно сообщение; таймаут -> TimeoutError (сигнал тишины)."""
        try:
            msg = await asyncio.wait_for(self._ws.receive(), timeout_s)
        except asyncio.TimeoutError:
            raise TimeoutError from None
        if msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING,
                        aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
            return WsMessage(closed=True, data="")
        return WsMessage(closed=False, data=msg.data)

    async def close(self) -> None:
        await self._ws.close()


class _AioWsFactory:
    """Фабрика WS-соединений (heartbeat=20 с — ping/pong ниже детектора тишины)."""

    def __init__(self, session: aiohttp.ClientSession) -> None:
        self._session = session

    async def connect(self, url: str) -> _AioWsConnection:
        ws = await self._session.ws_connect(url, heartbeat=20.0)
        return _AioWsConnection(ws)


async def build_position_manager(
    session: aiohttp.ClientSession,
    klines_provider: KlinesProvider | None,
    cfg: Any = None,
) -> PositionManager:
    """Собрать фасад. Ключи — из env (Config), сессия — сканера (переиспользуем).

    Raises:
        RuntimeError: real включён без ключей / запрещённый обменный
        SL-режим / не удалось инициализировать фильтры.
    """
    from config import Config as _Cfg  # граница с конфигом проекта

    cfg = cfg or _Cfg
    real = bool(getattr(cfg, "REAL_TRADING", False))
    testnet = bool(getattr(cfg, "BINANCE_TESTNET", False))
    settings = EngineSettings.from_config(cfg)

    mode = Mode.REAL if real else Mode.PAPER
    if real:
        api_key, secret = cfg.BINANCE_API_KEY, cfg.BINANCE_API_SECRET
        if not api_key or not secret:
            raise RuntimeError("REAL_TRADING включён, но ключи не заданы")
        if (
            getattr(cfg, "SL_MAINTENANCE", "program") == "exchange"
            and not testnet
            and os.getenv("ALLOW_EXCH_SL", "") != "true"
        ):
            raise RuntimeError(  # guard В3: непроверенный режим не идёт на прод
                "SL_MAINTENANCE=exchange запрещён на проде без ALLOW_EXCH_SL=true"
            )
        logger.error("=== REAL TRADING %s ===", "TESTNET" if testnet else "PROD")

    db_path = Path(getattr(cfg, "DB_FILE_V2", "wavex_v2.db"))
    storage = Storage(db_path)
    storage.initialize()

    base = (
        "https://testnet.binancefuture.com" if testnet else "https://fapi.binance.com"
    )

    async def public_fetch(path: str, params: Any = None) -> Any:
        """GET публичного REST для FiltersCache (независим от старого api.py)."""
        url = f"{base}{path}"
        if params:
            from urllib.parse import urlencode
            url = f"{url}?{urlencode(params)}"
        async with session.get(url) as resp:
            import json
            return json.loads(await resp.text())

    filters = FiltersCache(public_fetch)  # сигнатура JsonFetcher: (path, params)

    events: "asyncio.Queue[VenueEvent]" = asyncio.Queue()

    if real:
        limiter = RateLimiter(
            weight_per_min=6000 if testnet else 2400,
            orders_per_10s=300, orders_per_min=1200,
        )
        clock = Clock(public_fetch)
        await clock.sync()
        rest = BinanceRestClient(
            transport=AioHttpTransport(session), api_key=api_key,
            secret_key=secret, base_url=base, limiter=limiter, clock=clock,
        )
        venue = RealVenue(rest, events)
        user_stream = UserStream(
            api=rest,
            factory=_AioWsFactory(session),
            ws_base="wss://stream.binancefuture.com" if testnet
            else "wss://fstream.binance.com",
            events=events,
        )
        capital_base = Decimal("0")
        try:
            capital_base = await venue.available_balance()
        except Exception as exc:
            logger.warning("bootstrap: баланс недоступен на старте: %s", exc)
    else:
        venue = PaperVenue(
            starting_capital=Decimal(str(getattr(cfg, "PAPER_BALANCE", 1000.0))),
            price_provider=lambda s: None,  # live-цены придут через feed_price
        )
        capital_base = Decimal(str(getattr(cfg, "PAPER_BALANCE", 1000.0)))

    engine = TradingEngine(
        venue=venue, storage=storage, settings=settings,
        filters_cache=filters, calculator=CalculationsLevelCalculator(),
        notifier=LogNotifier(), mode=mode, capital_base=capital_base,
        volume_provider=klines_provider,
    )

    # Фильтры — блокирующе до старта цикла (§3 черновика); реального
    # рестарт-ретрая здесь нет: фасад.start() не стартует торговлю без них.
    await filters.initialize(
        attempts=int(getattr(cfg, "FILTERS_CACHE_INIT_RETRIES", 3)),
        backoff_start_s=0.5,
    )
    
    # фоновое обновление фильтров + user stream (real)
    stop_holder: dict[str, asyncio.Event] = {"stop": asyncio.Event()}

    async def _filters_bg() -> None:
        await filters.run_background(settings.filters_refresh_hours, stop_holder["stop"])

    async def _clock_bg() -> None:
        await clock.run_background(900.0, stop_holder["stop"])  # 15 мин, §13

    bg: list[asyncio.Task[None]] = [
        asyncio.create_task(_filters_bg(), name="filters-bg"),
    ]
    if real:
        async def _uds() -> None:
            await user_stream.run(stop_holder["stop"])
        bg.append(asyncio.create_task(_uds(), name="user-stream"))
        bg.append(asyncio.create_task(_clock_bg(), name="clock-bg"))

    facade = PositionManager(
        engine=engine, storage_path=db_path, settings=settings,
        mode=mode, capital_base=capital_base,
    )
    facade._bootstrap_tasks = bg
    return facade