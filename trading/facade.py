"""Фасад PositionManager-совместимости: scanner/gui работают со старым API.

Границы потоков (Д3): бот (asyncio) пишет через engine; GUI (tkinter)
читает ЛЕДИТОВЫЙ снимок _view — консистентный снапшот, собираемый
задачей _view_loop в потоке бота раз в 2 с (atomic swap атрибута —
GUI никогда не итерирует мутируемый словарь движка).

Девиации против старого (все — осознанные, REPORT):
- paper capital = base + Σnet (старый не возвращал capital при закрытии);
- win/loss считаются по полным закрытиям позиций (старый TP1 считал
  отдельной «сделкой»);
- breakevens — закрытия с |net|<0.005 (старый: pnl==0 события).
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping

from .engine import EntryIntent, TradingEngine
from .settings import EngineSettings
from .storage import StorageReader
from .types import Mode, Side

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class _View:
    """Иммутабельный снимок для GUI (меняется только целиком)."""

    capital: float
    total_pnl: float
    breakevens: int
    positions: dict[str, dict[str, Any]]


class PositionManager:
    """Старый публичный API поверх движка (контракт §2, сверен по коду)."""

    def __init__(
        self,
        engine: TradingEngine,
        storage_path: Path,
        settings: EngineSettings,
        mode: Mode,
        capital_base: Decimal,
        balance_refresh_sec: float = 60.0,
    ) -> None:
        """storage_path — для StorageReader (per-call соединения)."""
        self._engine = engine
        self._reader = StorageReader(storage_path)
        self._settings = settings
        self._mode = mode
        self._capital_base = capital_base
        self._balance_refresh_sec = balance_refresh_sec
        self._view = _View(
            capital=float(capital_base), total_pnl=0.0, breakevens=0,
            positions={},
        )
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task[None]] = []
        self._bootstrap_tasks: list[asyncio.Task[None]] = []  # наполняет bootstrap

    # ---------------- жизненный цикл ----------------

    async def start(self) -> bool:
        """Стартовая сверка + фоновые задачи движка.

        Returns:
            True если сверка прошла (торговля разрешена); False — бот
            продолжает мониторинг позиций, входы блокируются (паритет
            старого поведения scanner.init шаг 8).
        """
        try:
            await self._engine.startup_reconcile()
        except Exception as exc:
            logger.error("facade: стартовая сверка провалилась: %s", exc)
            ok = False
        else:
            ok = True
        self._tasks = [
            asyncio.create_task(self._engine.run(self._stop), name="engine-run"),
            asyncio.create_task(self._view_loop(), name="facade-view"),
        ]
        return ok

    def close(self) -> None:
        """Остановить задачи и writer (idempotent; сессию закрывает сканер)."""
        self._stop.set()
        for task in self._tasks + list(
            getattr(self, "_bootstrap_tasks", [])
        ):
            task.cancel()
        self._engine._storage.close()

    # ---------------- API для scanner ----------------

    async def open_position(
        self,
        symbol: str,
        price: float,
        score: float,
        confidence: str,
        klines_1h: list,
        high24: float,
        low24: float,
        structural_level: float | None = None,
        spread_pct: float = 0.0,
        side: str = "LONG",
        btc_trend: float = 0.0,
    ) -> tuple[bool, str]:
        """Входной сигнал -> движок. Совместимо 1:1 (scanner.scan шаг 7)."""
        intent = EntryIntent(
            symbol=symbol,
            price=Decimal(str(price)),
            score=float(score),
            confidence=confidence,
            side=Side.LONG if side == "LONG" else Side.SHORT,
            klines_1h=klines_1h,
            high24=float(high24),
            low24=float(low24),
            structural_level=(
                Decimal(str(structural_level))
                if structural_level is not None else None
            ),
            spread_pct=float(spread_pct),
            btc_trend=float(btc_trend),
        )
        return await self._engine.submit_signal(intent)

    async def update_positions(self, prices: Mapping[str, float]) -> None:
        """Тик цен -> движок/paper (старый вызов из scan и watcher)."""
        clean = {
            sym: Decimal(str(p)) for sym, p in prices.items()
            if p and p > 0
        }
        if clean:
            self._engine.feed_prices(clean)

    async def refresh_balance(self) -> None:
        """Real: capital <- availableBalance. Paper: no-op (паритет старого)."""
        if self._mode is not Mode.REAL:
            return
        try:
            balance = await self._engine._venue.available_balance()
        except Exception as exc:
            logger.warning("facade: баланс недоступен: %s", exc)
            return
        self._engine.set_capital_base(balance)
        self._capital_base = balance

    async def reconcile(self) -> bool:
        """Совместимость со старым вызовом: лёгкая сверка -> bool."""
        try:
            await self._engine.reconcile_light()
            return True
        except Exception as exc:
            logger.error("facade: reconcile: %s", exc)
            return False

    @staticmethod
    def get_adaptive_threshold(side: str, btc_trend: float) -> float:
        """Точная формула старого статика (scanner зовёт ДО сигнала)."""
        from config import Config  # граница: формула живёт с Config, 1:1

        if side == "SHORT":
            base = float(Config.SCORE_TRADE_THRESHOLD_SHORT)
            if btc_trend < -2.0:
                return base - 3
            if btc_trend > 2.0:
                return base + 3
            return base
        return float(Config.SCORE_TRADE_THRESHOLD)

    # ---------------- API для GUI ----------------

    @property
    def positions(self) -> dict[str, dict[str, Any]]:
        """{symbol: pos} — снимок (GUI: len/keys; watcher: keys)."""
        return self._view.positions

    @property
    def capital(self) -> float:
        """Капитал (снимок)."""
        return self._view.capital

    @property
    def total_pnl(self) -> float:
        """Σ net_pnl (снимок)."""
        return self._view.total_pnl

    @property
    def breakevens(self) -> int:
        """Счётчик BE-закрытий (снимок)."""
        return self._view.breakevens

    def get_open_positions(self) -> list[dict[str, Any]]:
        """Список позиций с ключами GUI (entry_time — epoch-секунды)."""
        return list(self._view.positions.values())

    def get_trades(self, limit: int = 100) -> list[dict[str, Any]]:
        """История сделок (ключи GUI/CSV, ISO-времена)."""
        return self._reader.get_trades(limit)

    def get_win_rate(self) -> tuple[float, int, int, int]:
        """(wr%, total, wins, losses)."""
        return self._reader.get_win_rate()

    def get_max_drawdown(self) -> float:
        """Макс. просадка, %."""
        return self._reader.get_max_drawdown()

    def get_stats(self) -> str:
        """Строка статистики — формат 1:1 со старым get_stats (логи сканера)."""
        wr, total, wins, losses = self.get_win_rate()
        return (
            f"Сделок: {total} | Win: {wins} | Loss: {losses} | "
            f"BE: {self.breakevens} | WR: {wr:.0f}% | "
            f"PnL: ${self.total_pnl:+.2f} | Капитал: ${self.capital:.2f} | "
            f"Открыто: {len(self.positions)}/{self._settings.max_open_positions}"
        )

    # ---------------- снимок для GUI ----------------

    async def _view_loop(self) -> None:
        """Пересбор снимка каждые 2 с (баланс real — раз в balance_refresh_sec)."""
        last_balance = 0.0
        while not self._stop.is_set():
            try:
                await asyncio.sleep(2.0)
                loop = asyncio.get_running_loop()
                now = loop.time()
                if (
                    self._mode is Mode.REAL
                    and now - last_balance >= self._balance_refresh_sec
                ):
                    last_balance = now
                    await self.refresh_balance()
                realized = self._engine.realized_total()
                positions = {
                    p["symbol"]: p for p in self._engine.open_positions()
                }
                self._view = _View(
                    capital=float(self._capital_base) + float(realized),
                    total_pnl=float(realized),
                    breakevens=self._engine.breakevens(),
                    positions=positions,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # снимок не должен ронять фасад
                logger.warning("facade: view refresh: %s", exc)