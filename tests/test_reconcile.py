"""Тесты сверки (§12): dead-close с дозаписью журнала, внешние позиции,
осиротевшие ордера, adopt при рестарте, реконнект -> точечная сверка,
paper-рестарт.

Отличие от test_engine.py: движок не торгует — сценарии рестартов
строятся посевом БД (сигнал/ордера/снапшот) + сценарной «биржей»
FakeReconVenue (контракт ExecutionVenue без логики). Тестируется
Reconciler, а не venue.
"""
import asyncio
import itertools
from decimal import Decimal
from pathlib import Path

from trading.engine import TradingEngine
from trading.filters import FiltersCache
from trading.levels import PercentLevelCalculator
from trading.notifier import LogNotifier
from trading.paper.venue import PaperVenue
from trading.settings import EngineSettings
from trading.storage import OrderRow, Storage, StoredPosition
from trading.types import Fill, Mode, OrderAck, OrderRequest, OrderState
from trading.venue import (
    ExchangePosition,
    ExecutionVenue,
    UnknownOrderError,
    VenueEvent,
    VenueReconnected,
)

START = 1_000_000


class FakeReconVenue(ExecutionVenue):
    """Сценарная «биржа»: всё состояние задаётся тестом напрямую."""

    def __init__(self) -> None:
        self._events: asyncio.Queue[VenueEvent] = asyncio.Queue()
        self.venue_positions: list[ExchangePosition] = []
        self.venue_trades: dict[str, list[Fill]] = {}
        self.venue_open: dict[str, tuple[str, OrderAck]] = {}
        self.cancelled: list[str] = []
        self._ids = itertools.count(5000)

    @property
    def events(self) -> "asyncio.Queue[VenueEvent]":
        return self._events

    def feed_price(self, symbol: str, price: Decimal) -> None:
        del symbol, price

    async def execute_order(self, request: OrderRequest) -> OrderAck:
        ack = OrderAck(
            client_order_id=request.client_order_id,
            exchange_order_id=next(self._ids),
            status=OrderState.NEW, raw={"fake": True},
        )
        self.venue_open[request.client_order_id] = (request.symbol, ack)
        return ack

    async def cancel_order(self, symbol: str, client_order_id: str) -> OrderAck:
        del symbol
        if client_order_id not in self.venue_open:
            raise UnknownOrderError(
                -2011, 400, "уже исполнен/отменён", "/fapi/v1/order"
            )
        self.cancelled.append(client_order_id)
        _, ack = self.venue_open.pop(client_order_id)
        return ack.model_copy(update={"status": OrderState.CANCELED})

    async def query_order(self, symbol: str, client_order_id: str) -> OrderAck | None:
        del symbol
        entry = self.venue_open.get(client_order_id)
        return entry[1] if entry else None

    async def open_orders(self, symbol: str) -> list[OrderAck]:
        return [ack for sym, ack in self.venue_open.values() if sym == symbol]

    async def cancel_all_orders(self, symbol: str) -> int:
        cids = [c for c, (sym, _) in self.venue_open.items() if sym == symbol]
        for c in cids:
            self.cancelled.append(c)
            self.venue_open.pop(c)
        return len(cids)

    async def positions(self) -> list[ExchangePosition]:
        return list(self.venue_positions)

    async def available_balance(self, asset: str = "USDT") -> Decimal:
        del asset
        return Decimal("1000")

    async def user_trades(self, symbol: str, start_ms: int) -> list[Fill]:
        return [
            f for f in self.venue_trades.get(symbol, []) if f.ts_ms >= start_ms
        ]

    async def commission_rate(self, symbol: str) -> tuple[Decimal, Decimal]:
        del symbol
        return Decimal("0"), Decimal("0")


def _fill(ex_id: int, price: str, qty: str, comm: str, ts: int) -> Fill:
    """Fill сценария (REST-стиль: только exchange_order_id)."""
    return Fill(
        exchange_order_id=ex_id, trade_id=ex_id, ts_ms=ts,
        price=Decimal(price), qty=Decimal(qty),
        commission=Decimal(comm), commission_asset="USDT",
    )


def _seed_position(storage: Storage) -> None:
    """Посев «мёртвого процесса»: сигнал, ордера, снапшот позиции."""
    sid = storage.insert_signal(
        START, Mode.PAPER, "RLCUSDT", "LONG", 8.0, "HIGH",
        "accepted", None, None,
    )
    rows = [
        ("wx1-in", 776, "BUY", "MARKET", "ENTRY", Decimal("62.4"), None),
        ("wx1-sl", 777, "SELL", "STOP_MARKET", "SL", None, Decimal("0.3139")),
        ("wx1-tp1", 778, "SELL", "TAKE_PROFIT_MARKET", "TP1",
         Decimal("31.2"), Decimal("0.3238")),
        ("wx1-tp2", 779, "SELL", "TAKE_PROFIT_MARKET", "TP2",
         Decimal("31.2"), Decimal("0.3299")),
    ]
    for cid, ex_id, side, type_, role, qty, stop in rows:
        storage.insert_order(OrderRow(
            ts_ms=START, mode=Mode.PAPER, symbol="RLCUSDT",
            client_order_id=cid, exchange_order_id=ex_id, side=side,
            type=type_, role=role, qty=qty, stop_price=stop,
            reduce_only=role != "ENTRY", close_position=role == "SL",
            status="FILLED" if role == "ENTRY" else "NEW",
            position_ref="wx1-in",
        ))
    storage.upsert_position(StoredPosition(
        symbol="RLCUSDT", side="LONG", entry_ts=START,
        entry_price=Decimal("0.32"), qty=Decimal("62.4"),
        size_usdt=Decimal("20"), score=8.0, sl_price=Decimal("0.3139"),
        signal_id=sid, iron_sl_price=Decimal("0.3129"),
        tp1_price=Decimal("0.3238"), tp2_price=Decimal("0.3299"),
        sl_client_id="wx1-sl", tp1_client_id="wx1-tp1",
        tp2_client_id="wx1-tp2", updated_ms=START,
    ))


async def _make_engine(
    venue: ExecutionVenue, db_path: Path, **settings_kw
) -> TradingEngine:
    """Движок с реальным Storage и FiltersCache, venue — сценарный."""
    storage = Storage(db_path)
    storage.initialize()

    async def fetch(path, params):
        return {"symbols": [{
            "symbol": "RLCUSDT", "status": "TRADING",
            "pricePrecision": 4, "quantityPrecision": 1,
            "filters": [
                {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                {"filterType": "MARKET_LOT_SIZE", "stepSize": "0.1",
                 "minQty": "0.1", "maxQty": "10000"},
                {"filterType": "MIN_NOTIONAL", "notional": "5"},
            ],
        }]}

    filters = FiltersCache(fetch)
    await filters.initialize(attempts=1)
    return TradingEngine(
        venue=venue, storage=storage,
        settings=EngineSettings(**settings_kw),
        filters_cache=filters,
        calculator=PercentLevelCalculator(Decimal("2"), Decimal("3")),
        notifier=LogNotifier(), mode=Mode.PAPER,
        capital_base=Decimal("1000"), now_ms=lambda: START,
        recon_interval_min=99999,
    )


class TestStartupDeadClose:
    """«Была в БД, на бирже нет» — журнал дозаписывается (§12)."""

    async def test_dead_close_by_sl_role_writes_journal(self, tmp_path) -> None:
        venue = FakeReconVenue()
        engine = await _make_engine(venue, tmp_path / "recon.db")
        storage = engine._storage
        _seed_position(storage)
        venue.venue_trades["RLCUSDT"] = [
            _fill(776, "0.32", "62.4", "0.008", START + 1),   # вход
            _fill(777, "0.31", "62.4", "0.0078", START + 2),   # SL
        ]
        await engine.startup_reconcile()

        row = storage._c().execute(
            "SELECT exit_reason, net_pnl, qty, exit_price, pnl_pct FROM trades"
        ).fetchone()
        assert row is not None
        assert row[0] == "SL"
        # gross = (0.31-0.32)*62.4 = -0.624; fees = 0.008+0.0078
        assert Decimal(row[1]) == Decimal("-0.6398")
        assert Decimal(row[2]) == Decimal("62.4")  # entry_qty из fills
        assert Decimal(row[3]) == Decimal("0.31")
        assert abs(row[4] + 3.199) < 0.0001
        assert storage.load_positions() == []
        # дневные лимиты подхватили dead-close (счётчики из trades)
        assert engine._gate_state.today_trades == 1
        assert engine._gate_state.today_realized == Decimal("-0.6398")
        eq = storage._c().execute(
            "SELECT COUNT(*) FROM equity WHERE reason='close'"
        ).fetchone()[0]
        assert eq >= 1
        storage.close()

    async def test_dead_close_by_unknown_order_external_or_price_fallback(self, tmp_path) -> None:
        """После алго-миграции исполнение SL может идти с биржевым id
        (actualOrderId) — роль по exchange_order_id не находится.
        Фолбэк: цена у SL-уровня (±0.5%) классифицируется как SL даже
        при чужом id (решение владельца, вариант (а)); далёкая цена —
        честный EXTERNAL_CLOSE."""
        venue = FakeReconVenue()
        engine = await _make_engine(venue, tmp_path / "recon.db")
        storage = engine._storage
        _seed_position(storage)
        # 1) чужой id, но цена = SL-уровень -> SL (алго-actualOrderId кейс)
        venue.venue_trades["RLCUSDT"] = [
            _fill(776, "0.32", "62.4", "0.008", START + 1),
            _fill(999, "0.315", "62.4", "0.0079", START + 2),
        ]
        await engine.startup_reconcile()
        row = storage._c().execute("SELECT exit_reason FROM trades").fetchone()
        assert row is not None and row[0] == "SL"  # ценовой фолбэк
        storage.close()
        # 2) чужой id И далёкая цена -> EXTERNAL_CLOSE (истинное внешнее)
        engine2 = await _make_engine(FakeReconVenue(), tmp_path / "recon2.db")
        storage2 = engine2._storage
        _seed_position(storage2)
        engine2._reconciler._venue.venue_trades["RLCUSDT"] = [
            _fill(776, "0.32", "62.4", "0.008", START + 1),
            _fill(999, "0.20", "62.4", "0.0079", START + 2),  # далеко от всех уровней
        ]
        await engine2.startup_reconcile()
        row2 = storage2._c().execute("SELECT exit_reason FROM trades").fetchone()
        assert row2 is not None and row2[0] == "EXTERNAL_CLOSE"
        storage2.close()

    async def test_dead_close_no_trades_unknown_reconcile(self, tmp_path) -> None:
        venue = FakeReconVenue()
        engine = await _make_engine(venue, tmp_path / "recon.db")
        storage = engine._storage
        _seed_position(storage)
        # сделок нет вообще — дозапись с нулевым PnL + инцидент
        await engine.startup_reconcile()
        row = storage._c().execute(
            "SELECT exit_reason, net_pnl FROM trades"
        ).fetchone()
        assert row is not None and row[0] == "UNKNOWN_RECONCILE"
        assert Decimal(row[1]) == Decimal("0")
        inc = storage._c().execute(
            "SELECT COUNT(*) FROM incidents WHERE type='recon_mismatch'"
        ).fetchone()[0]
        assert inc >= 1
        storage.close()


class TestStartupExternalAndOrphans:
    """Внешняя позиция — алерт, не трогаем; наш осиротевший ордер — отмена."""

    async def test_external_position_alerted_our_orphan_cancelled(self, tmp_path) -> None:
        venue = FakeReconVenue()
        engine = await _make_engine(venue, tmp_path / "recon.db")
        storage = engine._storage
        venue.venue_positions = [ExchangePosition(
            symbol="ETHUSDT", side="LONG", qty=Decimal("5"),
            entry_price=Decimal("2000"),
        )]
        # наш (wx-) осиротевший ордер рядом с внешней позицией
        venue.venue_open["wx9-tp2"] = ("ETHUSDT", OrderAck(
            client_order_id="wx9-tp2", exchange_order_id=9,
            status=OrderState.NEW, raw={},
        ))
        await engine.startup_reconcile()

        assert engine.local_position("ETHUSDT") is None  # не adopt, не трогаем
        inc = storage._c().execute(
            "SELECT COUNT(*) FROM incidents WHERE type='external_position'"
        ).fetchone()[0]
        assert inc == 1
        assert "wx9-tp2" in venue.cancelled
        storage.close()


class TestStartupAdopt:
    """Совпадение БД/биржа — восстановление из биржи + защита живых TP."""

    async def test_matching_position_adopted_tp_not_orphaned(self, tmp_path) -> None:
        venue = FakeReconVenue()
        engine = await _make_engine(venue, tmp_path / "recon.db")
        storage = engine._storage
        _seed_position(storage)
        venue.venue_positions = [ExchangePosition(
            symbol="RLCUSDT", side="LONG", qty=Decimal("31.2"),  # после TP1
            entry_price=Decimal("0.32"),
        )]
        for cid in ("wx1-sl", "wx1-tp1", "wx1-tp2"):
            venue.venue_open[cid] = ("RLCUSDT", OrderAck(
                client_order_id=cid, exchange_order_id=abs(hash(cid)) % 1000,
                status=OrderState.NEW, raw={},
            ))
        await engine.startup_reconcile()

        pos = engine.local_position("RLCUSDT")
        assert pos is not None
        assert pos.qty == Decimal("31.2")          # биржа приоритетна по цифрам
        assert not pos.unprotected
        assert venue.cancelled == []               # Б-2: TP не «осиротели»
        stored = storage.load_positions()[0]
        assert stored.qty == Decimal("31.2")
        sl_lost = storage._c().execute(
            "SELECT COUNT(*) FROM incidents WHERE type='sl_lost'"
        ).fetchone()[0]
        assert sl_lost == 0                        # SL на бирже жив
        storage.close()


class TestPeriodicAndPointed:
    """Расхождение в рантайме: light_check и реконнект -> точечная сверка."""

    async def _adopted_engine(self, tmp_path) -> tuple:
        venue = FakeReconVenue()
        engine = await _make_engine(venue, tmp_path / "recon.db")
        _seed_position(engine._storage)
        venue.venue_positions = [ExchangePosition(
            symbol="RLCUSDT", side="LONG", qty=Decimal("62.4"),
            entry_price=Decimal("0.32"),
        )]
        venue.venue_open["wx1-sl"] = ("RLCUSDT", OrderAck(
            client_order_id="wx1-sl", exchange_order_id=777,
            status=OrderState.NEW, raw={},
        ))
        await engine.startup_reconcile()
        assert engine.local_position("RLCUSDT") is not None
        # «бот умер»: на бирже позиция закрылась SL, ордера исчезли
        venue.venue_positions = []
        venue.venue_open.clear()
        venue.venue_trades["RLCUSDT"] = [
            _fill(776, "0.32", "62.4", "0.008", START + 1),
            _fill(777, "0.31", "62.4", "0.0078", START + 2),
        ]
        return engine, venue

    async def test_light_check_resolves_dead_close(self, tmp_path) -> None:
        engine, _ = await self._adopted_engine(tmp_path)
        storage = engine._storage
        await engine._reconciler.light_check()
        assert engine.local_position("RLCUSDT") is None
        row = storage._c().execute("SELECT exit_reason FROM trades").fetchone()
        assert row is not None and row[0] == "SL"
        assert storage.load_positions() == []
        assert "RLCUSDT" not in engine._gate_state.open_symbols
        storage.close()

    async def test_reconnect_event_triggers_pointed_reconcile(self, tmp_path) -> None:
        engine, _ = await self._adopted_engine(tmp_path)
        storage = engine._storage
        await engine._dispatch(VenueReconnected(reason="ws reconnect (test)"))
        assert engine.local_position("RLCUSDT") is None
        row = storage._c().execute("SELECT exit_reason FROM trades").fetchone()
        assert row is not None and row[0] == "SL"
        storage.close()


class TestPaperRestart:
    """Paper-рестарт: снапшот невосстановим — удаление + инцидент."""

    async def test_paper_restart_drops_stale_snapshot(self, tmp_path) -> None:
        venue = PaperVenue(Decimal("1000"), lambda s: None)
        engine = await _make_engine(venue, tmp_path / "recon.db")
        storage = engine._storage
        _seed_position(storage)
        await engine.startup_reconcile()
        assert storage.load_positions() == []
        inc = storage._c().execute(
            "SELECT COUNT(*) FROM incidents WHERE type='recon_mismatch'"
        ).fetchone()[0]
        assert inc == 1
        assert engine.ready
        # в trades фейковых записей нет
        trades = storage._c().execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        assert trades == 0
        storage.close()
