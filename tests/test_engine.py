"""tests/test_engine.py — полный жизненный цикл движка на PaperVenue.

Отличия от версии Части 3:
- задачи движка реально запускаются через engine.run(stop) (раньше
  consumer/monitor/iron не работали — тесты не могли пройти);
- числа пересчитаны под семантику 1:1 (Часть 4 §0): R-множители TP,
  буфер BE 0.15%, лестница трейлинга, iron 0.2%;
- сценарий трейлинга использует tp2_mult=3 (TP2 = +6%), т.к. при
  конфиговой паре «TP2=+2.2% < активация трейла +3%» TP2 на бирже
  всегда срабатывает раньше — трейлинг в боевом коде есть резервный
  путь (паритет со старым ботом, фиксируется в REPORT).
"""
import asyncio
from decimal import Decimal
from pathlib import Path

from trading.engine import EntryIntent, TradingEngine
from trading.filters import FiltersCache
from trading.levels import PercentLevelCalculator
from trading.notifier import LogNotifier
from trading.paper.venue import PaperVenue
from trading.settings import EngineSettings
from trading.storage import Storage
from trading.types import Mode, Side

PRICES: dict[str, Decimal] = {"RLCUSDT": Decimal("0.32")}

EI: dict = {
    "symbol": "RLCUSDT", "price": Decimal("0.32"), "score": 41.0,
    "confidence": "HIGH", "side": Side.LONG, "klines_1h": None,
    "high24": 0.35, "low24": 0.29, "structural_level": None,
    "spread_pct": 0.05, "btc_trend": 0.0,
}


def _reset() -> None:
    """Сброс ценового фида между тестами (мутация общего словаря)."""
    PRICES["RLCUSDT"] = Decimal("0.32")


class FixedTime:
    """Управляемое время движка (advancement — руками теста)."""

    def __init__(self) -> None:
        self.now = 1_000_000

    def __call__(self) -> int:
        return self.now


async def _fetch(path: str, params: dict | None) -> dict:
    """Заглушка exchangeInfo: только RLCUSDT с фильтрами как на реале."""
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


async def _make_engine(
    db_path: Path,
    tp2_mult: Decimal = Decimal("1.1"),
    **settings_kw,
) -> tuple[TradingEngine, PaperVenue, FixedTime]:
    """Движок на PaperVenue + Storage + FiltersCache; сверка выполнена."""
    venue = PaperVenue(
        starting_capital=Decimal("1000"),
        price_provider=lambda s: PRICES.get(s),
    )
    storage = Storage(db_path)
    storage.initialize()
    filters = FiltersCache(_fetch)
    await filters.initialize(attempts=1)
    clock = FixedTime()
    engine = TradingEngine(
        venue=venue, storage=storage,
        settings=EngineSettings(**settings_kw),
        filters_cache=filters,
        calculator=PercentLevelCalculator(Decimal("2"), tp2_mult),
        notifier=LogNotifier(), mode=Mode.PAPER,
        capital_base=Decimal("1000"), now_ms=clock,
        recon_interval_min=99999,
    )
    await engine.startup_reconcile()
    return engine, venue, clock


async def _start(engine: TradingEngine) -> tuple[asyncio.Event, asyncio.Task[None]]:
    """Запустить задачи движка (consumer/iron/monitor/recon)."""
    stop = asyncio.Event()
    task = asyncio.create_task(engine.run(stop))
    await asyncio.sleep(0.02)  # дать задачам стартовать
    return stop, task


async def _teardown(
    stop: asyncio.Event, task: asyncio.Task[None],
    engine: TradingEngine, db_path: Path,
) -> None:
    """Чистая остановка движка + удаление тестовой БД."""
    stop.set()
    await task
    engine._storage.close()
    db_path.unlink(missing_ok=True)


def _feed(engine: TradingEngine, price: Decimal) -> None:
    """Тик цены: обновить фид и прогнать через движок (paper-триггеры)."""
    PRICES["RLCUSDT"] = price
    engine.feed_prices(PRICES)


def _trade(engine: TradingEngine) -> tuple | None:
    """Первая строка trades: (exit_reason, qty, net_pnl) или None."""
    return engine._storage._c().execute(
        "SELECT exit_reason, qty, net_pnl FROM trades"
    ).fetchone()


class TestEntry:
    async def test_entry_places_sl_tp_levels_qty(self, tmp_path) -> None:
        _reset()
        db = tmp_path / "e.db"
        engine, venue, _ = await _make_engine(
            db, monitor_interval_sec=0.01, sl_rest_check_interval_sec=999.0,
        )
        stop, task = await _start(engine)
        try:
            ok, why = await engine.submit_signal(EntryIntent(**EI))
            assert ok, why
            await asyncio.sleep(0.05)  # consumer: entry-событие -> книга
            pos = engine.local_position("RLCUSDT")
            assert pos is not None
            # qty: 20/0.32 = 62.5, floor(step 0.1) = 62.5
            assert pos.qty == Decimal("62.5")
            # уровни 1:1 (PercentCalc: SL -2%, TP1 R=1.0, TP2 R=1.1)
            assert pos.sl_price == Decimal("0.3136")   # floor tick
            assert pos.tp1_price == Decimal("0.3264")  # floor tick
            assert pos.tp2_price == Decimal("0.3270")  # 0.32704 -> floor
            assert pos.iron_sl_price == Decimal("0.3129")  # 0.3136*0.998
            assert pos.entry_price == Decimal("0.32")  # paper fill по live
            # все три защитных ордера стоят на «бирже»
            cids = {a.client_order_id for a in await venue.open_orders("RLCUSDT")}
            assert {"wx1-sl", "wx1-tp1", "wx1-tp2"} <= cids
            # динамическая TP1-доля (Е10): 0.6 -> 37.5 (не 31.2 из Части 3)
            row = engine._storage._c().execute(
                "SELECT qty FROM orders WHERE role='TP1'"
            ).fetchone()
            assert row is not None and row[0] == "37.5"
        finally:
            await _teardown(stop, task, engine, db)


class TestManageFlow:
    async def test_tp1_be_trailing_trail_sl(self, tmp_path) -> None:
        _reset()
        db = tmp_path / "e.db"
        engine, venue, _ = await _make_engine(
            db, tp2_mult=Decimal("3"),  # TP2=+6% — не вытесняет трейлинг
            monitor_interval_sec=0.01, sl_rest_check_interval_sec=999.0,
        )
        stop, task = await _start(engine)
        try:
            ok, _ = await engine.submit_signal(EntryIntent(**EI))
            assert ok
            # --- TP1: +2% (0.3264) ---
            _feed(engine, Decimal("0.3264"))
            await asyncio.sleep(0.06)
            pos = engine.local_position("RLCUSDT")
            assert pos is not None and pos.tp1_done
            assert pos.qty == Decimal("25.0")               # 62.5 - 37.5
            assert pos.breakeven_done
            assert pos.local_sl_price == Decimal("0.32048")  # BE c буфером 0.15%
            # --- активация трейлинга: +3%; лестница (шаг 0, отступ 4%)
            #     0.3296*0.96 = 0.316416 < BE -> SL не двигается,
            #     но флаг активации стоит (Е18, 1:1 старого кода) ---
            _feed(engine, Decimal("0.3296"))
            await asyncio.sleep(0.06)
            pos = engine.local_position("RLCUSDT")
            assert pos is not None and pos.trail_active
            assert pos.local_sl_price == Decimal("0.32048")
            # --- +5.94%: 0.339*0.96 = 0.32544 > BE -> тянем SL ---
            _feed(engine, Decimal("0.3390"))
            await asyncio.sleep(0.06)
            pos = engine.local_position("RLCUSDT")
            assert pos is not None
            assert pos.local_sl_price == Decimal("0.32544")
            # --- касание 0.3250 <= 0.32544 -> TRAIL_SL ---
            _feed(engine, Decimal("0.3250"))
            await asyncio.sleep(0.12)
            assert engine.local_position("RLCUSDT") is None
            row = _trade(engine)
            assert row is not None
            assert row[0] == "TRAIL_SL"
            assert Decimal(row[1]) == Decimal("62.5")  # qty сделки = весь вход
            # после закрытия на «бирже» не осталось наших ордеров
            assert await venue.open_orders("RLCUSDT") == []
        finally:
            await _teardown(stop, task, engine, db)

    async def test_sl_closes_and_sets_cooldown(self, tmp_path) -> None:
        _reset()
        db = tmp_path / "e.db"
        engine, venue, clock = await _make_engine(
            db, monitor_interval_sec=0.01, sl_rest_check_interval_sec=999.0,
        )
        stop, task = await _start(engine)
        try:
            ok, _ = await engine.submit_signal(EntryIntent(**EI))
            assert ok
            _feed(engine, Decimal("0.3136"))  # точно уровень SL
            await asyncio.sleep(0.08)
            assert engine.local_position("RLCUSDT") is None
            row = _trade(engine)
            assert row is not None and row[0] == "SL"
            # Е7: SL-кулдаун 1 ч + история стопов (для repeat-блока)
            until = engine._gate_state.cooldown_until_ms.get("RLCUSDT")
            assert until is not None and until > clock.now
            assert engine._gate_state.stop_history_ms["RLCUSDT"]
        finally:
            await _teardown(stop, task, engine, db)

    async def test_iron_sl_closes_when_exchange_sl_lost(self, tmp_path) -> None:
        _reset()
        db = tmp_path / "e.db"
        engine, venue, _ = await _make_engine(
            db, monitor_interval_sec=0.05, sl_rest_check_interval_sec=999.0,
        )
        stop, task = await _start(engine)
        try:
            ok, _ = await engine.submit_signal(EntryIntent(**EI))
            assert ok
            venue._open.pop("wx1-sl")  # «биржа» потеряла SL (нештатно)
            _feed(engine, Decimal("0.3125"))  # ниже iron (0.3129)
            await asyncio.sleep(0.12)
            assert engine.local_position("RLCUSDT") is None
            row = _trade(engine)
            assert row is not None and row[0] == "IRON_SL"
            inc = engine._storage._c().execute(
                "SELECT COUNT(*) FROM incidents WHERE type='iron_sl'"
            ).fetchone()[0]
            assert inc == 1
        finally:
            await _teardown(stop, task, engine, db)

    async def test_part_b_restores_lost_sl(self, tmp_path) -> None:
        _reset()
        db = tmp_path / "e.db"
        engine, venue, _ = await _make_engine(
            db, monitor_interval_sec=0.01, sl_rest_check_interval_sec=0.0,
        )
        stop, task = await _start(engine)
        try:
            ok, _ = await engine.submit_signal(EntryIntent(**EI))
            assert ok
            venue._open.pop("wx1-sl")  # SL исчез, позиция есть -> Часть B
            await asyncio.sleep(0.15)
            cids = {a.client_order_id for a in await venue.open_orders("RLCUSDT")}
            assert any(c.startswith("wx1-rs") for c in cids)  # восстановлен
            pos = engine.local_position("RLCUSDT")
            assert pos is not None
            assert pos.sl_client_id is not None
            assert pos.sl_client_id.startswith("wx1-rs")
            assert not pos.unprotected
        finally:
            await _teardown(stop, task, engine, db)

    async def test_timeout_closes(self, tmp_path) -> None:
        _reset()
        db = tmp_path / "e.db"
        engine, venue, clock = await _make_engine(
            db, max_hold_sec=1.0, monitor_interval_sec=0.01,
            sl_rest_check_interval_sec=999.0,
        )
        stop, task = await _start(engine)
        try:
            ok, _ = await engine.submit_signal(EntryIntent(**EI))
            assert ok
            clock.now += 1500  # возраст позиции > 1 c (время движка — инъекция)
            await asyncio.sleep(0.12)
            row = _trade(engine)
            assert row is not None and row[0] == "TIMEOUT"
        finally:
            await _teardown(stop, task, engine, db)


class TestRejects:
    async def test_second_signal_max_positions(self, tmp_path) -> None:
        _reset()
        db = tmp_path / "e.db"
        engine, venue, _ = await _make_engine(
            db, monitor_interval_sec=0.01, sl_rest_check_interval_sec=999.0,
        )
        stop, task = await _start(engine)
        try:
            ok1, _ = await engine.submit_signal(EntryIntent(**EI))
            assert ok1
            ok2, why2 = await engine.submit_signal(EntryIntent(**EI))
            assert not ok2
            # порядок гейтов 1:1: max_positions проверяется раньше duplicate
            assert "max_positions" in why2
        finally:
            await _teardown(stop, task, engine, db)

    async def test_low_score_rejected_with_reason(self, tmp_path) -> None:
        _reset()
        db = tmp_path / "e.db"
        engine, venue, _ = await _make_engine(db)
        ok, why = await engine.submit_signal(
            EntryIntent(**{**EI, "score": 10.0})
        )
        assert not ok
        assert "score_threshold" in why
        # причина записана в signals (статистика реджектов, §3 черновика)
        row = engine._storage._c().execute(
            "SELECT decision, reject_reason FROM signals"
        ).fetchone()
        assert row is not None
        assert row[0] == "rejected" and row[1] == "score_threshold"
        engine._storage.close()
        db.unlink(missing_ok=True)