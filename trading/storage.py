"""SQLite-хранилище v2: WAL, один writer (loop движка) + reader (GUI).

Правила минимальной записи (решение владельца по Д6):
- INSERT только на события (сигнал/ордер/fill/trade/инцидент/equity);
- UPDATE orders — только при смене статуса (сравнение до записи);
- positions — dirty-check по значениям, никаких heartbeat-записей;
- equity — только open/close/ΔE ≥ equity_delta_pct (Б2-3а).

Почему вызовы writer синхронные, без to_thread: WAL +
synchronous=NORMAL + короткие транзакции (<1 мс локально);
to_thread внёс бы гонки порядка записи без внешней очереди задач.
Writer используется только из одного asyncio-цикла движка.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .types import Mode

_DDL = """
CREATE TABLE IF NOT EXISTS signals (
  id INTEGER PRIMARY KEY, ts_ms INTEGER NOT NULL,
  mode TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL,
  score REAL NOT NULL, confidence TEXT NOT NULL, decision TEXT NOT NULL,
  reject_reason TEXT, raw_context TEXT);
CREATE TABLE IF NOT EXISTS orders (
  id INTEGER PRIMARY KEY, ts_ms INTEGER NOT NULL, mode TEXT NOT NULL,
  symbol TEXT NOT NULL, client_order_id TEXT NOT NULL UNIQUE,
  exchange_order_id INTEGER, side TEXT NOT NULL, type TEXT NOT NULL,
  role TEXT NOT NULL, qty TEXT, price TEXT, stop_price TEXT,
  reduce_only INTEGER NOT NULL DEFAULT 0,
  close_position INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL, position_ref TEXT,
  reject_reason TEXT, raw_response TEXT);
CREATE TABLE IF NOT EXISTS fills (
  id INTEGER PRIMARY KEY,
  order_id INTEGER NOT NULL REFERENCES orders(id),
  trade_id TEXT, ts_ms INTEGER NOT NULL, price TEXT NOT NULL,
  qty TEXT NOT NULL, commission TEXT NOT NULL,
  commission_asset TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS positions (
  symbol TEXT PRIMARY KEY, side TEXT NOT NULL, entry_ts INTEGER NOT NULL,
  entry_price TEXT NOT NULL, qty TEXT NOT NULL, size_usdt TEXT NOT NULL,
  score REAL NOT NULL, sl_price TEXT NOT NULL, tp1_price TEXT,
  tp2_price TEXT, iron_sl_price TEXT,
  tp1_done INTEGER NOT NULL DEFAULT 0, tp2_done INTEGER NOT NULL DEFAULT 0,
  breakeven_done INTEGER NOT NULL DEFAULT 0,
  trail_active INTEGER NOT NULL DEFAULT 0,
  unprotected INTEGER NOT NULL DEFAULT 0,
  sl_client_id TEXT, tp1_client_id TEXT, tp2_client_id TEXT,
  signal_id INTEGER NOT NULL, updated_ms INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY, signal_id INTEGER NOT NULL,
  symbol TEXT NOT NULL, side TEXT NOT NULL,
  entry_ts INTEGER NOT NULL, exit_ts INTEGER NOT NULL,
  entry_price TEXT NOT NULL, exit_price TEXT NOT NULL, qty TEXT NOT NULL,
  gross_pnl TEXT NOT NULL, fees TEXT NOT NULL, net_pnl TEXT NOT NULL,
  pnl_pct REAL NOT NULL, exit_reason TEXT NOT NULL,
  mfe REAL, mae REAL, sl_pct REAL, tp_pct REAL,
  tp1_done INTEGER, tp2_done INTEGER, breakeven_done INTEGER);
CREATE TABLE IF NOT EXISTS incidents (
  id INTEGER PRIMARY KEY, ts_ms INTEGER NOT NULL, mode TEXT NOT NULL,
  type TEXT NOT NULL, symbol TEXT, severity TEXT NOT NULL,
  details TEXT, resolved INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS equity (
  id INTEGER PRIMARY KEY, ts_ms INTEGER NOT NULL, mode TEXT NOT NULL,
  capital TEXT NOT NULL, realized_pnl TEXT NOT NULL,
  unrealized_pnl TEXT NOT NULL, total_equity TEXT NOT NULL,
  reason TEXT NOT NULL, symbol TEXT);
CREATE INDEX IF NOT EXISTS idx_trades_ts ON trades(exit_ts);
CREATE INDEX IF NOT EXISTS idx_orders_symbol ON orders(symbol, ts_ms);
CREATE INDEX IF NOT EXISTS idx_orders_exchid ON orders(exchange_order_id);
CREATE INDEX IF NOT EXISTS idx_incidents_open ON incidents(resolved, type);
"""


def _iso(ts_ms: int) -> str:
    """Epoch-мс -> ISO-строка UTC (формат GUI get_trades)."""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def _dec_str(value: Decimal | float | int) -> str:
    """Decimal-подобное -> каноническая строка (одна точка конверсии)."""
    return format(value, "f") if isinstance(value, Decimal) else str(value)


@dataclass(slots=True)
class OrderRow:
    """Данные для insert_order (writer API)."""

    ts_ms: int
    mode: Mode
    symbol: str
    client_order_id: str
    side: str
    type: str
    role: str
    qty: Decimal | None
    stop_price: Decimal | None
    reduce_only: bool
    close_position: bool
    status: str
    position_ref: str | None
    reject_reason: str | None = None
    exchange_order_id: int | None = None
    raw_response: str | None = None


@dataclass(slots=True)
class TradeRecord:
    """Полный журнал закрытой позиции (одна строка trades)."""

    signal_id: int
    symbol: str
    side: str
    entry_ts: int
    exit_ts: int
    entry_price: Decimal
    exit_price: Decimal
    qty: Decimal
    gross_pnl: Decimal
    fees: Decimal
    net_pnl: Decimal
    pnl_pct: float
    exit_reason: str
    mfe: float
    mae: float
    sl_pct: float
    tp_pct: float
    tp1_done: bool
    tp2_done: bool
    breakeven_done: bool


@dataclass(slots=True)
class StoredPosition:
    """Снапшот открытой позиции (writer/reader обмен)."""

    symbol: str
    side: str
    entry_ts: int
    entry_price: Decimal
    qty: Decimal
    size_usdt: Decimal
    score: float
    sl_price: Decimal
    signal_id: int
    iron_sl_price: Decimal | None = None
    tp1_price: Decimal | None = None
    tp2_price: Decimal | None = None
    sl_client_id: str | None = None
    tp1_client_id: str | None = None
    tp2_client_id: str | None = None
    tp1_done: bool = False
    tp2_done: bool = False
    breakeven_done: bool = False
    trail_active: bool = False
    updated_ms: int = 0


class Storage:
    """Writer-хранилище: один экземпляр, только из потока движка.

    Инвариант: initialize() обязан быть вызван до любого другого
    метода; reader() отдаёт независимый объект для GUI-потока.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.Lock()  # защита от случайного кросс-потока

    def initialize(self) -> None:
        """Создать файл/схему; PRAGMA WAL + synchronous=NORMAL."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._path, check_same_thread=True)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.executescript(_DDL)
        self._conn.commit()

    def _c(self) -> sqlite3.Connection:
        """Активное соединение; RuntimeError если не инициализировано."""
        if self._conn is None:
            raise RuntimeError("Storage не инициализирован: вызовите initialize()")
        return self._conn

    def close(self) -> None:
        """Закрыть writer-соединение (idempotent)."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # ---------------- writer: сигналы ----------------

    def insert_signal(
        self, ts_ms: int, mode: Mode, symbol: str, side: str, score: float,
        confidence: str, decision: str,
        reject_reason: str | None, raw_context: Mapping[str, Any] | None,
    ) -> int:
        """INSERT signals; возвращает id (signal_id для clientOrderId)."""
        with self._lock, self._c() as conn:
            cur = conn.execute(
                "INSERT INTO signals (ts_ms, mode, symbol, side, score, confidence,"
                " decision, reject_reason, raw_context) VALUES (?,?,?,?,?,?,?,?,?)",
                (ts_ms, mode.value, symbol, side, score, confidence, decision,
                 reject_reason,
                 json.dumps(raw_context, default=str) if raw_context else None),
            )
        return int(cur.lastrowid or 0)

    # ---------------- writer: ордера/fills ----------------

    def insert_order(self, row: OrderRow) -> int:
        """INSERT orders; id для последующих update/ссылок fills."""
        with self._lock, self._c() as conn:
            cur = conn.execute(
                "INSERT INTO orders (ts_ms, mode, symbol, client_order_id,"
                " exchange_order_id, side, type, role, qty, stop_price,"
                " reduce_only, close_position, status, position_ref,"
                " reject_reason, raw_response) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row.ts_ms, row.mode.value, row.symbol, row.client_order_id,
                 row.exchange_order_id, row.side, row.type, row.role,
                 _dec_str(row.qty) if row.qty is not None else None,
                 _dec_str(row.stop_price) if row.stop_price is not None else None,
                 int(row.reduce_only), int(row.close_position), row.status,
                 row.position_ref, row.reject_reason, row.raw_response),
            )
        return int(cur.lastrowid or 0)

    def update_order_status(
        self, row_id: int, status: str, exchange_order_id: int | None = None,
        raw_response: Mapping[str, Any] | None = None,
    ) -> bool:
        """UPDATE orders при смене статуса.

        Returns:
            True если запись обновлена; False — статус не изменился
            (правило минимальной записи: тихий no-op).
        """
        with self._lock, self._c() as conn:
            cur = conn.execute(
                "SELECT status, exchange_order_id FROM orders WHERE id=?",
                (row_id,),
            ).fetchone()
            if cur is None:
                return False
            old_status, old_exch = cur[0], cur[1]
            new_exch = exchange_order_id if exchange_order_id is not None else old_exch
            if old_status == status and old_exch == new_exch:
                return False
            conn.execute(
                "UPDATE orders SET status=?, exchange_order_id=?, raw_response=?"
                " WHERE id=?",
                (status, new_exch,
                 json.dumps(raw_response, default=str) if raw_response else None,
                 row_id),
            )
        return True

    def insert_fill(
        self, order_row_id: int, trade_id: str | int, ts_ms: int,
        price: Decimal, qty: Decimal, commission: Decimal, asset: str,
    ) -> None:
        """INSERT fills (каждое исполнение — событие)."""
        with self._lock, self._c() as conn:
            conn.execute(
                "INSERT INTO fills (order_id, trade_id, ts_ms, price, qty,"
                " commission, commission_asset) VALUES (?,?,?,?,?,?,?)",
                (order_row_id, str(trade_id), ts_ms, _dec_str(price),
                 _dec_str(qty), _dec_str(commission), asset),
            )

    # ---------------- writer: позиции ----------------

    def upsert_position(self, pos: StoredPosition) -> bool:
        """UPSERT positions c dirty-check (без изменений — no-write)."""
        with self._lock, self._c() as conn:
            cur = conn.execute(
                "SELECT side, entry_ts, entry_price, qty, size_usdt, sl_price,"
                " tp1_price, tp2_price, iron_sl_price, tp1_done, tp2_done,"
                " breakeven_done, trail_active, sl_client_id, tp1_client_id,"
                " tp2_client_id FROM positions WHERE symbol=?",
                (pos.symbol,),
            ).fetchone()
            new = (
                pos.side, pos.entry_ts, _dec_str(pos.entry_price),
                _dec_str(pos.qty), _dec_str(pos.size_usdt), _dec_str(pos.sl_price),
                _dec_str(pos.tp1_price) if pos.tp1_price else None,
                _dec_str(pos.tp2_price) if pos.tp2_price else None,
                _dec_str(pos.iron_sl_price) if pos.iron_sl_price else None,
                int(pos.tp1_done), int(pos.tp2_done), int(pos.breakeven_done),
                int(pos.trail_active), pos.sl_client_id, pos.tp1_client_id,
                pos.tp2_client_id,
            )
            if cur is not None and tuple(cur) == new:
                return False
            conn.execute(
                "INSERT INTO positions (symbol, side, entry_ts, entry_price, qty,"
                " size_usdt, score, sl_price, tp1_price, tp2_price, iron_sl_price,"
                " tp1_done, tp2_done, breakeven_done, trail_active, sl_client_id,"
                " tp1_client_id, tp2_client_id, signal_id, updated_ms)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
                " ON CONFLICT(symbol) DO UPDATE SET side=excluded.side,"
                " entry_ts=excluded.entry_ts, entry_price=excluded.entry_price,"
                " qty=excluded.qty, size_usdt=excluded.size_usdt,"
                " score=excluded.score, sl_price=excluded.sl_price,"
                " tp1_price=excluded.tp1_price, tp2_price=excluded.tp2_price,"
                " iron_sl_price=excluded.iron_sl_price, tp1_done=excluded.tp1_done,"
                " tp2_done=excluded.tp2_done,"
                " breakeven_done=excluded.breakeven_done,"
                " trail_active=excluded.trail_active,"
                " sl_client_id=excluded.sl_client_id,"
                " tp1_client_id=excluded.tp1_client_id,"
                " tp2_client_id=excluded.tp2_client_id,"
                " signal_id=excluded.signal_id, updated_ms=excluded.updated_ms",
                (*new[:2], *_dec_str(pos.entry_price), _dec_str(pos.qty),
                 _dec_str(pos.size_usdt), pos.score, _dec_str(pos.sl_price),
                 _dec_str(pos.tp1_price) if pos.tp1_price else None,
                 _dec_str(pos.tp2_price) if pos.tp2_price else None,
                 _dec_str(pos.iron_sl_price) if pos.iron_sl_price else None,
                 int(pos.tp1_done), int(pos.tp2_done), int(pos.breakeven_done),
                 int(pos.trail_active), pos.sl_client_id, pos.tp1_client_id,
                 pos.tp2_client_id, pos.signal_id, pos.updated_ms),
            )
        return True

    def delete_position(self, symbol: str) -> None:
        """Удалить снапшот позиции (событие закрытия)."""
        with self._lock, self._c() as conn:
            conn.execute("DELETE FROM positions WHERE symbol=?", (symbol,))

    def load_positions(self) -> list[StoredPosition]:
        """Все открытые из БД (стартовая сверка). Reader-допустимо."""
        with self._lock, self._c() as conn:
            rows = conn.execute(
                "SELECT symbol, side, entry_ts, entry_price, qty, size_usdt, score,"
                " sl_price, signal_id, iron_sl_price, tp1_price, tp2_price,"
                " sl_client_id, tp1_client_id, tp2_client_id, tp1_done, tp2_done,"
                " breakeven_done, trail_active, updated_ms FROM positions"
            ).fetchall()
        out: list[StoredPosition] = []
        for r in rows:
            out.append(StoredPosition(
                symbol=r[0], side=r[1], entry_ts=r[2], entry_price=Decimal(r[3]),
                qty=Decimal(r[4]), size_usdt=Decimal(r[5]), score=r[6],
                sl_price=Decimal(r[7]), signal_id=r[8],
                iron_sl_price=Decimal(r[9]) if r[9] else None,
                tp1_price=Decimal(r[10]) if r[10] else None,
                tp2_price=Decimal(r[11]) if r[11] else None,
                sl_client_id=r[12], tp1_client_id=r[13], tp2_client_id=r[14],
                tp1_done=bool(r[15]), tp2_done=bool(r[16]),
                breakeven_done=bool(r[17]), trail_active=bool(r[18]),
                updated_ms=r[19],
            ))
        return out

    # ---------------- writer: trades/инциденты/equity ----------------

    def insert_trade(self, rec: TradeRecord) -> int:
        """INSERT trades (закрытие позиции — одно событие)."""
        with self._lock, self._c() as conn:
            cur = conn.execute(
                "INSERT INTO trades (signal_id, symbol, side, entry_ts, exit_ts,"
                " entry_price, exit_price, qty, gross_pnl, fees, net_pnl, pnl_pct,"
                " exit_reason, mfe, mae, sl_pct, tp_pct, tp1_done, tp2_done,"
                " breakeven_done) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (rec.signal_id, rec.symbol, rec.side, rec.entry_ts, rec.exit_ts,
                 _dec_str(rec.entry_price), _dec_str(rec.exit_price),
                 _dec_str(rec.qty), _dec_str(rec.gross_pnl), _dec_str(rec.fees),
                 _dec_str(rec.net_pnl), rec.pnl_pct, rec.exit_reason,
                 rec.mfe, rec.mae, rec.sl_pct, rec.tp_pct,
                 int(rec.tp1_done), int(rec.tp2_done), int(rec.breakeven_done)),
            )
        return int(cur.lastrowid or 0)

    def insert_incident(
        self, ts_ms: int, mode: Mode, type_: str, symbol: str | None,
        severity: str, details: str,
    ) -> int:
        """INSERT incidents (тип — из IncidentType)."""
        with self._lock, self._c() as conn:
            cur = conn.execute(
                "INSERT INTO incidents (ts_ms, mode, type, symbol, severity, details)"
                " VALUES (?,?,?,?,?,?)",
                (ts_ms, mode.value, type_, symbol, severity, details),
            )
        return int(cur.lastrowid or 0)

    def resolve_incident(self, incident_id: int, details: str) -> None:
        """Отметить инцидент решённым (SL восстановлен и т.п.)."""
        with self._lock, self._c() as conn:
            conn.execute(
                "UPDATE incidents SET resolved=1, details=details || ' | ' || ?"
                " WHERE id=?", (details, incident_id),
            )

    def insert_equity(
        self, ts_ms: int, mode: Mode, capital: Decimal, realized: Decimal,
        unrealized: Decimal, total: Decimal, reason: str, symbol: str | None,
    ) -> None:
        """INSERT equity — только по событию open/close/Δ≥порога."""
        with self._lock, self._c() as conn:
            conn.execute(
                "INSERT INTO equity (ts_ms, mode, capital, realized_pnl,"
                " unrealized_pnl, total_equity, reason, symbol)"
                " VALUES (?,?,?,?,?,?,?,?)",
                (ts_ms, mode.value, _dec_str(capital), _dec_str(realized),
                 _dec_str(unrealized), _dec_str(total), reason, symbol),
            )

    # ---------------- writer: справочники для reconcile/лимитов ----------------

    def find_order_role_by_exchange_id(
        self, mode: Mode, exchange_order_id: int
    ) -> str | None:
        """Роль ордера по биржевому id (маппинг exit_reason в сверке)."""
        with self._lock, self._c() as conn:
            row = conn.execute(
                "SELECT role FROM orders WHERE mode=? AND exchange_order_id=?",
                (mode.value, exchange_order_id),
            ).fetchone()
        return row[0] if row else None

    def today_realized(self, mode: Mode, day_start_ms: int) -> Decimal:
        """Σ net_pnl сделок с начала суток (дневной лимит).

        trades не содержит mode (схема Д6): одна БД на активный режим
        процесса (решение Б4-2), фильтр по mode не нужен. Аргумент mode
        сохранён в сигнатуре — точка будущей миграции, если режимы
        когда-нибудь будут вестись в одной БД параллельно.
        """
        del mode
        with self._lock, self._c() as conn:
            rows = conn.execute(
                "SELECT net_pnl FROM trades WHERE exit_ts>=?", (day_start_ms,),
            ).fetchall()
        return sum((Decimal(r[0]) for r in rows), Decimal("0"))

    def today_trades_count(self, day_start_ms: int) -> int:
        """Число закрытых сделок с начала суток (дневной лимит)."""
        with self._lock, self._c() as conn:
            row = conn.execute(
                "SELECT COUNT(*) FROM trades WHERE exit_ts>=?", (day_start_ms,),
            ).fetchone()
        return int(row[0]) if row else 0

class StorageReader:
    """Читатель для GUI-потока: свежее соединение на каждый вызов.

    Почему per-call: фасад используется из GUI-потока и из потока бота
    (get_stats в цикле сканера) — постоянное соединение нарушало бы
    check_same_thread. Запросы редкие (обновление GUI раз в 2 с),
    стоимость connect+SELECT пренебрежима.
    """

    def __init__(self, path: Path) -> None:
        self._path = path

    def _query(
        self, sql: str, params: tuple[Any, ...] = ()
    ) -> list[tuple[Any, ...]]:
        """SELECT через одноразовое соединение (WAL — неблокирующе для writer)."""
        conn = sqlite3.connect(self._path)
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    def get_trades(self, limit: int = 100) -> list[dict[str, Any]]:
        """Последние сделки в форме ключей старого GUI/CSV.

        Ключи (сверено по gui.py): entry_time/exit_time (ISO), timestamp
        (CSV-экспорт), symbol, side, entry_price, exit_price, pnl_usdt,
        pnl_pct, exit_reason, score.
        """
        rows = self._query(
            "SELECT t.symbol, t.side, t.entry_ts, t.exit_ts, t.entry_price,"
            " t.exit_price, t.net_pnl, t.pnl_pct, t.exit_reason, s.score"
            " FROM trades t JOIN signals s ON s.id = t.signal_id"
            " ORDER BY t.exit_ts DESC LIMIT ?",
            (limit,),
        )
        return [
            {
                "symbol": r[0], "side": r[1],
                "entry_time": _iso(r[2]), "exit_time": _iso(r[3]),
                "timestamp": _iso(r[3]),
                "entry_price": float(r[4]), "exit_price": float(r[5]),
                "pnl_usdt": float(r[6]), "pnl_pct": r[7],
                "exit_reason": r[8], "score": r[9],
            }
            for r in rows
        ]

    def get_win_rate(self) -> tuple[float, int, int, int]:
        """(win_rate%, total, wins, losses) по net_pnl."""
        rows = self._query("SELECT net_pnl FROM trades")
        wins = sum(1 for r in rows if Decimal(r[0]) > 0)
        total = len(rows)
        wr = (wins / total * 100.0) if total else 0.0
        return wr, total, wins, total - wins

    def get_max_drawdown(self) -> float:
        """Макс. просадка кривой total_equity, %."""
        rows = self._query(
            "SELECT total_equity FROM equity ORDER BY ts_ms"
        )
        if not rows:
            return 0.0
        peak = max_dd = 0.0
        peak = float(rows[0][0])
        for r in rows:
            value = float(r[0])
            peak = max(peak, value)
            if peak > 0:
                max_dd = max(max_dd, (peak - value) / peak * 100.0)
        return max_dd

    def get_equity_series(self, limit: int = 1000) -> list[tuple[int, float]]:
        """Кривая эквити (ts_ms, total) для графиков."""
        rows = self._query(
            "SELECT ts_ms, total_equity FROM equity"
            " ORDER BY ts_ms DESC LIMIT ?", (limit,)
        )
        return [(r[0], float(r[1])) for r in reversed(rows)]
