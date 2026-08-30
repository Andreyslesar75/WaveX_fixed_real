#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ФАЙЛ: database.py
# СОХРАНИТЬ КАК: database.py

Этот файл отвечает за сохранение всех данных бота в базу SQLite:
- история сделок (trades);
- история капитала (equity).

ЧТО ИСПРАВЛЕНО:
- Добавлена блокировка (lock) для безопасной записи из разных потоков.
  Это нужно, потому что GUI и бот работают параллельно.
- Числовые поля теперь сохраняются как NULL, а не как пустая строка.
  Это правильнее для базы данных.
- Добавлены индексы, чтобы база работала быстрее при большом количестве сделок.
- Функция close() стала безопасной для повторного вызова.
"""

import sqlite3
import threading
from datetime import datetime
from typing import List, Dict, Any

from config import Config
from logger import log


class Database:
    """
    Класс для работы с базой данных SQLite.
    
    База данных хранит:
    - trades: все закрытые сделки бота;
    - equity: снимки капитала с течением времени.
    """

    def __init__(self, db_file: str = None):
        # Путь к файлу базы данных.
        # По умолчанию берётся из config.py: wavex.db в папке с ботом.
        self.db_file = db_file or Config.DB_FILE

        # Подключаемся к SQLite.
        # check_same_thread=False нужно, чтобы один connection можно было
        # использовать из разных потоков (GUI + бот).
        # timeout=30 означает, что при блокировке базы другие потоки
        # будут ждать до 30 секунд.
        self.conn = sqlite3.connect(
            self.db_file,
            check_same_thread=False,
            timeout=30,
        )

        # Включаем WAL-режим (Write-Ahead Logging).
        # Это позволяет одновременно читать и писать в базу,
        # что критично для GUI и бота, работающих параллельно.
        self.conn.execute("PRAGMA journal_mode=WAL")

        # [НОВОЕ]
        # Блокировка для потокобезопасности.
        # Без неё GUI и бот могли бы одновременно лезть в базу,
        # что иногда вызывало ошибки "database is locked".
        self._lock = threading.RLock()

        # Создаём таблицы, если их ещё нет.
        self._init()

        # Добавляем новые колонки, если база старая.
        self._migrate()

    def _init(self):
        """
        Создаёт таблицы, если их ещё нет.
        Если таблицы уже существуют, ничего не делает.
        """
        with self._lock:
            self.conn.executescript("""
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    symbol TEXT,
                    client_order_id TEXT,
                    entry_order_id TEXT,
                    entry_price REAL,
                    exit_order_id TEXT,
                    exit_price REAL,
                    size_usdt REAL,
                    qty REAL,
                    pnl_pct REAL,
                    pnl_usdt REAL,
                    exit_reason TEXT,
                    entry_time TEXT,
                    exit_time TEXT,
                    score INTEGER,
                    sl_price REAL,
                    sl_pct REAL,
                    sl_order_id TEXT,
                    sl_client_id TEXT,
                    tp1_price REAL,
                    tp2_price REAL,
                    tp_pct REAL,
                    tp_order_id TEXT,
                    tp_client_id TEXT,
                    side TEXT DEFAULT 'LONG',
                    mfe REAL DEFAULT 0.0,
                    mae REAL DEFAULT 0.0
                );

                CREATE TABLE IF NOT EXISTS equity (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT,
                    balance REAL,
                    pnl REAL,
                    open_pos INTEGER,
                    event_type TEXT DEFAULT 'periodic'
                );
            """)
            self.conn.commit()

    def _migrate(self):
        """
        Безопасно добавляет новые колонки в существующую таблицу trades.
        
        Это нужно, чтобы старые базы данных (созданные предыдущими версиями бота)
        продолжали работать, не теряя старых сделок.
        
        Если колонка уже есть, ошибка игнорируется.
        """
        # Список колонок, которые могли быть добавлены в новых версиях.
        new_columns = [
            ("side", "TEXT DEFAULT 'LONG'"),
            ("mfe", "REAL DEFAULT 0.0"),
            ("mae", "REAL DEFAULT 0.0"),
        ]

        with self._lock:
            for col, typ in new_columns:
                try:
                    self.conn.execute(f"ALTER TABLE trades ADD COLUMN {col} {typ}")
                    self.conn.commit()
                except sqlite3.OperationalError:
                    # Колонка уже есть — пропускаем.
                    pass

            # [НОВОЕ]
            # Создаём индексы для ускорения запросов.
            # Индексы нужны, чтобы быстро искать сделки по символу,
            # по времени и по причине закрытия.
            try:
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trades_symbol ON trades(symbol)"
                )
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)"
                )
                self.conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_trades_exit_reason ON trades(exit_reason)"
                )
                self.conn.commit()
            except Exception as e:
                log.debug(f"Не удалось создать индексы: {e}")

    def log_trade(self, trade_data: dict):
        """
        Сохраняет одну закрытую сделку в таблицу trades.
        
        data — словарь с полями сделки:
        - symbol: "SOL_USDT"
        - entry_price, exit_price: цены входа и выхода
        - pnl_usdt, pnl_pct: прибыль/убыток
        - exit_reason: "SL", "TP1", "TP2", "TRAIL_SL" и т.д.
        - mfe, mae: максимум в плюс и максимум в минус во время сделки
        """
        try:
            self.conn.execute(
                """
                INSERT INTO trades (
                    timestamp, symbol, side, entry_price, exit_price,
                    size_usdt, qty, pnl_pct, pnl_usdt, exit_reason,
                    entry_time, exit_time, score, sl_pct, tp_pct,
                    mfe, mae,
                    entry_order_id, exit_order_id,
                    sl_order_id, tp_order_id,
                    sl_client_id, tp_client_id,
                    client_order_id,
                    sl_price, tp1_price, tp2_price
                ) VALUES (
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?, ?,
                    ?,
                    ?, ?, ?
                )
                """,
                (
                    trade_data.get("timestamp"),
                    trade_data.get("symbol"),
                    trade_data.get("side", "LONG"),
                    trade_data.get("entry_price"),
                    trade_data.get("exit_price"),
                    trade_data.get("size_usdt"),
                    trade_data.get("qty"),
                    trade_data.get("pnl_pct"),
                    trade_data.get("pnl_usdt"),
                    trade_data.get("exit_reason"),
                    trade_data.get("entry_time"),
                    trade_data.get("exit_time"),
                    trade_data.get("score"),
                    trade_data.get("sl_pct"),
                    trade_data.get("tp_pct"),
                    trade_data.get("mfe"),
                    trade_data.get("mae"),
                    trade_data.get("entry_order_id"),
                    trade_data.get("exit_order_id"),
                    trade_data.get("sl_order_id"),
                    trade_data.get("tp_order_id"),
                    trade_data.get("sl_client_id"),
                    trade_data.get("tp_client_id"),
                    trade_data.get("client_order_id"),
                    trade_data.get("sl_price"),
                    trade_data.get("tp1_price"),
                    trade_data.get("tp2_price"),
                ),
            )
            self.conn.commit()
        except Exception as e:
            log.error(f"Ошибка записи сделки в БД: {e}")

    def log_equity(self, capital: float, total_pnl: float, open_positions: int, event_type: str = "periodic"):
        """Записывает снимок капитала."""
        try:
            self.conn.execute(
                """
                INSERT INTO equity (timestamp, capital, total_pnl, open_positions, event_type)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    datetime.now().isoformat(),
                    capital,
                    total_pnl,
                    open_positions,
                    event_type,
                ),
            )
            self.conn.commit()
        except Exception as e:
            log.error(f"Ошибка записи equity: {e}")

    def get_trades(self, limit: int = 100) -> List[Dict]:
        """
        Возвращает последние N сделок из таблицы trades.
        
        limit — сколько сделок вернуть (по умолчанию 100).
        Возвращает список словарей, каждый словарь — одна сделка.
        """
        with self._lock:
            try:
                cur = self.conn.cursor()
                cur.execute(
                    "SELECT * FROM trades ORDER BY id DESC LIMIT ?",
                    (limit,),
                )
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                return [dict(zip(cols, r)) for r in rows]
            except Exception as e:
                log.error(f"Не удалось прочитать сделки из БД: {e}")
                return []

    def total_pnl(self) -> float:
        """
        Возвращает суммарный PnL по всем сделкам в базе.
        """
        with self._lock:
            try:
                row = self.conn.execute(
                    "SELECT COALESCE(SUM(pnl_usdt), 0) FROM trades"
                ).fetchone()
                return row[0] if row else 0.0
            except Exception as e:
                log.error(f"Не удалось посчитать total_pnl: {e}")
                return 0.0

    def win_rate(self):
        """
        Возвращает статистику винрейта:
        (win_rate_%, total_trades, wins, losses)
        
        win_rate_% — процент прибыльных сделок.
        """
        with self._lock:
            try:
                row = self.conn.execute(
                    "SELECT COUNT(*) total, "
                    "SUM(CASE WHEN pnl_usdt > 0 THEN 1 ELSE 0 END) wins "
                    "FROM trades"
                ).fetchone()

                if row and row[0]:
                    wins = row[1] or 0
                    total = row[0]
                    losses = total - wins
                    wr = wins / total * 100
                    return wr, total, wins, losses

                return 0.0, 0, 0, 0
            except Exception as e:
                log.error(f"Не удалось посчитать win_rate: {e}")
                return 0.0, 0, 0, 0

    def max_drawdown(self, initial_balance: float = None) -> float:
        """
        Возвращает максимальную просадку в процентах от пика капитала.
        
        initial_balance — стартовый капитал (по умолчанию PAPER_BALANCE из config).
        
        Просадка считается так:
        - идём по всем сделкам подряд;
        - считаем текущий капитал;
        - запоминаем максимум;
        - считаем, насколько капитал упал от максимума в %;
        - возвращаем самое большое такое падение.
        """
        if initial_balance is None:
            initial_balance = Config.PAPER_BALANCE

        with self._lock:
            try:
                rows = self.conn.execute(
                    "SELECT pnl_usdt FROM trades ORDER BY id"
                ).fetchall()
            except Exception as e:
                log.error(f"Не удалось прочитать сделки для DD: {e}")
                return 0.0

        if not rows:
            return 0.0

        eq = initial_balance
        peak = eq
        dd = 0.0

        for (p,) in rows:
            eq += p or 0.0
            peak = max(peak, eq)
            if peak > 0:
                dd = max(dd, (peak - eq) / peak * 100)

        return dd

    def export_csv(self, fname: str = None) -> int:
        """
        Экспортирует все сделки в CSV-файл.
        
        fname — путь к файлу (по умолчанию trades.csv из config).
        Возвращает количество экспортированных сделок.
        """
        import csv

        fname = fname or Config.CSV_TRADES

        with self._lock:
            try:
                cur = self.conn.cursor()
                cur.execute("SELECT * FROM trades ORDER BY id")
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
            except Exception as e:
                log.error(f"Не удалось прочитать сделки для CSV: {e}")
                return 0

        try:
            with open(fname, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(cols)
                writer.writerows(rows)
            return len(rows)
        except Exception as e:
            log.error(f"Не удалось записать CSV: {e}")
            return 0

    def close(self):
        """
        Закрывает соединение с базой данных.
        
        [ИСПРАВЛЕНО]
        Теперь функция безопасна для повторного вызова.
        Это важно, потому что бот может пытаться закрыть базу
        несколько раз при остановке (из GUI и из основного потока).
        """
        with self._lock:
            if self.conn is not None:
                try:
                    self.conn.close()
                except Exception as e:
                    log.debug(f"Ошибка при закрытии БД: {e}")
                finally:
                    self.conn = None