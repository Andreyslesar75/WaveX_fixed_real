#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ФАЙЛ: gui.py
# СОХРАНИТЬ КАК: gui.py

Этот файл отвечает за графический интерфейс (GUI) на базе Tkinter.
Здесь отображаются:
- открытые позиции;
- текущие сигналы;
- история сделок;
- статистика (баланс, PnL, win rate, drawdown).

ЧТО ИСПРАВЛЕНО:
1. Убран опасный вызов закрытия базы данных из GUI.
   Раньше при закрытии окна крестиком GUI вызывал pos_manager.close().
   Это было опасно, потому что GUI и бот работали в разных потоках,
   и закрытие БД из GUI могло сломать SQLite WAL-режим или привести
   к потере данных.
   Теперь GUI только говорит боту остановиться (scanner.stop()),
   а БД закрывается правильно внутри самого бота после остановки всех задач.

2. Статус WebSocket теперь реальный.
   Раньше кружок WS всегда горел зелёным, даже если соединение падало.
   Теперь он берётся из флага ws_client.connected.

3. Добавлены проверки на существование объектов.
   Если бот ещё не успел инициализировать какие-то поля,
   GUI не упадёт с ошибкой AttributeError, а просто подождёт.

4. Добавлены подробные русские комментарии.
"""

import csv
import time
import tkinter as tk
from datetime import datetime
from tkinter import ttk, font as tkfont, messagebox

from config import Config
from logger import log, fmt_price


class WaveXGUI(tk.Tk):
    """
    Главное окно приложения.
    """

    def __init__(self, scanner=None):
        super().__init__()

        # Ссылка на объект сканера, который создаётся в main.py.
        self.scanner = scanner

        # Флаг, что окно сейчас закрывается.
        # Нужен, чтобы остановить фоновые обновления GUI.
        self._closing = False

        # Текст режима в заголовке.
        mode_str = (
            "PAPER MODE (БУМАЖНАЯ ТОРГОВЛЯ)"
            if not Config.REAL_TRADING
            else "⚠ РЕАЛЬНАЯ ТОРГОВЛЯ ⚠"
        )

        self.title(f"WaveX — Гибридный сканер v3.0 (Binance Futures x1) | {mode_str}")
        self.geometry("1450x850")
        self.configure(bg="#0d1117")
        self.resizable(True, True)

        # Цветовая схема (тёмная тема GitHub).
        BG = "#0d1117"   # фон
        FG = "#c9d1d9"   # обычный текст
        HDR = "#161b22"  # заголовки
        ACC = "#58a6ff"  # акцентный синий
        GRN = "#3fb950"  # зелёный (прибыль)
        RED = "#f85149"  # красный (убыток)
        YEL = "#d29922"  # жёлтый (предупреждения)
        SEL = "#1f6feb"  # выделение

        self._c = {
            "BG": BG, "FG": FG, "HDR": HDR, "ACC": ACC,
            "GRN": GRN, "RED": RED, "YEL": YEL, "SEL": SEL,
        }

        # Шрифты.
        fmono = tkfont.Font(family="Consolas", size=9)
        fhdr = tkfont.Font(family="Consolas", size=10, weight="bold")

        # ================================================================
        # ШАПКА
        # ================================================================

        hf = tk.Frame(self, bg=HDR, pady=6)
        hf.pack(fill=tk.X)

        mode_color = YEL if not Config.REAL_TRADING else RED

        tk.Label(
            hf,
            text=f"⚡ WAVEX v3.0 — Binance Futures x1 | {mode_str}",
            bg=HDR,
            fg=mode_color,
            font=fhdr,
        ).pack(side=tk.LEFT, padx=12)

        self.lbl_btc = tk.Label(hf, text="BTC: —", bg=HDR, fg=FG, font=fmono)
        self.lbl_cycle = tk.Label(hf, text="Цикл: 0", bg=HDR, fg=FG, font=fmono)
        self.lbl_time = tk.Label(hf, text="", bg=HDR, fg=FG, font=fmono)
        self.lbl_ws = tk.Label(hf, text="WS: —", bg=HDR, fg=YEL, font=fmono)

        for w in (self.lbl_time, self.lbl_ws, self.lbl_cycle, self.lbl_btc):
            w.pack(side=tk.RIGHT, padx=10)

        # ================================================================
        # СТАТУС-СТРОКА
        # ================================================================

        sf = tk.Frame(self, bg=BG, pady=4)
        sf.pack(fill=tk.X, padx=8)

        self.lbl_bal = tk.Label(sf, text="💰 1000.00$", bg=BG, fg=FG, font=fmono)
        self.lbl_pnl = tk.Label(sf, text="📈 +0.00$", bg=BG, fg=GRN, font=fmono)
        self.lbl_wr = tk.Label(sf, text="🎯 WR: 0%", bg=BG, fg=FG, font=fmono)
        self.lbl_dd = tk.Label(sf, text="📉 DD: 0%", bg=BG, fg=YEL, font=fmono)
        self.lbl_open = tk.Label(
            sf,
            text=f"🔓 0/{Config.MAX_OPEN_POSITIONS}",
            bg=BG,
            fg=FG,
            font=fmono,
        )

        for w in (
            self.lbl_bal,
            self.lbl_pnl,
            self.lbl_wr,
            self.lbl_dd,
            self.lbl_open,
        ):
            w.pack(side=tk.LEFT, padx=14)

        # ================================================================
        # ВКЛАДКИ
        # ================================================================

        style = ttk.Style(self)
        style.theme_use("clam")
        style.configure("TNotebook", background=BG, borderwidth=0)
        style.configure(
            "TNotebook.Tab",
            background=HDR,
            foreground=FG,
            padding=[10, 4],
            font=fmono,
        )
        style.map(
            "TNotebook.Tab",
            background=[("selected", SEL)],
            foreground=[("selected", "#ffffff")],
        )

        nb = ttk.Notebook(self)
        nb.pack(fill=tk.BOTH, expand=True, padx=4, pady=2)

        tab_pos = ttk.Frame(nb)
        nb.add(tab_pos, text="  📊 Позиции  ")

        tab_sig = ttk.Frame(nb)
        nb.add(tab_sig, text="  🔔 Сигналы  ")

        tab_hist = ttk.Frame(nb)
        nb.add(tab_hist, text="  📋 История  ")

        self.tree_pos = self._tree(
            tab_pos, fmono, BG, FG, HDR, SEL,
            (
                "Символ", "Сторона", "Время входа", "Вход", "SL",
                "TP1", "TP2", "Размер", "Score", "Мин", "Статус",
            )
        )

        self.tree_sig = self._tree(
            tab_sig, fmono, BG, FG, HDR, SEL,
            (
                "Символ", "Сторона", "Цена", "Score", "Conf",
                "Отклонён", "Причина отказа", "Причины", "Штрафы",
            )
        )

        self.tree_hist = self._tree(
            tab_hist, fmono, BG, FG, HDR, SEL,
            (
                "Время входа", "Время выхода", "Символ", "Сторона",
                "Вход", "Выход", "PnL$", "PnL%", "Причина", "Score",
            )
        )

        for t in (self.tree_pos, self.tree_sig, self.tree_hist):
            t.tag_configure("green", foreground=GRN)
            t.tag_configure("red", foreground=RED)
            t.tag_configure("yel", foreground=YEL)
            t.tag_configure("acc", foreground=ACC)

        # ================================================================
        # КНОПКИ
        # ================================================================

        bf = tk.Frame(self, bg=BG, pady=4)
        bf.pack(fill=tk.X, padx=8)

        bs = dict(
            bg=HDR,
            fg=ACC,
            font=fmono,
            relief=tk.FLAT,
            padx=10,
            pady=3,
            cursor="hand2",
            activebackground=SEL,
            activeforeground="#fff",
        )

        self.btn_stop = tk.Button(bf, text="⏹ СТОП", **bs)
        self.btn_export = tk.Button(bf, text="💾 Экспорт CSV", **bs)

        self.btn_stop.pack(side=tk.LEFT, padx=4)
        self.btn_export.pack(side=tk.LEFT, padx=4)

        self.lbl_status = tk.Label(bf, text="● Запуск...", bg=BG, fg=YEL, font=fmono)
        self.lbl_status.pack(side=tk.RIGHT, padx=12)

        self.btn_stop.configure(command=self._on_stop)
        self.btn_export.configure(command=self._on_export)

        # [ИСПРАВЛЕНО]
        # Обработчик закрытия окна крестиком.
        self.protocol("WM_DELETE_WINDOW", self._on_window_close)

        # Запускаем тиканье часов.
        self._tick()

        # Запускаем обновление GUI через 500 мс.
        self.after(500, self.update_gui)

    # ================================================================
    # СОЗДАНИЕ ТАБЛИЦ (TREEVIEW)
    # ================================================================

    def _tree(self, parent, font, bg, fg, hdr_bg, sel_bg, cols):
        """
        Создаёт таблицу (Treeview) для вкладок.
        """
        fr = tk.Frame(parent, bg=bg)
        fr.pack(fill=tk.BOTH, expand=True)

        uid = f"T{id(fr)}.Treeview"

        s = ttk.Style()
        s.configure(
            uid,
            background=bg,
            foreground=fg,
            fieldbackground=bg,
            rowheight=22,
            font=font,
            borderwidth=0,
        )
        s.configure(
            f"{uid}.Heading",
            background=hdr_bg,
            foreground=fg,
            font=font,
            relief=tk.FLAT,
        )
        s.map(uid, background=[("selected", sel_bg)])

        vsb = ttk.Scrollbar(fr, orient=tk.VERTICAL)
        hsb = ttk.Scrollbar(fr, orient=tk.HORIZONTAL)

        tv = ttk.Treeview(
            fr,
            columns=cols,
            show="headings",
            style=uid,
            yscrollcommand=vsb.set,
            xscrollcommand=hsb.set,
        )

        vsb.configure(command=tv.yview)
        hsb.configure(command=tv.xview)

        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        hsb.pack(side=tk.BOTTOM, fill=tk.X)
        tv.pack(fill=tk.BOTH, expand=True)

        w = max(90, 1400 // len(cols))

        for col in cols:
            tv.heading(col, text=col)
            tv.column(col, width=w, minwidth=60, anchor=tk.CENTER)

        return tv

    # ================================================================
    # ТИКАНЬЕ ЧАСОВ
    # ================================================================

    def _tick(self):
        """
        Обновляет время в шапке каждую секунду.
        """
        if self._closing:
            return

        self.lbl_time.configure(text=datetime.now().strftime("%H:%M:%S"))
        self.after(1000, self._tick)

    # ================================================================
    # ОБРАБОТЧИКИ КНОПОК
    # ================================================================

    def _on_stop(self):
        """
        Обработчик кнопки "СТОП".
        """
        if self.scanner:
            self.scanner.stop()

        self.lbl_status.configure(text="● Остановлен", fg=self._c["RED"])

    def _on_export(self):
        """
        Обработчик кнопки "Экспорт CSV".
        """
        pm = getattr(self.scanner, "pos_manager", None)

        if not pm:
            return

        count = 0

        try:
            with open(
                Config.CSV_TRADES,
                "w",
                newline="",
                encoding="utf-8",
            ) as f:

                writer = csv.writer(f)

                writer.writerow([
                    "timestamp", "symbol", "side", "entry_price",
                    "exit_price", "pnl_usdt", "pnl_pct", "exit_reason",
                ])

                for t in pm.get_trades(1000):
                    writer.writerow([
                        t.get("timestamp"),
                        t.get("symbol"),
                        t.get("side", "LONG"),
                        t.get("entry_price"),
                        t.get("exit_price"),
                        t.get("pnl_usdt"),
                        t.get("pnl_pct"),
                        t.get("exit_reason"),
                    ])
                    count += 1

            self.lbl_status.configure(
                text=f"● CSV: {count} сделок",
                fg=self._c["YEL"],
            )
            log.info(f"Экспорт CSV: {count} сделок")

        except Exception as e:
            log.error(f"Ошибка экспорта CSV: {e}")

    def _on_window_close(self):
        """
        [ИСПРАВЛЕНО]
        Обработчик закрытия окна крестиком.

        Раньше здесь было:
            self.scanner.pos_manager.close()

        Это было опасно, потому что GUI работает в своём потоке,
        а бот — в другом (asyncio).
        Закрытие SQLite из GUI-потока, пока бот пишет туда сделки,
        могло приводить к ошибкам "database is locked" или потере
        незакоммиченных данных в WAL-файле.

        Теперь GUI только просит бота остановиться:
            self.scanner.stop()

        А сам бот в main.py корректно закроет БД после завершения
        всех своих задач.
        """
        self._closing = True

        try:
            if self.scanner:
                self.scanner.stop()
        except Exception as e:
            log.debug(f"Ошибка при остановке сканера из GUI: {e}")
        finally:
            self.destroy()

    # ================================================================
    # ОБНОВЛЕНИЕ GUI
    # ================================================================

    def update_gui(self):
        """
        Периодически обновляет все надписи и таблицы.
        Вызывается каждые 2 секунды.
        """
        if self._closing:
            return

        try:
            pm = getattr(self.scanner, "pos_manager", None)

            if not pm:
                self.after(1000, self.update_gui)
                return

            # ------------------------------------------------------------
            # Верхняя статистика
            # ------------------------------------------------------------

            bal = getattr(pm, "capital", 0.0) or 0.0
            pnl = getattr(pm, "total_pnl", 0.0) or 0.0

            self.lbl_bal.configure(text=f"💰 {bal:.2f}$")

            self.lbl_pnl.configure(
                text=f"📈 {pnl:+.2f}$",
                fg=self._c["GRN"] if pnl >= 0 else self._c["RED"],
            )

            wr, tot, wins, losses = pm.get_win_rate()
            bevs = getattr(pm, "breakevens", 0) or 0

            self.lbl_wr.configure(
                text=f"🎯 WR:{wr:.0f}% W:{wins} L:{losses} BE:{bevs}"
            )

            dd = pm.get_max_drawdown()

            self.lbl_dd.configure(
                text=f"📉 DD:{dd:.1f}%",
                fg=self._c["RED"] if dd > 5 else self._c["YEL"],
            )

            pos_count = len(getattr(pm, "positions", {}))
            self.lbl_open.configure(
                text=f"🔓 {pos_count}/{Config.MAX_OPEN_POSITIONS}"
            )

            self.lbl_status.configure(text="● Работает", fg=self._c["GRN"])

            btc = getattr(self.scanner, "btc_trend", 0.0) or 0.0

            self.lbl_btc.configure(
                text=f"BTC: {btc:+.2f}%",
                fg=self._c["GRN"] if btc >= 0 else self._c["RED"],
            )

            cycle = getattr(self.scanner, "cycle", 0) or 0
            self.lbl_cycle.configure(text=f"Цикл: {cycle}")

            # [ИСПРАВЛЕНО]
            # Реальный статус WebSocket.
            ws_client = getattr(self.scanner, "ws_client", None)
            ws_connected = getattr(ws_client, "connected", False) if ws_client else False

            self.lbl_ws.configure(
                text="WS: ●" if ws_connected else "WS: ○",
                fg=self._c["GRN"] if ws_connected else self._c["RED"],
            )

            # ------------------------------------------------------------
            # Таблица позиций
            # ------------------------------------------------------------

            for r in self.tree_pos.get_children():
                self.tree_pos.delete(r)

            for p in pm.get_open_positions():
                mins = round((time.time() - p["entry_time"]) / 60, 1)
                entry_time_str = datetime.fromtimestamp(
                    p["entry_time"]
                ).strftime("%H:%M:%S")

                status = "OPEN"
                tag = "yel"

                if p.get("tp2_done"):
                    status = "TP2✓"
                    tag = "acc"
                elif p.get("tp1_done"):
                    status = "TP1✓"
                    tag = "acc"

                self.tree_pos.insert(
                    "",
                    tk.END,
                    values=(
                        p["symbol"],
                        p.get("side", "LONG"),
                        entry_time_str,
                        fmt_price(p["entry_price"]),
                        fmt_price(p["sl_price"]),
                        fmt_price(p["tp1_price"]),
                        fmt_price(p["tp2_price"]),
                        f"{p['size_usdt']:.1f}$",
                        p["score"],
                        f"{mins}м",
                        status,
                    ),
                    tags=(tag,),
                )

            # ------------------------------------------------------------
            # Таблица сигналов
            # ------------------------------------------------------------

            for r in self.tree_sig.get_children():
                self.tree_sig.delete(r)

            signals = getattr(self.scanner, "signals_history", []) or []

            for sig in signals[:40]:
                rejected = sig.get("rejected", False)

                self.tree_sig.insert(
                    "",
                    tk.END,
                    values=(
                        sig["symbol"],
                        sig.get("side", "LONG"),
                        fmt_price(sig["price"]),
                        f"{sig['score']:.0f}",
                        sig["confidence"],
                        "Да" if rejected else "Нет",
                        sig.get("reject_reason", ""),
                        "; ".join(sig.get("reasons", [])[:3]),
                        "; ".join(sig.get("penalties", [])[:2]),
                    ),
                    tags=("red" if rejected else "green",),
                )

            # ------------------------------------------------------------
            # Таблица истории
            # ------------------------------------------------------------

            for r in self.tree_hist.get_children():
                self.tree_hist.delete(r)

            for t in pm.get_trades(50):
                pnl_t = t.get("pnl_usdt", 0) or 0

                self.tree_hist.insert(
                    "",
                    tk.END,
                    values=(
                        str(t.get("entry_time", ""))[:19].replace("T", " "),
                        str(t.get("exit_time", ""))[:19].replace("T", " "),
                        t.get("symbol", ""),
                        t.get("side", "LONG"),
                        fmt_price(t.get("entry_price", 0)),
                        fmt_price(t.get("exit_price", 0)),
                        f"{pnl_t:+.2f}$",
                        f"{t.get('pnl_pct', 0):+.2f}%",
                        t.get("exit_reason", ""),
                        t.get("score", 0),
                    ),
                    tags=("green" if pnl_t >= 0 else "red",),
                )

        except Exception as e:
            # GUI не должен падать из-за случайной ошибки чтения данных.
            log.debug(f"GUI update error: {e}")

        finally:
            if not self._closing:
                self.after(2000, self.update_gui)