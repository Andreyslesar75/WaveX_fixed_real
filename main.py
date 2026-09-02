#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ФАЙЛ: main.py
# СОХРАНИТЬ КАК: main.py

Это точка входа бота. Отсюда всё запускается.

Режимы запуска:
    python main.py             -> GUI (графическое окно), если доступен tkinter
    python main.py --headless  -> без GUI (только логи в консоли)
    python main.py --nogui     -> то же самое, что --headless

ЧТО ИСПРАВЛЕНО:
1. Добавлен простой контроль за фоновыми задачами.
   Раньше если одна из задач (scan / position_watcher / btc_trend)
   падала с ошибкой, бот продолжал работать в "сломанном" состоянии
   и никто об этом не узнавал.
   Теперь если задача падает, бот пишет ошибку в лог и останавливается.

2. База данных закрывается гарантированно и безопасно.
   Закрытие БД теперь происходит ТОЛЬКО внутри бота (scanner.close()),
   а не из GUI. GUI только просит бота остановиться.

3. Добавлены подробные русские комментарии.
"""

import asyncio
import signal
import sys
import threading

from config import Config
from logger import log

# Проверяем, установлен ли tkinter (библиотека для GUI).
# На некоторых серверах его нет, и это нормально — тогда работаем без GUI.
try:
    import tkinter as tk
    GUI_AVAILABLE = True
except ImportError:
    GUI_AVAILABLE = False


# ================================================================
# ФУНКЦИЯ ЗАПУСКА БОТА В ОТДЕЛЬНОМ ПОТОКЕ (для GUI-режима)
# ================================================================

def run_bot_in_thread(gui):
    """
    Запускает event loop asyncio в отдельном потоке.

    GUI работает в своём потоке (tkinter mainloop),
    а бот работает здесь, в отдельном потоке с asyncio.
    Так они не мешают друг другу.
    """

    # Импортируем scanner внутри функции, чтобы избежать
    # проблем с циклическими импортами.
    from scanner import WaveXScanner

    # Создаём новый event loop для этого потока.
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Создаём сканер и привязываем его к GUI,
    # чтобы GUI мог читать данные (позиции, сигналы и т.д.).
    scanner = WaveXScanner()
    gui.scanner = scanner

    async def _main():
        """
        Главная асинхронная функция бота.
        """
        try:
            # Инициализируем сканер:
            # HTTP-сессия, REST-клиент, WebSocket, PositionManager.
            await scanner.init()

            # Создаём три фоновые задачи:
            tasks = [
                # 1. Обновление тренда BTC каждые 30 секунд.
                asyncio.create_task(scanner.update_btc_trend()),

                # 2. Быстрый наблюдатель позиций каждые 2 секунды.
                #    Проверяет SL, TP, trailing.
                asyncio.create_task(scanner.position_watcher()),

                # 3. Основной цикл сканирования рынка.
                asyncio.create_task(scanner.scan()),
            ]

            # Сохраняем задачи в сканер, чтобы он мог их
            # корректно отменить при остановке.
            scanner._tasks.extend(tasks)

            # --------------------------------------------------------
            # [ИСПРАВЛЕНО] Контроль за задачами.
            #
            # Каждые 0.5 секунды проверяем:
            # 1. Не нажата ли кнопка СТОП (scanner._stop_flag).
            # 2. Не упала ли одна из фоновых задач с ошибкой.
            #
            # Раньше если задача падала, бот продолжал работать
            # в неполном состоянии, и это было незаметно.
            # --------------------------------------------------------
            while not scanner._stop_flag[0]:
                await asyncio.sleep(0.5)

                for t in tasks:
                    # Если задача завершилась и не была отменена вручную,
                    # значит она либо упала с ошибкой, либо закончилась сама.
                    if t.done() and not t.cancelled():
                        try:
                            exc = t.exception()
                        except Exception:
                            exc = None

                        if exc is not None:
                            log.error(
                                f"Фоновая задача упала с ошибкой: {exc}. "
                                f"Останавливаю бота."
                            )
                            # Просим бота остановиться.
                            scanner.stop()
                            break

        finally:
            # --------------------------------------------------------
            # [ИСПРАВЛЕНО] Гарантированное закрытие сканера.
            #
            # scanner.close() делает:
            # 1. Останавливает все фоновые задачи.
            # 2. Закрывает HTTP-сессию.
            # 3. Закрывает базу данных.
            #
            # Это происходит в любом случае: и при нормальной
            # остановке, и при ошибке.
            # --------------------------------------------------------
            await scanner.close()

    try:
        # Запускаем асинхронную функцию в event loop.
        loop.run_until_complete(_main())

    except asyncio.CancelledError:
        # Это нормальная ситуация при остановке — не логируем как ошибку.
        pass

    except Exception as e:
        log.exception(f"Ошибка в боте: {e}")

    finally:
        # --------------------------------------------------------
        # Подстраховка: отменяем все оставшиеся задачи и закрываем loop.
        # --------------------------------------------------------
        try:
            pending = asyncio.all_tasks(loop)

            for p in pending:
                p.cancel()

            if pending:
                loop.run_until_complete(
                    asyncio.gather(*pending, return_exceptions=True)
                )
        except Exception:
            pass

        loop.close()

        # --------------------------------------------------------
        # Дополнительная подстраховка закрытия БД.
        #
        # Database.close() теперь безопасен для повторного вызова,
        # поэтому если БД уже была закрыта внутри scanner.close(),
        # второй вызов просто ничего не сделает.
        # --------------------------------------------------------
        try:
            if scanner.pos_manager:
                scanner.pos_manager.close()
        except Exception:
            pass


# ================================================================
# GUI-РЕЖИМ
# ================================================================

def run_gui():
    """
    Запускает бота с графическим интерфейсом.
    """
    from gui import WaveXGUI

    # Создаём главное окно GUI.
    # Сначала scanner=None, потому что бот ещё не запущен.
    root = WaveXGUI(None)

    # Запускаем бота в отдельном потоке.
    # daemon=True значит, что если главное окно закроется,
    # поток бота тоже завершится автоматически.
    bot_thread = threading.Thread(
        target=run_bot_in_thread,
        args=(root,),
        daemon=True,
    )
    bot_thread.start()

    try:
        # Запускаем главный цикл tkinter.
        # Эта строка "держит" окно открытым, пока пользователь его не закроет.
        root.mainloop()

    except KeyboardInterrupt:
        # Пользователь нажал Ctrl+C в терминале.
        pass

    finally:
        # При закрытии окна просим бота остановиться.
        try:
            if root.scanner:
                root.scanner.stop()
        except Exception:
            pass

        # Даём боту 5 секунд на корректное завершение.
        bot_thread.join(timeout=5)

# ================================================================
# HEADLESS-РЕЖИМ (без GUI)
# ================================================================

async def run_headless_async():
    """
    Запуск бота без графического интерфейса.
    Подходит для серверов и для работы в фоне.
    """
    from scanner import WaveXScanner

    scanner = WaveXScanner()

    # --------------------------------------------------------
    # Обработчик сигналов ОС (Ctrl+C, kill).
    # Когда пользователь нажимает Ctrl+C, мы просим бота
    # остановиться корректно, а не убиваем его резко.
    # --------------------------------------------------------
    def _stop_handler(*_):
        scanner.stop()

    try:
        signal.signal(signal.SIGINT, _stop_handler)
        signal.signal(signal.SIGTERM, _stop_handler)
    except Exception:
        # На некоторых системах (например Windows) не все сигналы доступны.
        pass

    try:
        # Инициализируем сканер.
        await scanner.init()

        # Создаём те же три фоновые задачи.
        tasks = [
            asyncio.create_task(scanner.update_btc_trend()),
            asyncio.create_task(scanner.position_watcher()),
            asyncio.create_task(scanner.scan()),
        ]
        scanner._tasks.extend(tasks)

        # Ждём, пока все задачи завершатся.
        # Они завершатся, когда scanner.stop() поднимет флаг остановки.
        await asyncio.gather(*tasks, return_exceptions=True)

    finally:
        # Гарантированное закрытие сканера и БД.
        await scanner.close()


def run_headless():
    """
    Обёртка для запуска headless-режима.
    """
    try:
        asyncio.run(run_headless_async())
    except KeyboardInterrupt:
        pass


# ================================================================
# ТОЧКА ВХОДА
# ================================================================

def main():
    """
    Главная функция. Решает, в каком режиме запускать бота.
    """

    # --------------------------------------------------------
    # Проверка: если включён реальный режим, но ключи не заданы.
    # --------------------------------------------------------
    if Config.REAL_TRADING and (
        not Config.BINANCE_API_KEY or not Config.BINANCE_API_SECRET
    ):
        print(
            "❌ REAL_TRADING=true, но BINANCE_API_KEY/BINANCE_API_SECRET не заданы."
        )
        sys.exit(1)

    # --------------------------------------------------------
    # Если ключи не заданы, предупреждаем, что работаем в бумажном режиме.
    # --------------------------------------------------------
    if not Config.BINANCE_API_KEY or not Config.BINANCE_API_SECRET:
        print("⚠️  API ключи не заданы. Бот работает в бумажном режиме.")
        print(
            "   Для реальной торговли создайте .env с "
            "BINANCE_API_KEY / BINANCE_API_SECRET / REAL_TRADING=true"
        )

    # --------------------------------------------------------
    # Определяем, нужен ли GUI.
    # --------------------------------------------------------
    no_gui = "--nogui" in sys.argv or "--headless" in sys.argv

    if no_gui or not GUI_AVAILABLE:
        if not GUI_AVAILABLE and not no_gui:
            print(
                "tkinter не установлен — запускаю в headless-режиме. "
                "Для GUI установите python3-tk."
            )
        run_headless()
        return

    # Если всё в порядке, запускаем GUI.
    run_gui()


# Это стандартная конструкция Python.
# Она означает: "запусти main(), только если этот файл запустили напрямую".
if __name__ == "__main__":
    main()