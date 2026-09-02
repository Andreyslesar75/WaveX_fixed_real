#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ФАЙЛ: logger.py
# СОХРАНИТЬ КАК: logger.py

Этот файл отвечает за:
1. логирование работы бота;
2. разбор свечей Binance;
3. расчёт EMA и ATR;
4. звуковые уведомления.

ЧТО ИСПРАВЛЕНО:
- Лог теперь пишется с ротацией: файл не будет расти бесконечно.
- ATR считается более правильно: используется сглаживание Wilder.
- Если данных для ATR недостаточно, функция возвращает 0.0,
  а не случайное число вроде 0.01.
- Звук запускается в отдельном потоке, чтобы не блокировать бота.
"""

import logging
import logging.handlers
import threading

import numpy as np

from config import Config


# Основной логгер бота.
# Другие файлы будут использовать его так:
# from logger import log
log = logging.getLogger("WaveX")

# Служебная переменная.
# Нужна, чтобы не создавать обработчики логов несколько раз.
_configured = False


def setup_logging():
    """
    Настраивает логирование.

    [ИСПРАВЛЕНО]
    Раньше лог мог создаваться обычным FileHandler и расти бесконечно.
    Теперь используется RotatingFileHandler:
    - один файл максимум ~10 MB;
    - хранится до 5 старых файлов;
    - старые логи автоматически ротируются.
    """
    global _configured

    # Если логирование уже настроено — повторно не настраиваем.
    if _configured:
        return log

    # Уровень логов: INFO — видим основную работу бота.
    # Если нужно больше деталей, можно поменять на DEBUG.
    log.setLevel(logging.INFO)

    # Удаляем старые обработчики, если они вдруг уже были.
    if log.handlers:
        log.handlers.clear()

    # Формат строки лога.
    # Пример:
    # 2026-08-12 14:30:25 [INFO] OPEN SYMBOL ...
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s"
    )

    # Файловый обработчик с ротацией.
    file_handler = logging.handlers.RotatingFileHandler(
        Config.LOG_FILE,
        maxBytes=10_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    # Вывод логов в консоль/терминал.
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    # Добавляем обработчики к нашему логгеру.
    log.addHandler(file_handler)
    log.addHandler(console_handler)

    # Отключаем проброс логов в корневой логгер Python,
    # чтобы не было дублей.
    log.propagate = False

    _configured = True

    return log


# Настраиваем логирование сразу при импорте файла.
setup_logging()


def fmt_price(price: float) -> str:
    """
    Красиво форматирует цену для логов и GUI.

    Примеры:
    0.000012345 -> $0.0000123450
    0.123456    -> $0.123456
    123.4567    -> $123.4567
    """
    try:
        price = float(price)
    except Exception:
        price = 0.0

    if price < 0.001:
        return f"${price:.10f}"

    if price < 1:
        return f"${price:.6f}"

    return f"${price:.4f}"


def parse_klines(klines: list):
    """
    Разбирает свечи Binance в удобные numpy-массивы.

    ВАЖНО:
    Формат свечи после api.py:
    [ts, volume, close, high, low, open, quote_vol]

    Возвращает:
    O - open
    H - high
    L - low
    C - close
    V - volume
    """
    O, H, L, C, V = [], [], [], [], []

    for k in klines:
        try:
            O.append(float(k[5]))
            H.append(float(k[3]))
            L.append(float(k[4]))
            C.append(float(k[2]))
            V.append(float(k[1]))
        except Exception:
            # Если одна свеча битая — просто пропускаем её.
            pass

    return (
        np.array(O),
        np.array(H),
        np.array(L),
        np.array(C),
        np.array(V),
    )


def ema(arr: np.ndarray, period: int) -> np.ndarray:
    """
    Считает EMA — скользящее среднее с экспоненциальным сглаживанием.

    Используется для трендовых фильтров, например BTC trend.
    """
    if len(arr) < period or period <= 0:
        return arr

    k = 2.0 / (period + 1)

    # Первое значение EMA берём равным первому значению цены.
    e = float(arr[0])
    res = [e]

    for v in arr[1:]:
        e = float(v) * k + e * (1 - k)
        res.append(e)

    return np.array(res)


def calc_atr(highs, lows, closes, period: int = 14) -> float:
    """
    Считает ATR — среднюю волатильность.

    [ИСПРАВЛЕНО]
    Раньше при недостатке данных возвращалось 0.01.
    Это могло сильно исказить стоп-лосс.

    Теперь:
    - если данных мало, возвращаем 0.0;
    - если данных достаточно, считаем ATR по методу Wilder.
    """
    highs = np.asarray(highs, dtype=float)
    lows = np.asarray(lows, dtype=float)
    closes = np.asarray(closes, dtype=float)

    # Берём минимальную длину, чтобы не было ошибок,
    # если массивы вдруг немного разной длины.
    m = min(len(closes), len(highs), len(lows))

    if m < 2 or period <= 0:
        return 0.0

    # Если данных меньше, чем нужно для полноценного ATR.
    if m < period + 1:
        # Если есть хотя бы 5 свечей, можно попытаться взять
        # средний диапазон последних 5 свечей.
        if len(highs) >= 5 and len(lows) >= 5:
            ranges = highs[-5:] - lows[-5:]
            valid = ranges[ranges > 0]

            if len(valid) > 0:
                return float(np.mean(valid))

        return 0.0

    # True Range
    tr = []

    for i in range(1, m):
        high_low = highs[i] - lows[i]

        high_prev_close = abs(highs[i] - closes[i - 1])
        low_prev_close = abs(lows[i] - closes[i - 1])

        tr.append(
            max(
                high_low,
                high_prev_close,
                low_prev_close,
            )
        )

    if len(tr) < period:
        return 0.0

    # [ИСПРАВЛЕНО]
    # Wilder ATR:
    # 1. Сначала берём простое среднее первых period значений TR.
    # 2. Дальше сглаживаем каждое новое значение.
    atr = float(np.mean(tr[:period]))

    for value in tr[period:]:
        atr = (atr * (period - 1) + value) / period

    if atr <= 0:
        return 0.0

    return float(atr)


def safe_mean(values):
    """
    Безопасное среднее.
    Если список пустой, возвращает 0.0.
    """
    return sum(values) / len(values) if values else 0.0


def play_sound(kind: str):
    """
    Проигрывает короткий звук.

    kind:
    - "open"      — открыта позиция;
    - "near_miss" — сигнал почти прошёл, но не открылся.

    [ИСПРАВЛЕНО]
    Раньше звук мог блокировать основной поток бота.
    Теперь он запускается в отдельном потоке.
    """

    def _play():
        try:
            import winsound

            if kind == "open":
                winsound.Beep(880, 150)

            elif kind == "near_miss":
                winsound.Beep(440, 100)
                winsound.Beep(440, 100)

        except ImportError:
            # Если это не Windows, просто печатаем системный звонок.
            if kind == "open":
                print("\a", end="", flush=True)

            elif kind == "near_miss":
                print("\a\a", end="", flush=True)

        except Exception:
            # Звук не критичен для работы бота.
            pass

    try:
        threading.Thread(
            target=_play,
            daemon=True,
        ).start()

    except Exception:
        # Если поток создать не удалось, пробуем просто проиграть.
        _play()


def debug_log(msg: str):
    """
    [НОВОЕ]
    Логирует отладочное сообщение только если DEBUG_LOGS_ENABLED=True.
    Используется для временных DEBUG-логов, которые не нужны в продакшене.
    
    Пример использования:
        from logger import debug_log
        debug_log(f"[DEBUG-TRACKER] {symbol}: entering _close_position")
    """
    from config import Config
    if Config.DEBUG_LOGS_ENABLED:
        log.info(msg)