#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ФАЙЛ: calculations.py
# СОХРАНИТЬ КАК: calculations.py

Этот файл считает:
1. Stop Loss и Take Profit для LONG.
2. Stop Loss и Take Profit для SHORT.
3. Размер позиции по риску.

ЧТО ИСПРАВЛЕНО:
- calculate_position_size() больше НЕ увеличивает размер сделки принудительно до 5 USDT.
  Раньше было: min(max_size, max(5.0, size)).
  Это могло нарушать риск, особенно на маленьком капитале.
- Теперь, если расчётный размер слишком маленький, функция возвращает реальный размер,
  а проверка "можно ли открывать такую сделку" делается отдельно.
- Добавлена функция size_is_valid(), чтобы risk_manager мог проверять
  минимальный размер сделки отдельно от расчёта риска.
"""

from typing import List, Optional, Tuple

from config import Config
from logger import calc_atr


def calc_sl_tp(
    entry_price: float,
    klines_1h: List,
    structural_low: Optional[float] = None,
    spread_pct: float = 0.0,
    high24: Optional[float] = None,
    low24: Optional[float] = None,
) -> Tuple[float, float, float, float, float, float, str]:
    """
    Рассчитывает SL и TP для LONG-позиции.

    Возвращает:
    sl_price, sl_pct, tp1_price, tp1_pct, tp2_price, tp2_pct, sl_source

    Логика выбора SL:
    1. Сначала пробуем ATR-стоп.
    2. Если есть структурный уровень (например impulse_low), пробуем его.
    3. Выбираем более близкий к цене входной уровень.
    4. Если нормальных уровней нет, используем запасной вариант по 24h диапазону.
    """

    # Защита от некорректной цены входа.
    if entry_price <= 0:
        return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "invalid_entry_price"

    # ------------------------------------------------------------
    # 1. ATR-стоп
    # ------------------------------------------------------------

    atr_sl = None

    if klines_1h and len(klines_1h) >= Config.ATR_PERIOD + 1:
        try:
            highs = [float(k[3]) for k in klines_1h]
            lows = [float(k[4]) for k in klines_1h]
            closes = [float(k[2]) for k in klines_1h]

            atr = calc_atr(highs, lows, closes, Config.ATR_PERIOD)

            if atr > 0:
                # Для LONG стоп находится ниже входа:
                # entry - ATR * множитель
                atr_sl = entry_price - atr * Config.ATR_MULTIPLIER

        except Exception:
            atr_sl = None

    # ------------------------------------------------------------
    # 2. Структурный стоп
    # ------------------------------------------------------------

    struct_sl = None

    if structural_low and structural_low > 0:
        # Берём структурный уровень и добавляем небольшой буфер ниже.
        struct_sl = structural_low * (1 - Config.SWING_LOW_BUFFER_PCT / 100)

    # ------------------------------------------------------------
    # 3. Выбор лучшего кандидата
    # ------------------------------------------------------------

    # Для LONG подходят только стопы ниже цены входа.
    valid_candidates = [
        s
        for s in (atr_sl, struct_sl)
        if s is not None and 0 < s < entry_price
    ]

    if valid_candidates:
        # Для LONG выбираем максимальный стоп,
        # то есть более близкий к цене входа.
        sl_price = max(valid_candidates)

        if (
            atr_sl is not None
            and struct_sl is not None
            and abs(atr_sl - struct_sl) < 1e-9
        ):
            sl_source = "ATR+Structural"

        elif sl_price == atr_sl:
            sl_source = "ATR"

        else:
            sl_source = "Structural(impulse_low)"

    # ------------------------------------------------------------
    # 4. Запасной вариант по 24h диапазону
    # ------------------------------------------------------------

    elif high24 and low24 and high24 > low24 > 0:
        range_pct = (high24 - low24) / low24 * 100

        sl_pct_fb = range_pct * Config.VOL24_SL_MULTIPLIER

        # Ограничиваем fallback-стоп минимальным и максимальным значением.
        sl_pct_fb = max(
            Config.ATR_SL_MIN_PCT,
            min(Config.ATR_SL_MAX_PCT, sl_pct_fb),
        )

        sl_price = entry_price * (1 - sl_pct_fb / 100)
        sl_source = f"Vol24h({range_pct:.1f}%)"

    # ------------------------------------------------------------
    # 5. Последний запасной вариант
    # ------------------------------------------------------------

    else:
        sl_price = entry_price * (1 - Config.ATR_SL_MIN_PCT / 100)
        sl_source = "fallback"

    # ------------------------------------------------------------
    # 6. Пересчёт процента стопа
    # ------------------------------------------------------------

    sl_pct = (entry_price - sl_price) / entry_price * 100

    # Если спред большой, добавляем небольшой буфер к стопу.
    if spread_pct and spread_pct > Config.SPREAD_SL_BUFFER_TRIGGER_PCT:
        sl_pct += Config.SPREAD_SL_BUFFER_PCT
        sl_source += "+spread_buf"

    # Ограничиваем стоп допустимым диапазоном.
    sl_pct = max(
        Config.ATR_SL_MIN_PCT,
        min(Config.ATR_SL_MAX_PCT, sl_pct),
    )

    # Пересчитываем цену стопа после ограничений.
    sl_price = entry_price * (1 - sl_pct / 100)

    # ------------------------------------------------------------
    # 7. Расчёт TP1 и TP2
    # ------------------------------------------------------------

    tp1_pct = sl_pct * Config.FIRST_TP_MULTIPLIER
    tp2_pct = sl_pct * Config.SECOND_TP_MULTIPLIER

    tp1_price = entry_price * (1 + tp1_pct / 100)
    tp2_price = entry_price * (1 + tp2_pct / 100)

    return (
        sl_price,
        sl_pct,
        tp1_price,
        tp1_pct,
        tp2_price,
        tp2_pct,
        sl_source,
    )


def calc_sl_tp_short(
    entry_price: float,
    klines_1h: List,
    structural_high: Optional[float] = None,
    spread_pct: float = 0.0,
    high24: Optional[float] = None,
    low24: Optional[float] = None,
) -> Tuple[float, float, float, float, float, float, str]:
    """
    Рассчитывает SL и TP для SHORT-позиции.

    Возвращает:
    sl_price, sl_pct, tp1_price, tp1_pct, tp2_price, tp2_pct, sl_source

    Логика зеркальна LONG, но:
    - стоп находится выше входа;
    - тейки находятся ниже входа.
    """

    # Защита от некорректной цены входа.
    if entry_price <= 0:
        return 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, "invalid_entry_price"

    # ------------------------------------------------------------
    # 1. ATR-стоп
    # ------------------------------------------------------------

    atr_sl = None

    if klines_1h and len(klines_1h) >= Config.ATR_PERIOD + 1:
        try:
            highs = [float(k[3]) for k in klines_1h]
            lows = [float(k[4]) for k in klines_1h]
            closes = [float(k[2]) for k in klines_1h]

            atr = calc_atr(highs, lows, closes, Config.ATR_PERIOD)

            if atr > 0:
                # Для SHORT стоп находится выше входа:
                # entry + ATR * множитель
                atr_sl = entry_price + atr * Config.ATR_MULTIPLIER

        except Exception:
            atr_sl = None

    # ------------------------------------------------------------
    # 2. Структурный стоп
    # ------------------------------------------------------------

    struct_sl = None

    if structural_high and structural_high > 0:
        # Берём структурный уровень и добавляем небольшой буфер выше.
        struct_sl = structural_high * (1 + Config.SWING_LOW_BUFFER_PCT / 100)

    # ------------------------------------------------------------
    # 3. Выбор лучшего кандидата
    # ------------------------------------------------------------

    # Для SHORT подходят только стопы выше цены входа.
    valid_candidates = [
        s
        for s in (atr_sl, struct_sl)
        if s is not None and s > entry_price
    ]

    if valid_candidates:
        # Для SHORT выбираем минимальный стоп,
        # то есть более близкий к цене входа.
        sl_price = min(valid_candidates)

        if (
            atr_sl is not None
            and struct_sl is not None
            and abs(atr_sl - struct_sl) < 1e-9
        ):
            sl_source = "ATR+Structural"

        elif sl_price == atr_sl:
            sl_source = "ATR"

        else:
            sl_source = "Structural(impulse_high)"

    # ------------------------------------------------------------
    # 4. Запасной вариант по 24h диапазону
    # ------------------------------------------------------------

    elif high24 and low24 and high24 > low24 > 0:
        range_pct = (high24 - low24) / low24 * 100

        sl_pct_fb = range_pct * Config.VOL24_SL_MULTIPLIER

        # Ограничиваем fallback-стоп минимальным и максимальным значением.
        sl_pct_fb = max(
            Config.ATR_SL_MIN_PCT,
            min(Config.ATR_SL_MAX_PCT, sl_pct_fb),
        )

        sl_price = entry_price * (1 + sl_pct_fb / 100)
        sl_source = f"Vol24h({range_pct:.1f}%)"

    # ------------------------------------------------------------
    # 5. Последний запасной вариант
    # ------------------------------------------------------------

    else:
        sl_price = entry_price * (1 + Config.ATR_SL_MIN_PCT / 100)
        sl_source = "fallback"

    # ------------------------------------------------------------
    # 6. Пересчёт процента стопа
    # ------------------------------------------------------------

    sl_pct = (sl_price - entry_price) / entry_price * 100

    # Если спред большой, добавляем небольшой буфер к стопу.
    if spread_pct and spread_pct > Config.SPREAD_SL_BUFFER_TRIGGER_PCT:
        sl_pct += Config.SPREAD_SL_BUFFER_PCT
        sl_source += "+spread_buf"

    # Ограничиваем стоп допустимым диапазоном.
    sl_pct = max(
        Config.ATR_SL_MIN_PCT,
        min(Config.ATR_SL_MAX_PCT, sl_pct),
    )

    # Пересчитываем цену стопа после ограничений.
    sl_price = entry_price * (1 + sl_pct / 100)

    # ------------------------------------------------------------
    # 7. Расчёт TP1 и TP2
    # ------------------------------------------------------------

    tp1_pct = sl_pct * Config.FIRST_TP_MULTIPLIER
    tp2_pct = sl_pct * Config.SECOND_TP_MULTIPLIER

    tp1_price = entry_price * (1 - tp1_pct / 100)
    tp2_price = entry_price * (1 - tp2_pct / 100)

    return (
        sl_price,
        sl_pct,
        tp1_price,
        tp1_pct,
        tp2_price,
        tp2_pct,
        sl_source,
    )


def calculate_position_size(
    capital: float,
    sl_pct: float,
    risk_pct: float,
    max_pct: float,
) -> float:
    """
    Считает размер позиции по риску.

    [ИСПРАВЛЕНО]
    Раньше здесь было:
        return min(max_size, max(5.0, size))

    Это плохо, потому что если расчётный размер получался меньше 5 USDT,
    бот всё равно мог попытаться открыть 5 USDT и нарушить риск.

    Теперь функция просто считает риск и ограничивает размер максимумом.
    Если размер получился слишком маленьким — это должно проверяться отдельно.
    """

    if capital <= 0 or sl_pct <= 0:
        return 0.0

    # Сколько денег мы готовы потерять в этой сделке.
    risk_usdt = capital * risk_pct / 100.0

    # Размер позиции, при котором потеря на SL будет примерно risk_usdt.
    size_by_risk = risk_usdt / (sl_pct / 100.0)

    # Максимальный размер позиции по лимиту капитала.
    max_size = capital * max_pct / 100.0

    # Итоговый размер — меньшее из риска и макс.лимита.
    size = min(size_by_risk, max_size)

    if size <= 0:
        return 0.0

    return size


def size_is_valid(size: float, min_notional: float = 5.0) -> bool:
    """
    Проверяет, можно ли открывать сделку такого размера.

    Использует два лимита:
    1. min_notional — минимальный размер ордера на Binance.
    2. Config.MIN_POSITION_SIZE_USDT — наш внутренний минимальный размер.

    [НОВОЕ]
    Эта функция нужна, чтобы не форсить минимальный размер внутри расчёта риска,
    а просто отклонять слишком маленькие сделки.
    """

    if size <= 0:
        return False

    min_allowed = max(
        float(min_notional),
        float(Config.MIN_POSITION_SIZE_USDT),
    )

    return size >= min_allowed