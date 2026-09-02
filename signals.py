#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
# ФАЙЛ: signals.py
# СОХРАНИТЬ КАК: signals.py

Этот файл отвечает за поиск сигналов:
- импульс вверх/вниз;
- исторический памп/дамп;
- откат/отскок;
- консолидация;
- ракета;
- итоговый score для LONG и SHORT.

ЧТО ИСПРАВЛЕНО:
1. В detect_rocket_pullback() и detect_rocket_pushback_down()
   защита от слишком большого 24h изменения теперь использует abs().

   Раньше SHORT-функция ждала отрицательное значение:
       change_24h <= -400

   Но scanner мог передавать абсолютное изменение цены,
   из-за чего защита не срабатывала.

   Теперь:
       abs(change_24h) >= 400

   Это работает и если передаётся signed change,
   и если передаётся abs change.

2. Добавлены комментарии, чтобы было понятно,
   что делает каждая группа функций.
"""

import numpy as np
from typing import Optional, Tuple

from config import Config
from logger import safe_mean


# ================================================================
# ФИЛЬТРЫ
# ================================================================

def check_not_freefall(O: np.ndarray, C: np.ndarray) -> Tuple[bool, str]:
    """
    Фильтр свободного падения для LONG.

    Если монета падает без отскоков и без зелёных свечей,
    LONG-вход считается слишком опасным.
    """

    # Если фильтр выключен в config.py — всегда пропускаем.
    if not Config.FREEFALL_FILTER_ENABLED:
        return True, ""

    n = len(C)
    w = Config.FREEFALL_WINDOW

    if n < w + 3:
        return True, ""

    # Условие A:
    # последняя закрытая свеча зелёная.
    cond_a = bool(C[-2] > O[-2])

    # Условие B:
    # среди последних закрытых свечей достаточно зелёных.
    wO = O[-(w + 1):-1]
    wC = C[-(w + 1):-1]

    ratio = float(np.sum(wC > wO)) / len(wC) if len(wC) > 0 else 1.0
    cond_b = ratio >= Config.FREEFALL_MIN_GREEN_RATIO

    # Условие C:
    # предыдущая закрытая свеча не ниже свечи до неё.
    cond_c = bool(n >= 3 and C[-2] >= C[-3])

    # Если хотя бы одно условие выполнено — это не свободное падение.
    if cond_a or cond_b or cond_c:
        return True, ""

    return False, (
        f"свободное падение: посл.{'✓' if cond_a else '✗'} "
        f"доля_зел={ratio * 100:.0f}%{'✓' if cond_b else '✗'} "
        f"моментум{'✓' if cond_c else '✗'}"
    )


def check_pullback_setup(
    H: np.ndarray,
    L: np.ndarray,
    C: np.ndarray,
    impulse: dict,
) -> Tuple[bool, str, float]:
    """
    Проверка LONG-отката после импульса вверх.

    Нужно, чтобы цена:
    - уже отошла от хая импульса;
    - но не ушла слишком глубоко;
    - желательно было сжатие/консолидация.
    """

    iH = impulse["impulse_high"]
    iL = impulse["impulse_low"]

    rng = iH - iL

    if rng <= 0:
        return True, "", 0.0

    cur = float(C[-1])

    # Насколько цена откатила от хая импульса.
    retrace_pct = (iH - cur) / rng * 100.0

    if retrace_pct < Config.PULLBACK_MIN_RETRACE_PCT:
        return False, (
            f"откат {retrace_pct:.0f}% < {Config.PULLBACK_MIN_RETRACE_PCT:.0f}% "
            f"(слишком близко к хаю)"
        ), retrace_pct

    if retrace_pct > Config.PULLBACK_MAX_RETRACE_PCT:
        return False, (
            f"откат {retrace_pct:.0f}% > {Config.PULLBACK_MAX_RETRACE_PCT:.0f}% "
            f"(слишком глубоко)"
        ), retrace_pct

    n = len(C)
    w = Config.PULLBACK_CONSOL_BARS

    # Проверяем сжатие последних баров.
    if n >= w + 1 and cur > 0:
        recent_H = H[-(w + 1):-1]
        recent_L = L[-(w + 1):-1]

        if len(recent_H) > 0:
            consol_range_pct = (
                float(np.max(recent_H)) - float(np.min(recent_L))
            ) / cur * 100.0

            if consol_range_pct > Config.PULLBACK_CONSOL_MAX_RANGE_PCT:
                return False, (
                    f"нет сжатия: диапазон посл.{w} баров "
                    f"{consol_range_pct:.1f}% > "
                    f"{Config.PULLBACK_CONSOL_MAX_RANGE_PCT:.1f}%"
                ), retrace_pct

    return True, "", retrace_pct


def check_pushback_setup(
    H: np.ndarray,
    L: np.ndarray,
    C: np.ndarray,
    impulse: dict,
) -> Tuple[bool, str, float]:
    """
    Проверка SHORT-отскока после импульса вниз.

    Зеркальная версия check_pullback_setup().
    """

    iH = impulse["impulse_high"]
    iL = impulse["impulse_low"]

    rng = iH - iL

    if rng <= 0:
        return True, "", 0.0

    cur = float(C[-1])

    # Насколько цена отскочила от лоя импульса.
    retrace_pct = (cur - iL) / rng * 100.0

    if retrace_pct < Config.PULLBACK_MIN_RETRACE_PCT:
        return False, (
            f"отскок {retrace_pct:.0f}% < {Config.PULLBACK_MIN_RETRACE_PCT:.0f}% "
            f"(слишком близко к лоу)"
        ), retrace_pct

    if retrace_pct > Config.PULLBACK_MAX_RETRACE_PCT:
        return False, (
            f"отскок {retrace_pct:.0f}% > {Config.PULLBACK_MAX_RETRACE_PCT:.0f}% "
            f"(слишком высокий отскок)"
        ), retrace_pct

    n = len(C)
    w = Config.PULLBACK_CONSOL_BARS

    if n >= w + 1 and cur > 0:
        recent_H = H[-(w + 1):-1]
        recent_L = L[-(w + 1):-1]

        if len(recent_H) > 0:
            consol_range_pct = (
                float(np.max(recent_H)) - float(np.min(recent_L))
            ) / cur * 100.0

            if consol_range_pct > Config.PULLBACK_CONSOL_MAX_RANGE_PCT:
                return False, (
                    f"нет сжатия: диапазон посл.{w} баров "
                    f"{consol_range_pct:.1f}% > "
                    f"{Config.PULLBACK_CONSOL_MAX_RANGE_PCT:.1f}%"
                ), retrace_pct

    return True, "", retrace_pct


# ================================================================
# ИМПУЛЬС
# ================================================================

def find_directed_impulse_down(
    H: np.ndarray,
    L: np.ndarray,
    C: np.ndarray,
    V: np.ndarray,
) -> Optional[dict]:
    """
    Ищет направленный импульс вниз на 1m свечах.

    Возвращает словарь с параметрами импульса или None.
    """

    n = len(C)

    if n < Config.IMPULSE_WINDOW + 20:
        return None

    seg_start = n - Config.IMPULSE_WINDOW

    sH = H[seg_start:]
    sL = L[seg_start:]
    sV = V[seg_start:]

    best = None

    for i in range(len(sH) - Config.IMPULSE_MIN_BARS):
        high_val = sH[i]

        if high_val <= 0:
            continue

        end = min(i + Config.IMPULSE_MAX_BARS, len(sL))

        local_L = sL[i + 1:end]

        if len(local_L) < Config.IMPULSE_MIN_BARS - 1:
            continue

        low_val = float(np.min(local_L))

        lo_off = int(np.argmin(local_L))
        lo_idx = i + 1 + lo_off

        pct = (high_val - low_val) / high_val * 100

        if pct < Config.IMPULSE_MIN_PCT:
            continue

        # Между стартом падения и минимумом не должно быть
        # значимого обновления хая.
        mid_highs = sH[i + 1:lo_idx]

        if len(mid_highs) > 0 and float(np.max(mid_highs)) > high_val * 1.015:
            continue

        # Объём импульса относительно фона.
        imp_v = float(np.mean(sV[i:lo_idx + 1]))
        bg_v = float(np.mean(sV[max(0, i - 20):i])) if i > 0 else imp_v

        vm = imp_v / bg_v if bg_v > 0 else 1.0

        if vm < 0.7:
            continue

        age = len(sL) - 1 - lo_idx

        if age > Config.IMPULSE_MAX_AGE_BARS:
            continue

        candidate = {
            "impulse_low": float(low_val),
            "impulse_high": float(high_val),
            "impulse_pct": float(pct),
            "impulse_bars": int(lo_idx - i),
            "vol_mult": float(vm),
            "age_bars": int(age),
            "low_idx": int(seg_start + lo_idx),
            "high_idx": int(seg_start + i),
        }

        if (
            best is None
            or candidate["low_idx"] > best["low_idx"]
            or (
                candidate["low_idx"] == best["low_idx"]
                and candidate["impulse_pct"] > best["impulse_pct"]
            )
        ):
            best = candidate

    return best


def find_directed_impulse(
    H: np.ndarray,
    L: np.ndarray,
    C: np.ndarray,
    V: np.ndarray,
) -> Optional[dict]:
    """
    Ищет направленный импульс вверх на 1m свечах.

    Возвращает словарь с параметрами импульса или None.
    """

    n = len(C)

    if n < Config.IMPULSE_WINDOW + 20:
        return None

    seg_start = n - Config.IMPULSE_WINDOW

    sH = H[seg_start:]
    sL = L[seg_start:]
    sV = V[seg_start:]

    best = None

    for i in range(len(sL) - Config.IMPULSE_MIN_BARS):
        low_val = sL[i]

        if low_val <= 0:
            continue

        end = min(i + Config.IMPULSE_MAX_BARS, len(sH))

        local_H = sH[i + 1:end]

        if len(local_H) < Config.IMPULSE_MIN_BARS - 1:
            continue

        high_val = float(np.max(local_H))

        hi_off = int(np.argmax(local_H))
        hi_idx = i + 1 + hi_off

        pct = (high_val - low_val) / low_val * 100

        if pct < Config.IMPULSE_MIN_PCT:
            continue

        # Между стартом роста и максимумом не должно быть
        # значимого обновления лоя.
        mid_lows = sL[i + 1:hi_idx]

        if len(mid_lows) > 0 and float(np.min(mid_lows)) < low_val * 0.985:
            continue

        # Объём импульса относительно фона.
        imp_v = float(np.mean(sV[i:hi_idx + 1]))
        bg_v = float(np.mean(sV[max(0, i - 20):i])) if i > 0 else imp_v

        vm = imp_v / bg_v if bg_v > 0 else 1.0

        if vm < 0.7:
            continue

        age = len(sL) - 1 - hi_idx

        if age > Config.IMPULSE_MAX_AGE_BARS:
            continue

        candidate = {
            "impulse_low": float(low_val),
            "impulse_high": float(high_val),
            "impulse_pct": float(pct),
            "impulse_bars": int(hi_idx - i),
            "vol_mult": float(vm),
            "age_bars": int(age),
            "low_idx": int(seg_start + i),
            "high_idx": int(seg_start + hi_idx),
        }

        if (
            best is None
            or candidate["high_idx"] > best["high_idx"]
            or (
                candidate["high_idx"] == best["high_idx"]
                and candidate["impulse_pct"] > best["impulse_pct"]
            )
        ):
            best = candidate

    return best


# ================================================================
# ИСТОРИЧЕСКАЯ ВОЛНА / РАКЕТА
# ================================================================

def detect_historical_pump(candles_1h: list) -> Optional[dict]:
    """
    Ищет исторический памп на 1h свечах.

    Используется для логики "второй волны":
    монета уже сильно выросла раньше, затем откатила,
    и теперь может быть вторая волна.
    """

    if len(candles_1h) < 20:
        return None

    window = candles_1h[-168:] if len(candles_1h) >= 168 else candles_1h

    highs = [c[2] for c in window]

    peak_idx = int(np.argmax(highs))
    peak_price = highs[peak_idx]

    if peak_idx < 3:
        return None

    base_price = min(c[3] for c in window[:peak_idx])

    if base_price <= 0:
        return None

    pump_pct = (peak_price - base_price) / base_price * 100.0

    if pump_pct < Config.HIST_PUMP_MIN_PCT:
        return None

    hours_since_peak = len(window) - 1 - peak_idx

    if hours_since_peak <= 72:
        freshness_bonus = 10
        label = "🔥 свежий"

    elif hours_since_peak <= 168:
        freshness_bonus = 0
        label = "✅ актуальный"

    else:
        freshness_bonus = -5
        label = "🟡 устаревший"

    return {
        "pump_pct": pump_pct,
        "peak_price": peak_price,
        "peak_idx": peak_idx,
        "base_price": base_price,
        "hours_since_peak": hours_since_peak,
        "freshness_bonus": freshness_bonus,
        "freshness_label": label,
    }


def detect_historical_dump(candles_1h: list) -> Optional[dict]:
    """
    Ищет исторический дамп на 1h свечах.

    SHORT-версия detect_historical_pump().
    """

    if len(candles_1h) < 20:
        return None

    window = candles_1h[-168:] if len(candles_1h) >= 168 else candles_1h

    lows = [c[3] for c in window]

    trough_idx = int(np.argmin(lows))
    trough_price = lows[trough_idx]

    if trough_idx < 3:
        return None

    base_price = max(c[2] for c in window[:trough_idx])

    if base_price <= 0:
        return None

    dump_pct = (base_price - trough_price) / base_price * 100.0

    if dump_pct < Config.HIST_PUMP_MIN_PCT:
        return None

    hours_since_trough = len(window) - 1 - trough_idx

    if hours_since_trough <= 72:
        freshness_bonus = 10
        label = "🔥 свежий"

    elif hours_since_trough <= 168:
        freshness_bonus = 0
        label = "✅ актуальный"

    else:
        freshness_bonus = -5
        label = "🟡 устаревший"

    return {
        "dump_pct": dump_pct,
        "trough_price": trough_price,
        "trough_idx": trough_idx,
        "base_price": base_price,
        "hours_since_trough": hours_since_trough,
        "freshness_bonus": freshness_bonus,
        "freshness_label": label,
    }


def detect_pullback(candles_1h: list, hist_pump: dict) -> Optional[dict]:
    """
    Ищет откат после исторического пампа.
    """

    if not hist_pump:
        return None

    peak_idx = hist_pump["peak_idx"]

    # Пик должен быть не в самом конце,
    # иначе после него ещё нет отката.
    if peak_idx >= len(candles_1h) - 2:
        return None

    after = candles_1h[peak_idx:]

    if len(after) < 3:
        return None

    lows = [c[3] for c in after]

    pullback_low = min(lows)
    pullback_idx = lows.index(pullback_low) + peak_idx

    pullback_pct = (
        (hist_pump["peak_price"] - pullback_low)
        / hist_pump["peak_price"]
        * 100.0
    )

    if pullback_pct < Config.PULLBACK_MIN_PCT or pullback_pct > 80:
        return None

    current_price = float(candles_1h[-1][4])

    current_pullback = (
        (hist_pump["peak_price"] - current_price)
        / hist_pump["peak_price"]
        * 100.0
    )

    return {
        "pullback_pct": pullback_pct,
        "pullback_low": pullback_low,
        "pullback_idx": pullback_idx,
        "current_pullback_pct": current_pullback,
        "tier": "FULL" if pullback_pct >= 10 else "PARTIAL",
    }


def detect_pushback(candles_1h: list, hist_dump: dict) -> Optional[dict]:
    """
    Ищет отскок после исторического дампа.

    SHORT-версия detect_pullback().
    """

    if not hist_dump:
        return None

    trough_idx = hist_dump["trough_idx"]

    if trough_idx >= len(candles_1h) - 2:
        return None

    after = candles_1h[trough_idx:]

    if len(after) < 3:
        return None

    highs = [c[2] for c in after]

    pushback_high = max(highs)
    pushback_idx = highs.index(pushback_high) + trough_idx

    pushback_pct = (
        (pushback_high - hist_dump["trough_price"])
        / hist_dump["trough_price"]
        * 100.0
    )

    if pushback_pct < Config.PULLBACK_MIN_PCT or pushback_pct > 80:
        return None

    current_price = float(candles_1h[-1][4])

    current_pushback = (
        (current_price - hist_dump["trough_price"])
        / hist_dump["trough_price"]
        * 100.0
    )

    return {
        "pushback_pct": pushback_pct,
        "pushback_high": pushback_high,
        "pushback_idx": pushback_idx,
        "current_pushback_pct": current_pushback,
        "tier": "FULL" if pushback_pct >= 10 else "PARTIAL",
    }


def detect_consolidation(candles_1h: list, pullback_info: dict) -> Optional[dict]:
    """
    Проверяет, есть ли консолидация после отката.
    """

    if not pullback_info:
        return None

    after = candles_1h[pullback_info["pullback_idx"]:]

    if len(after) < 3:
        return None

    window = after[-36:] if len(after) > 36 else after

    high = max(c[2] for c in window)
    low = min(c[3] for c in window)

    range_pct = (high - low) / low * 100.0 if low > 0 else 999

    is_cons = range_pct <= Config.CONSOLIDATION_PRICE_RANGE_PCT

    vols = [c[5] for c in window]

    vol_declining = False

    if len(vols) >= 6:
        half = len(vols) // 2

        fh = safe_mean(vols[:half])
        sh = safe_mean(vols[half:])

        vol_declining = sh < fh * 0.7 if fh > 0 else False

    return {
        "is_consolidating": is_cons,
        "range_pct": range_pct,
        "duration_hours": len(after),
        "vol_declining": vol_declining,
    }


def detect_consolidation_short(candles_1h: list, pushback_info: dict) -> Optional[dict]:
    """
    SHORT-версия detect_consolidation().
    """

    if not pushback_info:
        return None

    return detect_consolidation(
        candles_1h,
        {"pullback_idx": pushback_info["pushback_idx"]},
    )


def detect_rocket_pullback(candles_5m: list, change_24h: float = 0.0) -> Optional[dict]:
    """
    Ищет паттерн "ракета":
    быстрый рост на 5m, затем откат, затем восстановление.

    [ИСПРАВЛЕНО]
    Раньше здесь было:
        change_24h >= 400

    Теперь:
        abs(change_24h) >= 400

    Это защищает функцию и при signed change, и при abs change.
    """

    if len(candles_5m) < 20 or abs(change_24h) >= 400:
        return None

    lookback = Config.ROCKET_LOOKBACK_HOURS * 12

    window = candles_5m[-lookback:] if len(candles_5m) >= lookback else candles_5m

    if len(window) < 20:
        return None

    highs = [c[2] for c in window]
    lows = [c[3] for c in window]
    closes = [c[4] for c in window]
    volumes = [c[5] for c in window]

    peak_idx = int(np.argmax(highs))
    peak_price = highs[peak_idx]

    if len(window) - 1 - peak_idx < 3 or peak_idx < 5:
        return None

    base_price = min(lows[:peak_idx])

    if base_price <= 0:
        return None

    rocket_pct = (peak_price - base_price) / base_price * 100.0

    if rocket_pct < Config.ROCKET_MIN_PCT:
        return None

    lows_after = lows[peak_idx:]

    pullback_low = min(lows_after)

    pullback_ratio = (
        (peak_price - pullback_low)
        / (peak_price - base_price)
        * 100.0
    )

    if not (
        Config.ROCKET_PULLBACK_MIN_PCT
        <= pullback_ratio
        <= Config.ROCKET_PULLBACK_MAX_PCT
    ):
        return None

    current_price = closes[-1]

    if current_price <= pullback_low:
        return None

    recovery_pct = (current_price - pullback_low) / pullback_low * 100.0

    if recovery_pct < 1.0:
        return None

    pb_idx = peak_idx + int(np.argmin(lows_after))

    vols_before = volumes[max(0, pb_idx - 5):pb_idx]
    vols_after = volumes[pb_idx + 1:min(len(window), pb_idx + 6)]

    avg_before = safe_mean(vols_before)
    avg_after = safe_mean(vols_after)

    vol_accel = avg_after / avg_before if avg_before > 0 else 1.0

    if vol_accel < Config.ROCKET_VOL_ACCEL_MIN:
        return None

    hours_since = (window[-1][0] - window[peak_idx][0]) / 3600000.0

    return {
        "rocket_pct": rocket_pct,
        "peak_price": peak_price,
        "pullback_low": pullback_low,
        "pullback_ratio_pct": pullback_ratio,
        "recovery_pct": recovery_pct,
        "vol_accel": vol_accel,
        "hours_since_peak": hours_since,
        "current_price": current_price,
        "is_rocket": True,
    }


def detect_rocket_pushback_down(candles_5m: list, change_24h: float = 0.0) -> Optional[dict]:
    """
    Ищет паттерн "обвал":
    быстрое падение на 5m, затем отскок, затем слабость.

    [ИСПРАВЛЕНО]
    Раньше здесь было:
        change_24h <= -400

    Теперь:
        abs(change_24h) >= 400

    Это защищает функцию и при signed change, и при abs change.
    """

    if len(candles_5m) < 20 or abs(change_24h) >= 400:
        return None

    lookback = Config.ROCKET_LOOKBACK_HOURS * 12

    window = candles_5m[-lookback:] if len(candles_5m) >= lookback else candles_5m

    if len(window) < 20:
        return None

    highs = [c[2] for c in window]
    lows = [c[3] for c in window]
    closes = [c[4] for c in window]
    volumes = [c[5] for c in window]

    trough_idx = int(np.argmin(lows))
    trough_price = lows[trough_idx]

    if len(window) - 1 - trough_idx < 3 or trough_idx < 5:
        return None

    base_price = max(highs[:trough_idx])

    if base_price <= 0:
        return None

    dump_pct = (base_price - trough_price) / base_price * 100.0

    if dump_pct < Config.ROCKET_MIN_PCT:
        return None

    highs_after = highs[trough_idx:]

    pushback_high = max(highs_after)

    pushback_ratio = (
        (pushback_high - trough_price)
        / (base_price - trough_price)
        * 100.0
    )

    if not (
        Config.ROCKET_PULLBACK_MIN_PCT
        <= pushback_ratio
        <= Config.ROCKET_PULLBACK_MAX_PCT
    ):
        return None

    current_price = closes[-1]

    if current_price >= pushback_high:
        return None

    pullback_pct = (pushback_high - current_price) / pushback_high * 100.0

    if pullback_pct < 1.0:
        return None

    pb_idx = trough_idx + int(np.argmax(highs_after))

    vols_before = volumes[max(0, pb_idx - 5):pb_idx]
    vols_after = volumes[pb_idx + 1:min(len(window), pb_idx + 6)]

    avg_before = safe_mean(vols_before)
    avg_after = safe_mean(vols_after)

    vol_accel = avg_after / avg_before if avg_before > 0 else 1.0

    if vol_accel < Config.ROCKET_VOL_ACCEL_MIN:
        return None

    hours_since = (window[-1][0] - window[trough_idx][0]) / 3600000.0

    return {
        "dump_pct": dump_pct,
        "trough_price": trough_price,
        "pushback_high": pushback_high,
        "pushback_ratio_pct": pushback_ratio,
        "pullback_pct": pullback_pct,
        "vol_accel": vol_accel,
        "hours_since_trough": hours_since,
        "current_price": current_price,
        "is_rocket": True,
    }


# ================================================================
# СКОРИНГ
# ================================================================

def compute_hybrid_score(
    impulse,
    hist_pump,
    pullback,
    consolidation,
    rocket,
    micro,
    volume_24h,
    change_pct,
    btc_trend,
    symbol_trend_dev_pct=0.0,
):
    """
    Считает score для LONG.

    Чем выше score, тем сильнее сигнал.
    """

    score = 0.0
    reasons = []
    penalties = []

    # ------------------------------------------------------------
    # 1. Импульс
    # ------------------------------------------------------------

    if impulse:
        age = impulse.get("age_bars", 0)

        if age > 20:
            s = 10 + min(10, impulse["impulse_pct"] - Config.IMPULSE_MIN_PCT)
            s *= 0.8

            score += s

            reasons.append(
                f"импульс {impulse['impulse_pct']:.1f}% "
                f"(x{impulse['vol_mult']:.1f}) устар.{age}"
            )

        else:
            s = 10 + min(10, impulse["impulse_pct"] - Config.IMPULSE_MIN_PCT)

            if impulse["vol_mult"] >= 2.0:
                s += 5

            elif impulse["vol_mult"] >= 1.5:
                s += 3

            score += s

            reasons.append(
                f"импульс {impulse['impulse_pct']:.1f}% "
                f"(x{impulse['vol_mult']:.1f})"
            )

    else:
        penalties.append("нет микро-импульса")

    # ------------------------------------------------------------
    # 2. Исторический памп
    # ------------------------------------------------------------

    if hist_pump:
        s = 12 + hist_pump.get("freshness_bonus", 0)

        score += s

        reasons.append(
            f"ист.памп {hist_pump['pump_pct']:.0f}% "
            f"{hist_pump.get('freshness_label', '')}"
        )

        if pullback:
            if pullback["tier"] == "FULL":
                s = 10
                reasons.append(f"откат {pullback['pullback_pct']:.0f}% (полный)")
            else:
                s = 5
                reasons.append(f"откат {pullback['pullback_pct']:.0f}% (частичный)")

            score += s

        if consolidation and consolidation["is_consolidating"]:
            s = 8

            if consolidation.get("vol_declining"):
                s += 3
                reasons.append("объём снижается в конс.")

            score += s

            reasons.append(f"консолидация {consolidation['range_pct']:.1f}%")

    else:
        penalties.append("нет исторического пампа")

    # ------------------------------------------------------------
    # 3. Ракета
    # ------------------------------------------------------------

    if rocket and rocket.get("is_rocket"):
        s = 15

        if rocket["vol_accel"] >= 2.0:
            s += 5
            reasons.append(f"V-ускор x{rocket['vol_accel']:.1f}")

        elif rocket["vol_accel"] >= 1.5:
            s += 3

        if 20 <= rocket["pullback_ratio_pct"] <= 40:
            s += 3
            reasons.append(f"откат {rocket['pullback_ratio_pct']:.0f}% (идеал)")

        score += s

        reasons.append(f"🚀 ракета {rocket['rocket_pct']:.0f}%")

    # ------------------------------------------------------------
    # 4. Микроструктура
    # ------------------------------------------------------------

    if micro:
        spread = micro.get("spread_pct", 1)
        bsr = micro.get("buy_sell_ratio", 1)
        agg = micro.get("aggression_pct", 50)
        tape = micro.get("tape_speed", 0)

        if spread < 0.08:
            score += 6
            reasons.append(f"спред {spread:.3f}%")

        elif spread < 0.2:
            score += 3

        else:
            penalties.append(f"спред {spread:.2f}%")

        if bsr >= 1.5:
            score += 6
            reasons.append(f"buy/sell {bsr:.2f}")

        elif bsr >= 1.2:
            score += 3

        if agg >= 65:
            score += 4
            reasons.append(f"агрессия {agg:.0f}%")

        if tape >= 200:
            score += 4
            reasons.append(f"лента {tape:.0f} сд/мин")

    else:
        penalties.append("нет микроструктуры")

    # ------------------------------------------------------------
    # 5. BTC trend
    # ------------------------------------------------------------

    if btc_trend >= 1.0:
        score += 6
        reasons.append(f"BTC {btc_trend:+.1f}%")

    elif btc_trend >= -0.5:
        score += 3

    elif btc_trend < Config.BTC_DROP_WARN_PCT:
        penalties.append(f"BTC падает {btc_trend:+.1f}%")
        score -= 1

    if btc_trend < Config.BTC_DROP_STOP_PCT:
        score -= 10

    # ------------------------------------------------------------
    # 6. Ликвидность
    # ------------------------------------------------------------

    if volume_24h > 1_000_000:
        score += 5
        reasons.append(f"V24h ${volume_24h:,.0f}")

    elif volume_24h > 500_000:
        score += 3

    else:
        penalties.append(f"объём {volume_24h:,.0f}")

    # ------------------------------------------------------------
    # 7. Трендовый фильтр символа
    # ------------------------------------------------------------

    if Config.TREND_FILTER_ENABLED:
        if symbol_trend_dev_pct <= -Config.TREND_STRONG_PCT:
            penalties.append(
                f"против тренда 1h {symbol_trend_dev_pct:+.1f}% (сильный)"
            )
            score -= Config.TREND_STRONG_PENALTY

        elif symbol_trend_dev_pct <= -Config.TREND_WARN_PCT:
            penalties.append(f"против тренда 1h {symbol_trend_dev_pct:+.1f}%")
            score -= Config.TREND_WARN_PENALTY

    score = max(0, min(100, score))

    if score >= Config.SCORE_TRADE_THRESHOLD:
        confidence = "HIGH" if score >= 65 else "MEDIUM"

    elif score >= Config.SCORE_WATCH_THRESHOLD:
        confidence = "LOW"

    else:
        confidence = "SKIP"

    return score, confidence, reasons, penalties


def compute_hybrid_score_short(
    impulse_down,
    hist_dump,
    pushback,
    consolidation,
    rocket_down,
    micro,
    volume_24h,
    change_pct,
    btc_trend,
    symbol_trend_dev_pct=0.0,
):
    """
    Считает score для SHORT.

    Чем выше score, тем сильнее сигнал на шорт.
    """

    score = 0.0
    reasons = []
    penalties = []

    # ------------------------------------------------------------
    # 1. Импульс вниз
    # ------------------------------------------------------------

    if impulse_down:
        age = impulse_down.get("age_bars", 0)

        if age > 20:
            s = 10 + min(10, impulse_down["impulse_pct"] - Config.IMPULSE_MIN_PCT)
            s *= 0.8

            score += s

            reasons.append(
                f"импульс вниз {impulse_down['impulse_pct']:.1f}% "
                f"(x{impulse_down['vol_mult']:.1f}) устар.{age}"
            )

        else:
            s = 10 + min(10, impulse_down["impulse_pct"] - Config.IMPULSE_MIN_PCT)

            if impulse_down["vol_mult"] >= 2.0:
                s += 5

            elif impulse_down["vol_mult"] >= 1.5:
                s += 3

            score += s

            reasons.append(
                f"импульс вниз {impulse_down['impulse_pct']:.1f}% "
                f"(x{impulse_down['vol_mult']:.1f})"
            )

    else:
        penalties.append("нет микро-импульса вниз")

    # ------------------------------------------------------------
    # 2. Исторический дамп
    # ------------------------------------------------------------

    if hist_dump:
        s = 12 + hist_dump.get("freshness_bonus", 0)

        score += s

        reasons.append(
            f"ист.обвал {hist_dump['dump_pct']:.0f}% "
            f"{hist_dump.get('freshness_label', '')}"
        )

        if pushback:
            if pushback["tier"] == "FULL":
                s = 10
                reasons.append(f"отскок {pushback['pushback_pct']:.0f}% (полный)")
            else:
                s = 5
                reasons.append(f"отскок {pushback['pushback_pct']:.0f}% (частичный)")

            score += s

        if consolidation and consolidation["is_consolidating"]:
            s = 8

            if consolidation.get("vol_declining"):
                s += 3
                reasons.append("объём снижается в конс.")

            score += s

            reasons.append(f"консолидация {consolidation['range_pct']:.1f}%")

    # ------------------------------------------------------------
    # 3. Обвал / ракета вниз
    # ------------------------------------------------------------

    if rocket_down and rocket_down.get("is_rocket"):
        s = 15

        if rocket_down["vol_accel"] >= 2.0:
            s += 5
            reasons.append(f"V-ускор x{rocket_down['vol_accel']:.1f}")

        elif rocket_down["vol_accel"] >= 1.5:
            s += 3

        if 20 <= rocket_down["pushback_ratio_pct"] <= 40:
            s += 3
            reasons.append(f"отскок {rocket_down['pushback_ratio_pct']:.0f}% (идеал)")

        score += s

        reasons.append(f"📉 обвал {rocket_down['dump_pct']:.0f}%")

    # ------------------------------------------------------------
    # 4. Микроструктура
    # ------------------------------------------------------------

    if micro:
        spread = micro.get("spread_pct", 1)
        bsr = micro.get("buy_sell_ratio", 1)
        agg = micro.get("aggression_pct", 50)
        tape = micro.get("tape_speed", 0)

        if spread < 0.08:
            score += 6
            reasons.append(f"спред {spread:.3f}%")

        elif spread < 0.2:
            score += 3

        else:
            penalties.append(f"спред {spread:.2f}%")

        # BSR — это стакан, а не реальные сделки.
        # Для SHORT он менее надёжен, чем aggression_pct.
        if bsr <= 1.1:
            score += 4
            reasons.append(f"sell/buy {(1 / bsr if bsr > 0 else 0):.2f}")

        elif bsr <= 1.3:
            score += 2

        elif bsr <= 1.6:
            score += 1

        # Агрессия продавцов — более честный сигнал давления продаж.
        if agg <= 45:
            score += 6
            reasons.append(f"агрессия продавцов {100 - agg:.0f}%")

        elif agg <= 55:
            score += 3

        # Скорость ленты сделок.
        if tape >= 200:
            score += 5
            reasons.append(f"лента {tape:.0f} сд/мин")

        elif tape >= 120:
            score += 2

    else:
        penalties.append("нет микроструктуры")

    # ------------------------------------------------------------
    # 5. BTC trend
    # ------------------------------------------------------------

    # [TEMP CHANGE 2026-09-01]
    # BTC penalty for SHORT was reduced by 30% to make shorts less sensitive to BTC strength.
    # This does NOT change SCORE_TRADE_THRESHOLD_SHORT or any other entry threshold.
    short_btc_penalty_factor = 0.7

    if btc_trend <= -1.0:
        score += 6
        reasons.append(f"BTC {btc_trend:+.1f}%")

    elif btc_trend <= 0.5:
        score += 3

    elif btc_trend > Config.BTC_DROP_WARN_PCT * 2:
        penalties.append(f"BTC растёт {btc_trend:+.1f}%")
        score -= 1 * short_btc_penalty_factor

    elif btc_trend > 0.5:
        score -= 1 * short_btc_penalty_factor

    if btc_trend > -Config.BTC_DROP_STOP_PCT * 0.5:
        score -= 5 * short_btc_penalty_factor

    # ------------------------------------------------------------
    # 6. Ликвидность
    # ------------------------------------------------------------

    if volume_24h > 1_000_000:
        score += 5
        reasons.append(f"V24h ${volume_24h:,.0f}")

    elif volume_24h > 500_000:
        score += 3

    else:
        penalties.append(f"объём {volume_24h:,.0f}")

    # ------------------------------------------------------------
    # 7. Трендовый фильтр символа
    # ------------------------------------------------------------

    if Config.TREND_FILTER_ENABLED:
        if symbol_trend_dev_pct >= Config.TREND_STRONG_PCT:
            penalties.append(
                f"против тренда 1h {symbol_trend_dev_pct:+.1f}% (сильный)"
            )
            score -= Config.TREND_STRONG_PENALTY

        elif symbol_trend_dev_pct >= Config.TREND_WARN_PCT:
            penalties.append(f"против тренда 1h {symbol_trend_dev_pct:+.1f}%")
            score -= Config.TREND_WARN_PENALTY

    score = max(0, min(100, score))

    if score >= Config.SCORE_TRADE_THRESHOLD_SHORT:
        confidence = "HIGH" if score >= 65 else "MEDIUM"

    elif score >= Config.SCORE_WATCH_THRESHOLD:
        confidence = "LOW"

    else:
        confidence = "SKIP"

    return score, confidence, reasons, penalties