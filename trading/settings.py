"""Настройки торговой части (единственный источник порогов движка).

[ТРЕБУЕТСЯ СВЕРКА — Часть 4] Значения ниже — рабочие дефолты для
тестов. При интеграции каждое поле маппится один-в-один из
существующего config.py (семантика 1:1, решение В1): перечень
соответствий — в сверочном списке Части 4. Движок никогда не
читает Config напрямую — только через этот класс (граница систем).
"""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field


class AdaptiveThresholds(BaseModel):
    """Порог score для входа в зависимости от side и тренда BTC.

    [ТРЕБУЕТСЯ СВЕРКА] Формула воспроизводит PositionManager.
    get_adaptive_threshold(side, btc_trend): base[side] +
    linear-поправка по btc_trend с ограничением.
    """

    model_config = ConfigDict(strict=True)

    base_long: float = 6.0
    base_short: float = 7.0
    trend_adjust: float = 1.5  # |поправка| при |btc_trend| == 1.0
    trend_cap: float = 2.0

    def threshold(self, side_long: bool, btc_trend: float) -> float:
        """Порог score: база по стороне + поправка по тренду BTC."""
        base = self.base_long if side_long else self.base_short
        adj = max(-self.trend_cap, min(self.trend_cap, btc_trend * self.trend_adjust))
        # лонг в восходящем BTC-тренде — порог ниже (легче войти), и наоборот
        if side_long:
            return base - adj
        return base + adj


class EngineSettings(BaseModel):
    """Все пороги/тайминги движка. Значения — [ТРЕБУЕТСЯ СВЕРКА]."""

    model_config = ConfigDict(strict=True)

    # --- размер и лимиты ---
    position_size_usdt: Decimal = Decimal("20")
    max_open_positions: int = Field(default=5, ge=1)
    daily_max_loss_usdt: Decimal = Decimal("50")
    daily_max_trades: int = Field(default=20, ge=1)

    # --- кулдауны (сек) ---
    sl_cooldown_sec: float = 900.0      # после SL-закрытия по символу
    repeat_cooldown_sec: float = 60.0   # между любыми сделками по символу
    gap_cooldown_sec: float = 300.0     # окно защиты от входа после гэпа
    gap_min_move_pct: float = 1.0       # что считать гэпом от цены выхода

    # --- тейки (биржевые ордера, Д8-v2) ---
    tp1_activation_pct: float = 1.2     # profit%, при котором стоит TP1
    tp1_share_pct: Decimal = Decimal("50")  # доля позиции в TP1
    tp2_activation_pct: float = 2.4

    # --- сопровождение (программное, В3 program-режим) ---
    trailing_activation_pct: float = 2.0
    trailing_step_pct: float = 0.4
    max_hold_sec: float = 3600.0
    vol_decay_enabled: bool = False  # [ТРЕБУЕТСЯ РЕШЕНИЕ: подача объёмов]
    vol_decay_ratio: float = 0.5     # volume/avgVolume ниже этого
    vol_decay_min_profit_pct: float = 0.3

    # --- защита ---
    iron_sl_offset_pct: float = 0.3   # хуже SL (§10)
    sl_restore_attempts: int = 3
    sl_restore_interval_sec: float = 0.3
    sl_rest_check_interval_sec: float = 30.0  # REST-контроль Части A

    # --- служебное ---
    monitor_interval_sec: float = 2.0
    reconcile_interval_min: float = 10.0
    filters_refresh_hours: float = 3.0
    balance_buffer_pct: float = 0.5   # запас на комиссию/slippage при гейте
    equity_delta_pct: float = 0.01    # Б2-3(а)
    max_spread_pct: float | None = None  # гейт спреда; None = выкл [СВЕРКА]
    min_score: float = 0.0
    adaptive: AdaptiveThresholds = AdaptiveThresholds()