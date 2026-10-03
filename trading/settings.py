"""Настройки движка — значения 1:1 из config.py (сверка Части 4, решение В1).

Единственный источник порогов для trading/*. Config читается один раз
в from_config(); движок Config напрямую не читает (граница систем).
Девиации против config.py отсутствуют, кроме помеченных.
"""
from __future__ import annotations

from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field

_DEFAULT_TRAILING_STEPS: dict[int, float] = {0: 4.0, 8: 3.0, 15: 2.0, 25: 1.0, 40: 0.5}


class EngineSettings(BaseModel):
    """Все пороги/тайминги; комментарии — имена атрибутов Config."""

    model_config = ConfigDict(strict=True)

    # --- размер/лимиты (config §3) ---
    position_size_usdt: Decimal = Decimal("20")    # FIXED (был manual 20.0)
    min_position_size_usdt: Decimal = Decimal("11")  # MIN_POSITION_SIZE_USDT
    max_open_positions: int = 1                    # MAX_OPEN_POSITIONS
    daily_max_loss_usdt: Decimal = Decimal("50")   # DAILY_MAX_LOSS_USDT
    daily_max_trades: int = 20                     # MAX_TRADES_PER_DAY (0=off)

    # --- score (config §9) ---
    score_threshold_long: float = 41.0             # SCORE_TRADE_THRESHOLD
    score_threshold_short: float = 35.0            # SCORE_TRADE_THRESHOLD_SHORT
    strong_score_reentry: float = 68.0             # STRONG_SCORE_FOR_REENTRY
    short_trading_enabled: bool = True             # SHORT_TRADING_ENABLED

    # --- кулдауны (config §13) ---
    sl_cooldown_sec: float = 3600.0                # SL_COOLDOWN_HOURS=1
    repeat_stop_limit: int = 2                     # REPEAT_STOP_LIMIT / 24ч
    repeat_block_sec: float = 1800.0                # REPEAT_BLOCK_HOURS=0.5
    gap_block_sec: float = 1800.0                  # GAP_BLOCK_HOURS (ветка спит, как в старом коде)

    # --- TP (R-мультипликаторы, config §11) ---
    tp1_share: float = 0.6                         # TP1_SIZE_FRAC
    min_notional_safety: float = 1.05              # MIN_NOTIONAL_SAFETY_MARGIN

    # --- сопровождение (config §11/§12) ---
    breakeven_activation_pct: float = 1.5          # BREAKEVEN_ACTIVATION_PCT
    breakeven_buffer_pct: float = 0.15             # BREAKEVEN_BUFFER_PCT
    trailing_activation_pct: float = 3.0            # TRAILING_ACTIVATION_PCT
    trailing_steps: dict[int, float] = Field(
        default_factory=lambda: dict(_DEFAULT_TRAILING_STEPS)
    )
    max_hold_sec: float = 86400.0                  # POSITION_TIMEOUT_HOURS=24
    vol_decay_after_min: float = 60.0              # VOL_DECAY_CHECK_AFTER_MIN
    vol_decay_window_min: int = 10                 # VOL_DECAY_WINDOW_MIN
    vol_decay_prior_min: int = 60                  # VOL_DECAY_PRIOR_WINDOW_MIN
    vol_decay_ratio: float = 0.3                   # VOL_DECAY_RATIO

    # --- защита (config §11 + черновик §11) ---
    iron_sl_offset_pct: float = 0.2                # IRON_SL_BUFFER_PCT
    sl_restore_attempts: int = 3                   # tracker: 3 попытки
    sl_restore_interval_sec: float = 0.5           # tracker: sleep(0.5)
    sl_rest_check_interval_sec: float = 30.0       # tracker: не чаще 30 с

    # --- служебное ---
    monitor_interval_sec: float = 2.0             # POSITION_CHECK_INTERVAL
    reconcile_interval_min: float = 10.0
    filters_refresh_hours: float = 3.0             # FILTERS_CACHE_UPDATE_INTERVAL/3600
    equity_delta_pct: float = 0.01                 # Б2-3(а)

    def adaptive_threshold(self, side_long: bool, btc_trend: float) -> float:
        """Точная формула старого get_adaptive_threshold (сверено по коду).

        LONG: база SCORE_TRADE_THRESHOLD без поправок.
        SHORT: база ±3 при |btc_trend| > 2.
        """
        if side_long:
            return self.score_threshold_long
        if btc_trend < -2.0:
            return self.score_threshold_short - 3.0
        if btc_trend > 2.0:
            return self.score_threshold_short + 3.0
        return self.score_threshold_short

    @classmethod
    def from_config(cls, cfg: object) -> "EngineSettings":
        """Собрать настройки из Config по именам атрибутов (getattr —
        конфиг может не иметь новых ключей; дефолты уже = значениям Config)."""
        def g(name: str, default: object) -> object:
            return getattr(cfg, name, default)

        return cls(
            position_size_usdt=Decimal(str(g("FIXED_POSITION_SIZE_USDT", 20.0))),
            min_position_size_usdt=Decimal(str(g("MIN_POSITION_SIZE_USDT", 11.0))),
            max_open_positions=int(g("MAX_OPEN_POSITIONS", 1)),
            daily_max_loss_usdt=Decimal(str(g("DAILY_MAX_LOSS_USDT", 50.0))),
            daily_max_trades=int(g("MAX_TRADES_PER_DAY", 20)),
            score_threshold_long=float(g("SCORE_TRADE_THRESHOLD", 41.0)),
            score_threshold_short=float(g("SCORE_TRADE_THRESHOLD_SHORT", 35.0)),
            strong_score_reentry=float(g("STRONG_SCORE_FOR_REENTRY", 68.0)),
            short_trading_enabled=bool(g("SHORT_TRADING_ENABLED", True)),
            sl_cooldown_sec=float(g("SL_COOLDOWN_HOURS", 1.0)) * 3600.0,
            repeat_stop_limit=int(g("REPEAT_STOP_LIMIT", 2)),
            repeat_block_sec=float(g("REPEAT_BLOCK_HOURS", 0.5)) * 3600.0,
            gap_block_sec=float(g("GAP_BLOCK_HOURS", 0.5)) * 3600.0,
            tp1_share=float(g("TP1_SIZE_FRAC", 0.6)),
            min_notional_safety=float(g("MIN_NOTIONAL_SAFETY_MARGIN", 1.05)),
            breakeven_activation_pct=float(g("BREAKEVEN_ACTIVATION_PCT", 1.5)),
            breakeven_buffer_pct=float(g("BREAKEVEN_BUFFER_PCT", 0.15)),
            trailing_activation_pct=float(g("TRAILING_ACTIVATION_PCT", 3.0)),
            trailing_steps=dict(g("TRAILING_STEPS", _DEFAULT_TRAILING_STEPS)),
            max_hold_sec=float(g("POSITION_TIMEOUT_HOURS", 24.0)) * 3600.0,
            vol_decay_after_min=float(g("VOL_DECAY_CHECK_AFTER_MIN", 60)),
            vol_decay_window_min=int(g("VOL_DECAY_WINDOW_MIN", 10)),
            vol_decay_prior_min=int(g("VOL_DECAY_PRIOR_WINDOW_MIN", 60)),
            vol_decay_ratio=float(g("VOL_DECAY_RATIO", 0.3)),
            iron_sl_offset_pct=float(g("IRON_SL_BUFFER_PCT", 0.2)),
            monitor_interval_sec=float(g("POSITION_CHECK_INTERVAL", 2.0)),
            filters_refresh_hours=float(g("FILTERS_CACHE_UPDATE_INTERVAL", 10800)) / 3600.0,
        )