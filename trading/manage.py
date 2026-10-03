"""Сопровождение позиций: программные ветки (BE, трейлинг, TIMEOUT,
VOL_DECAY) и трекинг MFE/MAE.

Разделение ответственности (Д8-v2): TP1/TP2 — биржевые ордера, их
исполнение приходит событием venue (engine); этот модуль решает
только программные ветки монитора. Пороги — EngineSettings
([ТРЕБУЕТСЯ СВЕРКА значений при интеграции, В1]).

Семантика закрытия по локальному SL: срабатывает только если
local_sl_price отличается от биржевого sl_price (т.е. уровень
двигали BE/трейлингом); не сдвинутый уровень ждёт биржевой ордер —
иначе мы бы дублировали срабатывание SL на опережение.
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .settings import EngineSettings
from .types import ExitReason, PositionSnapshot, Side


@dataclass(frozen=True, slots=True)
class ManageAction:
    """Решение монитора по позиции."""

    kind: str  # "none" | "close" | "trail_move"
    exit_reason: ExitReason | None = None
    new_local_sl: Decimal | None = None
    detail: str = ""


def profit_pct(pos: PositionSnapshot, price: Decimal) -> Decimal:
    """Текущая прибыль позиции в % (знаковая, по направлению)."""
    if pos.side is Side.LONG:
        return (price - pos.entry_price) / pos.entry_price * Decimal("100")
    return (pos.entry_price - price) / pos.entry_price * Decimal("100")


def update_mfe_mae(
    pos: PositionSnapshot, price: Decimal
) -> tuple[Decimal, Decimal]:
    """Обновить экстремумы позиции (возвращает новые mfe/mae)."""
    if pos.mfe_price is None or price > pos.mfe_price:
        pos.mfe_price = price
    if pos.mae_price is None or price < pos.mae_price:
        pos.mae_price = price
    return pos.mfe_price, pos.mae_price  # тип-хвост: mypy strict допускает?


def check_position(
    pos: PositionSnapshot,
    price: Decimal,
    now_ms: int,
    settings: EngineSettings,
    volume_ratio: float | None = None,
) -> ManageAction:
    """Принять решение по позиции для этого тика монитора.

    Args:
        pos: текущее состояние (mutate только mfe/mae снаружи);
        price: live-цена;
        now_ms: время;
        settings: пороги;
        volume_ratio: vol/avgVol, если доступен (иначе VOL_DECAY
            пропускается — данные не подаются, решение владельца).

    Returns:
        ManageAction: close/trail_move/none с точной причиной.
    """
    # 1) трейлинг-выход по локальному SL (если уровень двигали)
    if pos.local_sl_price != pos.sl_price:
        hit = (
            price <= pos.local_sl_price
            if pos.side is Side.LONG
            else price >= pos.local_sl_price
        )
        if hit:
            reason = ExitReason.TRAIL_SL if pos.trail_active else ExitReason.BE_SL
            return ManageAction(kind="close", exit_reason=reason,
                                detail=f"local_sl={pos.local_sl_price}")
    # 2) активация/ступени трейлинга (только ужесточение)
    pct = profit_pct(pos, price)
    if not pos.trail_active and pct >= Decimal(str(settings.trailing_activation_pct)):
        pos.trail_active = True
        step = Decimal(str(settings.trailing_step_pct)) / Decimal("100")
        if pos.side is Side.LONG:
            new_sl = price * (Decimal("1") - step)
            if new_sl > pos.local_sl_price:
                return ManageAction(kind="trail_move", new_local_sl=new_sl)
        else:
            new_sl = price * (Decimal("1") + step)
            if new_sl < pos.local_sl_price:
                return ManageAction(kind="trail_move", new_local_sl=new_sl)
    elif pos.trail_active:
        step = Decimal(str(settings.trailing_step_pct)) / Decimal("100")
        if pos.side is Side.LONG:
            candidate = pos.local_sl_price * (Decimal("1") + step)
            if price >= candidate:  # цена ушла на ступень — тянем SL
                return ManageAction(kind="trail_move", new_local_sl=candidate)
        else:
            candidate = pos.local_sl_price * (Decimal("1") - step)
            if price <= candidate:
                return ManageAction(kind="trail_move", new_local_sl=candidate)
    # 3) VOL_DECAY (только при поданных объёмах)
    if (
        settings.vol_decay_enabled
        and volume_ratio is not None
        and volume_ratio < settings.vol_decay_ratio
        and pct < Decimal(str(settings.vol_decay_min_profit_pct))
    ):
        return ManageAction(kind="close", exit_reason=ExitReason.VOL_DECAY,
                            detail=f"vol_ratio={volume_ratio:.2f}")
    # 4) TIMEOUT
    age_sec = (now_ms - pos.entry_ts_ms) / 1000
    if age_sec >= settings.max_hold_sec:
        return ManageAction(kind="close", exit_reason=ExitReason.TIMEOUT,
                            detail=f"age={age_sec:.0f}s")
    return ManageAction(kind="none")