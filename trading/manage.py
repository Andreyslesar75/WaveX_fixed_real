"""Программные ветки сопровождения — семантика 1:1 с position_tracker
(сверено по коду, Часть 4 §0): BE (2 пути, буфер 0.15%, только улучшение),
лестница трейлинга TRAILING_STEPS, локальный SL-выход с классификацией
TRAIL_SL/BE_SL, TIMEOUT, VOL_DECAY (после 60 мин удержания).

TP1/TP2 — биржевые ордера (Д8-v2), здесь не проверяются.
Классификация причин на закрытии — как в старом _close_position:
trail_active -> TRAIL_SL; breakeven -> BE_SL; иначе SL (биржевой путь).
"""
from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Mapping

from .settings import EngineSettings
from .types import ExitReason, PositionSnapshot, Side


@dataclass(frozen=True, slots=True)
class ManageAction:
    """Решение монитора: none | close | trail_move | breakeven."""

    kind: str
    exit_reason: ExitReason | None = None
    new_local_sl: Decimal | None = None
    detail: str = ""


def profit_pct(pos: PositionSnapshot, price: Decimal) -> Decimal:
    """Прибыль позиции в % (по направлению)."""
    if pos.side is Side.LONG:
        return (price - pos.entry_price) / pos.entry_price * Decimal("100")
    return (pos.entry_price - price) / pos.entry_price * Decimal("100")


def breakeven_price(side: Side, entry: Decimal, settings: EngineSettings) -> Decimal:
    """Цена BE с буфером (entry×(1±0.15%)) — 1:1 _set_breakeven."""
    buffer = Decimal(str(settings.breakeven_buffer_pct)) / Decimal("100")
    mult = Decimal("1") + buffer if side is Side.LONG else Decimal("1") - buffer
    return entry * mult


def trailing_sl(
    profit: Decimal, side: Side, price: Decimal,
    steps: Mapping[int, float],
) -> Decimal:
    """SL по лестнице: ступень = максимальный ключ ≤ profit (1:1 _calc_trailing_sl)."""
    step_key = min(steps) if steps else 0
    for key in sorted(steps):
        if profit >= Decimal(str(key)):
            step_key = key
    offset = Decimal(str(steps[step_key])) / Decimal("100")
    if side is Side.LONG:
        return price * (Decimal("1") - offset)
    return price * (Decimal("1") + offset)


def improves(side: Side, candidate: Decimal, current: Decimal) -> bool:
    """Улучшение SL: LONG — выше, SHORT — ниже (1:1)."""
    if side is Side.LONG:
        return candidate > current
    return candidate < current


def update_mfe_mae(pos: PositionSnapshot, price: Decimal) -> None:
    """Экстремумы (mutate позиции движка — под локом монитора)."""
    if pos.mfe_price is None or price > pos.mfe_price:
        pos.mfe_price = price
    if pos.mae_price is None or price < pos.mae_price:
        pos.mae_price = price


def check_position(
    pos: PositionSnapshot,
    price: Decimal,
    now_ms: int,
    settings: EngineSettings,
    volume_ratio: float | None = None,
) -> ManageAction:
    """Решение по позиции на тике монитора (порядок — как в _check_conditions).

    volume_ratio: recent/prior средние объёмов; None = данных нет
    (VOL_DECAY пропускается — как при ошибке klines в старом коде).
    """
    # 1) TIMEOUT
    if (now_ms - pos.entry_ts_ms) / 1000 >= settings.max_hold_sec:
        return ManageAction(kind="close", exit_reason=ExitReason.TIMEOUT,
                            detail="timeout")
    # 2) VOL_DECAY (после 60 мин, при поданном ratio)
    hold_min = (now_ms - pos.entry_ts_ms) / 60_000
    if (
        volume_ratio is not None
        and hold_min >= settings.vol_decay_after_min
        and volume_ratio < settings.vol_decay_ratio
    ):
        return ManageAction(kind="close", exit_reason=ExitReason.VOL_DECAY,
                            detail=f"vol_ratio={volume_ratio:.2f}")
    pct = profit_pct(pos, price)
    # 3) трейлинг: активация ставит флаг ДАЖЕ если SL лестницы пока не
    # улучшает текущий (1:1 position_tracker: после активации выход
    # классифицируется TRAIL_SL, а не BE_SL)
    if pct >= Decimal(str(settings.trailing_activation_pct)):
        pos.trail_active = True
        new_sl = trailing_sl(pct, pos.side, price, settings.trailing_steps)
        if improves(pos.side, new_sl, pos.local_sl_price):
            return ManageAction(kind="trail_move", new_local_sl=new_sl)
    # 5) локальный SL (уровень двигали BE/трейлингом; биржевой SL отдельно)
    if pos.local_sl_price != pos.sl_price:
        hit = (
            price <= pos.local_sl_price
            if pos.side is Side.LONG
            else price >= pos.local_sl_price
        )
        if hit:
            reason = (
                ExitReason.TRAIL_SL if pos.trail_active else ExitReason.BE_SL
            )
            return ManageAction(kind="close", exit_reason=reason,
                                detail=f"local_sl={pos.local_sl_price}")
    # 6) отдельный BE (без TP1): профит >= 1.5%
    if (
        not pos.breakeven_done
        and pct >= Decimal(str(settings.breakeven_activation_pct))
    ):
        be = breakeven_price(pos.side, pos.entry_price, settings)
        if improves(pos.side, be, pos.local_sl_price):
            return ManageAction(kind="breakeven", new_local_sl=be)
    return ManageAction(kind="none")