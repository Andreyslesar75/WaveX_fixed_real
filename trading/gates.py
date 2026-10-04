"""Гейты входа — порядок и строки причин 1:1 со старым risk_manager.open_position.

Сверено по реальному коду (Часть 4, §0). Ключевые точки паритета:
- порядок: max_positions -> already_open -> short_disabled -> confidence
  -> дневные лимиты -> score -> repeat -> gap -> SL-кулдаун (обход при
  score >= STRONG_SCORE_FOR_REENTRY, repeat/gap НЕ обходятся) -> фильтры
  -> размер/минимумы -> баланс (real, без буфера: bal < size);
- gap-ветка присутствует, но GAP_SL никто не генерирует (и в старом коде
  была мертва) — паритет сохранён, пробуждать не будем (REPORT).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal

from .levels import CalculatedLevels
from .money import compute_entry_qty
from .settings import EngineSettings
from .types import RejectReason, Side, SignalInput, SymbolFilters


@dataclass(slots=True)
class GateState:
    """Кулдауны/лимиты (1:1 со словарями старого risk_manager)."""

    open_symbols: set[str] = field(default_factory=set)
    cooldown_until_ms: dict[str, int] = field(default_factory=dict)      # _cooldown_until (SL)
    repeat_block_until_ms: dict[str, int] = field(default_factory=dict)  # _repeat_block_until
    gap_block_until_ms: dict[str, int] = field(default_factory=dict)     # _gap_block_until
    stop_history_ms: dict[str, list[int]] = field(default_factory=dict)  # _stop_history
    today_trades: int = 0
    today_realized: Decimal = Decimal("0")
    day_start_ms: int = 0


@dataclass(frozen=True, slots=True)
class GateOutcome:
    """Результат гейтов: ok+qty либо точная причина (строка — в detail)."""

    ok: bool
    reason: RejectReason | None = None
    detail: str = ""
    qty: Decimal | None = None

    @classmethod
    def passed(cls, qty: Decimal) -> GateOutcome:
        return cls(ok=True, qty=qty)

    @classmethod
    def fail(cls, reason: RejectReason, detail: str = "") -> GateOutcome:
        return cls(ok=False, reason=reason, detail=detail)


def check_gates(
    signal: SignalInput,
    levels: CalculatedLevels,
    state: GateState,
    settings: EngineSettings,
    filters: SymbolFilters,
    balance: Decimal | None,   # None => пропуск балансового гейта (paper, паритет)
    now_ms: int,
) -> GateOutcome:
    """Прогнать сигнал; первый провал = реджект с точной причиной."""
    del levels  # геометрия уровней уже гарантируется fixup-блоком levels.py
    sym = signal.symbol

    if len(state.open_symbols) >= settings.max_open_positions:
        return GateOutcome.fail(RejectReason.MAX_POSITIONS, "max_positions")
    if sym in state.open_symbols:
        return GateOutcome.fail(RejectReason.DUPLICATE, "already_open")
    if signal.side is Side.SHORT and not settings.short_trading_enabled:
        return GateOutcome.fail(RejectReason.SHORT_DISABLED, "short_disabled")
    if signal.confidence not in ("HIGH", "MEDIUM"):
        return GateOutcome.fail(
            RejectReason.LOW_CONFIDENCE, f"low_conf_{signal.confidence}"
        )
    if settings.daily_max_trades > 0 and state.today_trades >= settings.daily_max_trades:
        return GateOutcome.fail(
            RejectReason.DAILY_LIMIT,
            f"max_trades_per_day {state.today_trades}/{settings.daily_max_trades}",
        )
    if state.today_realized <= -settings.daily_max_loss_usdt:
        return GateOutcome.fail(
            RejectReason.DAILY_LIMIT,
            f"daily_loss_limit {state.today_realized:.2f} "
            f"<= {-settings.daily_max_loss_usdt:.1f}",
        )
    threshold = settings.adaptive_threshold(
        signal.side is Side.LONG, signal.btc_trend
    )
    if signal.score < threshold:
        return GateOutcome.fail(
            RejectReason.SCORE_THRESHOLD,
            f"score_{signal.score:.0f}<{threshold:.0f}",
        )
    repeat_until = state.repeat_block_until_ms.get(sym, 0)
    if now_ms < repeat_until:
        return GateOutcome.fail(
            RejectReason.COOLDOWN_REPEAT,
            f"repeat_block_{int((repeat_until - now_ms) / 1000)}s",
        )
    gap_until = state.gap_block_until_ms.get(sym, 0)
    if now_ms < gap_until:
        return GateOutcome.fail(
            RejectReason.GAP_PROTECTION,
            f"gap_block_{int((gap_until - now_ms) / 1000)}s",
        )
    cooldown_until = state.cooldown_until_ms.get(sym, 0)
    if now_ms < cooldown_until and signal.score < settings.strong_score_reentry:
        return GateOutcome.fail(
            RejectReason.COOLDOWN_SL,
            f"blocked_until_{int((cooldown_until - now_ms) / 1000)}s",
        )
    if not filters.is_trading:
        return GateOutcome.fail(RejectReason.SYMBOL_NOT_TRADING, filters.status)
    if settings.position_size_usdt < settings.min_position_size_usdt:
        return GateOutcome.fail(
            RejectReason.NOTIONAL_BELOW_MIN,
            f"size_too_small ({settings.position_size_usdt:.2f} < min "
            f"{settings.min_position_size_usdt:.2f})",
        )

    qty_result = compute_entry_qty(
        settings.position_size_usdt, signal.price, filters
    )
    if qty_result[0] is None:
        reason = qty_result[1]
        assert reason is not None  # инвариант QtyResult
        detail = reason.value
        if reason is RejectReason.NOTIONAL_BELOW_MIN:
            min_needed = max(
                filters.min_notional, settings.min_position_size_usdt
            )
            detail = (
                f"size_too_small ({settings.position_size_usdt:.2f} "
                f"< min {min_needed:.2f})"
            )
        return GateOutcome.fail(reason, detail)
    if balance is not None and balance < settings.position_size_usdt:
        return GateOutcome.fail(
            RejectReason.INSUFFICIENT_BALANCE, "insufficient_balance"
        )
    return GateOutcome.passed(qty_result[0])
