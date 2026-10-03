# trading/gates.py
"""Гейты входа (Д7): порядок фиксирован, первый провал = реджект.

Чистые функции над GateState — тестируются без движка. Причина
провала возвращается точная (RejectReason) и пишется в signals
(статистика причин реджектов — требование черновика §3).
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
    """Кулдауны/лимиты (in-memory; точные значения таймингов — settings).

    Переживает только процесс: при рестарте дневной лимит
    восстанавливается из БД (engine), кулдауны — сброс (стартовая
    сверка их покрывает: позиция либо есть, либо нет).
    """

    open_symbols: set[str] = field(default_factory=set)
    last_exit_ts_ms: dict[str, int] = field(default_factory=dict)
    last_sl_exit_ts_ms: dict[str, int] = field(default_factory=dict)
    last_exit_price: dict[str, Decimal] = field(default_factory=dict)
    today_trades: int = 0
    today_realized: Decimal = Decimal("0")


@dataclass(frozen=True, slots=True)
class GateOutcome:
    """Результат прогонки гейтов: ok либо точная причина."""

    ok: bool
    reason: RejectReason | None = None
    detail: str = ""

    @classmethod
    def passed(cls) -> "GateOutcome":
        """Успешное прохождение всех гейтов."""
        return cls(ok=True)

    @classmethod
    def fail(cls, reason: RejectReason, detail: str = "") -> "GateOutcome":
        """Провал с причиной (detail — человекочитаемый контекст)."""
        return cls(ok=False, reason=reason, detail=detail)


def check_gates(
    signal: SignalInput,
    levels: CalculatedLevels,
    state: GateState,
    settings: EngineSettings,
    filters: SymbolFilters,
    balance: Decimal | None,
    now_ms: int,
) -> GateOutcome:
    """Прогнать сигнал по гейтам Д7 в фиксированном порядке.

    Args:
        signal: валидированный вход (types.SignalInput);
        levels: рассчитанные SL/TP (до округления tick);
        state: кулдауны/лимиты движка;
        filters: фильтры символа из кэша;
        balance: доступный баланс (None — проверка баланса пропущена,
            её делает движок отдельно после этой функции).

    Returns:
        GateOutcome с точной RejectReason первого провала.
    """
    if signal.confidence == "SKIP":
        return GateOutcome.fail(RejectReason.LOW_CONFIDENCE, "confidence=SKIP")
    threshold = settings.adaptive.threshold(
        signal.side is Side.LONG, signal.btc_trend
    )
    if signal.score < max(threshold, settings.min_score):
        return GateOutcome.fail(
            RejectReason.SCORE_THRESHOLD,
            f"score={signal.score:.2f} < threshold={threshold:.2f}",
        )
    if signal.symbol in state.open_symbols:
        return GateOutcome.fail(RejectReason.DUPLICATE, "позиция уже открыта")
    # кулдауны
    sl_until = state.last_sl_exit_ts_ms.get(signal.symbol)
    if sl_until is not None and now_ms < sl_until + int(
        settings.sl_cooldown_sec * 1000
    ):
        return GateOutcome.fail(RejectReason.COOLDOWN_SL, "после SL-выхода")
    rep_until = state.last_exit_ts_ms.get(signal.symbol)
    if rep_until is not None and now_ms < rep_until + int(
        settings.repeat_cooldown_sec * 1000
    ):
        return GateOutcome.fail(RejectReason.COOLDOWN_REPEAT, "после выхода")
    last_px = state.last_exit_price.get(signal.symbol)
    if last_px is not None:
        moved = abs(signal.price - last_px) / last_px * Decimal("100")
        if moved >= Decimal(str(settings.gap_min_move_pct)) and now_ms < (
            state.last_exit_ts_ms.get(signal.symbol, 0)
            + int(settings.gap_cooldown_sec * 1000)
        ):
            return GateOutcome.fail(RejectReason.GAP_PROTECTION, "цена ушла от выхода")
    # лимиты
    if len(state.open_symbols) >= settings.max_open_positions:
        return GateOutcome.fail(RejectReason.MAX_POSITIONS, "слишком много открытых")
    if state.today_trades >= settings.daily_max_trades:
        return GateOutcome.fail(RejectReason.DAILY_LIMIT, "лимит сделок за день")
    if state.today_realized <= -settings.daily_max_loss_usdt:
        return GateOutcome.fail(RejectReason.DAILY_LIMIT, "дневной убыток достигнут")
    # фильтры символа
    if not filters.is_trading:
        return GateOutcome.fail(RejectReason.SYMBOL_NOT_TRADING, filters.status)
    # money-гейты (qty/минимумы) — до уровня
    qty, reason = compute_entry_qty(
        settings.position_size_usdt, signal.price, filters
    )
    if reason is not None:
        return GateOutcome.fail(reason, "qty/notional")
    # направления уровней (Этап A черновика §2)
    if signal.side is Side.LONG:
        if not (levels.sl_price < signal.price < levels.tp2_price):
            return GateOutcome.fail(RejectReason.INVALID_LEVELS, "LONG: sl<p<tp нарушено")
    else:
        if not (levels.tp2_price < signal.price < levels.sl_price):
            return GateOutcome.fail(RejectReason.INVALID_LEVELS, "SHORT: tp<p<sl нарушено")
    # спред-гейт (опционален, [ТРЕБУЕТСЯ СВЕРКА])
    if settings.max_spread_pct is not None and signal.spread_pct > settings.max_spread_pct:
        return GateOutcome.fail(RejectReason.INVALID_SIGNAL, "spread слишком велик")
    # баланс с буфером
    if balance is not None:
        need = settings.position_size_usdt * (
            Decimal("1") + Decimal(str(settings.balance_buffer_pct)) / Decimal("100")
        )
        if balance < need:
            return GateOutcome.fail(
                RejectReason.INSUFFICIENT_BALANCE,
                f"balance={balance} < need~{need}",
            )
    return GateOutcome.passed()