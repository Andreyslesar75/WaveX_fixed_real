"""Тесты гейтов: порядок 1:1, строки причин, STRONG-обход, repeat-блок."""
from decimal import Decimal

from trading.gates import GateState, check_gates
from trading.levels import CalculatedLevels
from trading.settings import EngineSettings
from trading.types import RejectReason, Side, SignalInput, SymbolFilters

LEVELS = CalculatedLevels(
    sl_price=Decimal("98"), tp1_price=Decimal("102"), tp2_price=Decimal("102.2"),
    sl_pct=Decimal("2"), tp_pct=Decimal("2"),
)


def _signal(**kw) -> SignalInput:
    base = dict(
        symbol="RLCUSDT", side=Side.LONG, price=Decimal("100"), score=41.0,
        confidence="HIGH", spread_pct=0.05, btc_trend=0.0,
        high24=110.0, low24=90.0, structural_level=None,
    )
    base.update(kw)
    return SignalInput(**base)


def _filters() -> SymbolFilters:
    return SymbolFilters(
        symbol="RLCUSDT", status="TRADING", tick_size=Decimal("0.0001"),
        step_size=Decimal("0.1"), min_qty=Decimal("0.1"),
        max_qty=Decimal("10000"), min_notional=Decimal("5"),
        price_precision=4, quantity_precision=1,
    )


class TestParity:
    def test_all_pass_qty(self) -> None:
        out = check_gates(_signal(), LEVELS, GateState(), EngineSettings(),
                          _filters(), Decimal("100"), 0)
        # size 20 USDT / цена 100 = 0.2 (floor по step 0.1 — без изменений)
        assert out.ok and out.qty == Decimal("0.2")

    def test_skip_confidence(self) -> None:
        out = check_gates(_signal(confidence="SKIP"), LEVELS, GateState(),
                          EngineSettings(), _filters(), None, 0)
        assert out.reason is RejectReason.LOW_CONFIDENCE
        assert out.detail == "low_conf_SKIP"

    def test_short_disabled(self) -> None:
        s = EngineSettings(short_trading_enabled=False)
        out = check_gates(_signal(side=Side.SHORT), LEVELS, GateState(), s,
                          _filters(), None, 0)
        assert out.reason is RejectReason.SHORT_DISABLED

    def test_score_41_boundary(self) -> None:
        out = check_gates(_signal(score=40.9), LEVELS, GateState(),
                          EngineSettings(), _filters(), None, 0)
        assert out.detail == "score_41<41"

    def test_short_threshold_adjusts_btc(self) -> None:
        s = EngineSettings()
        out = check_gates(_signal(side=Side.SHORT, score=32.9, btc_trend=-3.0),
                          LEVELS, GateState(), s, _filters(), None, 0)
        assert out.ok  # 35-3=32 <= 32.9

    def test_balance_gate_no_buffer(self) -> None:
        out = check_gates(_signal(), LEVELS, GateState(), EngineSettings(),
                          _filters(), Decimal("19.99"), 0)
        assert out.reason is RejectReason.INSUFFICIENT_BALANCE

    def test_paper_balance_none_passes(self) -> None:
        out = check_gates(_signal(), LEVELS, GateState(), EngineSettings(),
                          _filters(), None, 0)
        assert out.ok


class TestCooldowns:
    def test_sl_cooldown_blocked(self) -> None:
        state = GateState(cooldown_until_ms={"RLCUSDT": 10_000})
        out = check_gates(_signal(), LEVELS, state, EngineSettings(),
                          _filters(), None, 2_000)
        assert out.reason is RejectReason.COOLDOWN_SL
        assert out.detail == "blocked_until_0s"[:12] or out.detail.startswith("blocked_until")

    def test_strong_score_bypasses_sl_cooldown(self) -> None:
        state = GateState(cooldown_until_ms={"RLCUSDT": 10_000})
        out = check_gates(_signal(score=68.0), LEVELS, state,
                          EngineSettings(), _filters(), None, 5_000)
        assert out.ok

    def test_strong_score_not_bypass_repeat(self) -> None:
        state = GateState(repeat_block_until_ms={"RLCUSDT": 10_000})
        out = check_gates(_signal(score=90.0), LEVELS, state,
                          EngineSettings(), _filters(), None, 5_000)
        assert out.reason is RejectReason.COOLDOWN_REPEAT

    def test_daily_loss_limit(self) -> None:
        s = EngineSettings()
        state = GateState(today_realized=Decimal("-50"))
        out = check_gates(_signal(), LEVELS, state, s, _filters(), None, 0)
        assert out.reason is RejectReason.DAILY_LIMIT
