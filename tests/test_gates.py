"""Тесты гейтов: порядок, причины, кулдауны, лимиты."""
from decimal import Decimal

from trading.gates import GateState, check_gates
from trading.levels import CalculatedLevels
from trading.settings import EngineSettings
from trading.types import RejectReason, Side, SignalInput, SymbolFilters

LEVELS = CalculatedLevels(
    sl_price=Decimal("98"), tp2_price=Decimal("103"),
    sl_pct=Decimal("2"), tp_pct=Decimal("3"),
)


def _signal(**kw) -> SignalInput:
    base = dict(
        symbol="RLCUSDT", side=Side.LONG, price=Decimal("100"),
        score=8.0, confidence="HIGH", spread_pct=0.05, btc_trend=0.0,
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


class TestOrderAndReasons:
    def test_skip_confidence(self) -> None:
        out = check_gates(_signal(confidence="SKIP"), LEVELS, GateState(),
                          EngineSettings(), _filters(), None, 0)
        assert out.reason is RejectReason.LOW_CONFIDENCE

    def test_score_threshold(self) -> None:
        out = check_gates(_signal(score=1.0), LEVELS, GateState(),
                          EngineSettings(), _filters(), None, 0)
        assert out.reason is RejectReason.SCORE_THRESHOLD

    def test_duplicate(self) -> None:
        state = GateState(open_symbols={"RLCUSDT"})
        out = check_gates(_signal(), LEVELS, state, EngineSettings(),
                          _filters(), None, 0)
        assert out.reason is RejectReason.DUPLICATE

    def test_sl_cooldown(self) -> None:
        state = GateState(last_sl_exit_ts_ms={"RLCUSDT": 1000})
        out = check_gates(_signal(), LEVELS, state, EngineSettings(),
                          _filters(), None, 1000 + 10_000)
        assert out.reason is RejectReason.COOLDOWN_SL

    def test_daily_loss_limit(self) -> None:
        s = EngineSettings(daily_max_loss_usdt=Decimal("50"))
        state = GateState(today_realized=Decimal("-50"))
        out = check_gates(_signal(), LEVELS, state, s, _filters(), None, 0)
        assert out.reason is RejectReason.DAILY_LIMIT

    def test_notional(self) -> None:
        s = EngineSettings(position_size_usdt=Decimal("1"))
        out = check_gates(_signal(), LEVELS, GateState(), s, _filters(),
                          None, 0)
        assert out.reason is RejectReason.NOTIONAL_BELOW_MIN

    def test_invalid_levels_geometry(self) -> None:
        bad = CalculatedLevels(sl_price=Decimal("101"), tp2_price=Decimal("103"),
                               sl_pct=Decimal("1"), tp_pct=Decimal("3"))
        out = check_gates(_signal(), bad, GateState(), EngineSettings(),
                          _filters(), None, 0)
        assert out.reason is RejectReason.INVALID_LEVELS

    def test_balance_gate(self) -> None:
        out = check_gates(_signal(), LEVELS, GateState(), EngineSettings(),
                          _filters(), Decimal("10"), 0)
        assert out.reason is RejectReason.INSUFFICIENT_BALANCE

    def test_all_pass(self) -> None:
        out = check_gates(_signal(), LEVELS, GateState(), EngineSettings(),
                          _filters(), Decimal("1000"), 0)
        assert out.ok and out.reason is None