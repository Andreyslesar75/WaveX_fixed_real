"""Golden-тесты сопровождения 1:1 (сверка по position_tracker, Часть 4)."""
from decimal import Decimal

from trading.manage import (
    check_position,
)
from trading.settings import EngineSettings
from trading.types import ExitReason, PositionSnapshot, Side

NOW = 1_000_000
S = EngineSettings()


def _pos(side: Side = Side.LONG, **kw) -> PositionSnapshot:
    base = dict(
        symbol="RLCUSDT", side=side, signal_id=1, entry_ts_ms=NOW,
        entry_price=Decimal("100"), qty=Decimal("10"), size_usdt=Decimal("20"),
        score=41.0, sl_price=Decimal("98"), local_sl_price=Decimal("98"),
        tp1_price=Decimal("102"), tp2_price=Decimal("102.2"),
        iron_sl_price=Decimal("97.8"), updated_ms=NOW,
    )
    base.update(kw)
    return PositionSnapshot(**base)


class TestTrailingLadder:
    def test_ladder_0_at_3pct(self) -> None:
        # активация 3% -> ступень 0 -> отступ 4%: SL = 103*0.96 = 98.88 > 98
        action = check_position(_pos(), Decimal("103"), NOW, S)
        assert action.kind == "trail_move"
        assert action.new_local_sl == Decimal("98.88")

    def test_ladder_8_at_9pct(self) -> None:
        # 9% -> ступень 8 -> отступ 3%: SL = 109*0.97 = 105.73
        pos = _pos(trail_active=True, local_sl_price=Decimal("98.88"))
        action = check_position(pos, Decimal("109"), NOW, S)
        assert action.new_local_sl == Decimal("105.73")

    def test_ladder_no_improve_keeps_sl(self) -> None:
        # BE 100.15 выше, чем трейл 98.88 — лестница не ухудшает
        pos = _pos(breakeven_done=True, local_sl_price=Decimal("100.15"))
        action = check_position(pos, Decimal("103"), NOW, S)
        assert action.kind == "none"

    def test_trail_sl_hit_after_move(self) -> None:
        pos = _pos(trail_active=True, local_sl_price=Decimal("105.73"))
        action = check_position(pos, Decimal("105.5"), NOW, S)
        assert action.kind == "close"
        assert action.exit_reason is ExitReason.TRAIL_SL

    def test_be_sl_hit(self) -> None:
        pos = _pos(breakeven_done=True, local_sl_price=Decimal("100.15"))
        action = check_position(pos, Decimal("100"), NOW, S)
        assert action.exit_reason is ExitReason.BE_SL


class TestBreakeven:
    def test_standalone_be_at_1_5pct_with_buffer(self) -> None:
        action = check_position(_pos(), Decimal("101.6"), NOW, S)
        assert action.kind == "breakeven"
        assert action.new_local_sl == Decimal("100.15")  # 100*(1+0.0015)

    def test_be_not_before_activation(self) -> None:
        assert check_position(_pos(), Decimal("101.4"), NOW, S).kind == "none"

    def test_be_short_mirror(self) -> None:
        pos = _pos(Side.SHORT, sl_price=Decimal("102"),
                   local_sl_price=Decimal("102"))
        action = check_position(pos, Decimal("98.4"), NOW, S)
        assert action.kind == "breakeven"
        assert action.new_local_sl == Decimal("99.85")  # 100*(1-0.0015)


class TestTimeoutVolDecay:
    def test_timeout_24h(self) -> None:
        action = check_position(_pos(), Decimal("100"), NOW + 86_400_000, S)
        assert action.exit_reason is ExitReason.TIMEOUT

    def test_vol_decay_after_60min_low_ratio(self) -> None:
        pos = _pos(entry_ts_ms=NOW - 61 * 60_000)
        action = check_position(pos, Decimal("100"), NOW, S, volume_ratio=0.2)
        assert action.exit_reason is ExitReason.VOL_DECAY

    def test_vol_decay_before_60min_ignored(self) -> None:
        pos = _pos(entry_ts_ms=NOW - 30 * 60_000)
        assert check_position(pos, Decimal("100"), NOW, S, 0.1).kind == "none"

    def test_unmoved_sl_waits_exchange(self) -> None:
        assert check_position(_pos(), Decimal("97.9"), NOW, S).kind == "none"
