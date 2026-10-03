"""Golden-тесты сопровождения: BE, трейлинг, TIMEOUT, VOL_DECAY."""
from decimal import Decimal

from trading.manage import check_position, profit_pct
from trading.settings import EngineSettings
from trading.types import ExitReason, PositionSnapshot, Side

NOW = 1_000_000


def _pos(side: Side = Side.LONG, **kw) -> PositionSnapshot:
    base = dict(
        symbol="RLCUSDT", side=side, signal_id=1, entry_ts_ms=NOW,
        entry_price=Decimal("100"), qty=Decimal("10"),
        size_usdt=Decimal("20"), score=8.0,
        sl_price=Decimal("98"), local_sl_price=Decimal("98"),
        tp1_price=Decimal("101.2"), tp2_price=Decimal("102.4"),
        iron_sl_price=Decimal("97.7"), updated_ms=NOW,
    )
    base.update(kw)
    return PositionSnapshot(**base)


S = EngineSettings()


class TestTrailingAndBe:
    def test_no_action_far_from_levels(self) -> None:
        action = check_position(_pos(), Decimal("100.5"), NOW, S)
        assert action.kind == "none"

    def test_trail_activation_moves_sl(self) -> None:
        action = check_position(_pos(), Decimal("102.1"), NOW, S)
        assert action.kind == "trail_move"
        assert action.new_local_sl is not None
        assert action.new_local_sl > Decimal("98")

    def test_trail_hit_closes_trail_sl(self) -> None:
        pos = _pos(trail_active=True, local_sl_price=Decimal("101"))
        action = check_position(pos, Decimal("100.9"), NOW, S)
        assert action.kind == "close"
        assert action.exit_reason is ExitReason.TRAIL_SL

    def test_be_hit_closes_be_sl(self) -> None:
        pos = _pos(breakeven_done=True, local_sl_price=Decimal("100"))
        action = check_position(pos, Decimal("99.9"), NOW, S)
        assert action.exit_reason is ExitReason.BE_SL

    def test_unmoved_sl_waits_exchange(self) -> None:
        # local == биржевой SL: ждём биржевой ордер, не дублируем
        action = check_position(_pos(), Decimal("97.9"), NOW, S)
        assert action.kind == "none"

    def test_trail_moves_only_tighter_long(self) -> None:
        pos = _pos(trail_active=True, local_sl_price=Decimal("101"))
        # ступень вверх: 101 * 1.004 = 101.404 — цена достигла
        action = check_position(pos, Decimal("101.5"), NOW, S)
        assert action.kind == "trail_move"
        assert action.new_local_sl == Decimal("101.404")


class TestTimeoutAndVolDecay:
    def test_timeout(self) -> None:
        action = check_position(_pos(), Decimal("100"), NOW + 7200_000, S)
        assert action.exit_reason is ExitReason.TIMEOUT

    def test_vol_decay_disabled_by_default(self) -> None:
        action = check_position(_pos(), Decimal("100"), NOW, S, volume_ratio=0.1)
        assert action.kind == "none"

    def test_vol_decay_when_enabled(self) -> None:
        s = EngineSettings(vol_decay_enabled=True)
        action = check_position(_pos(), Decimal("100.1"), NOW, s, volume_ratio=0.3)
        assert action.exit_reason is ExitReason.VOL_DECAY

    def test_short_mirror(self) -> None:
        pos = _pos(Side.SHORT, sl_price=Decimal("102"),
                   local_sl_price=Decimal("102"),
                   iron_sl_price=Decimal("102.3"))
        action = check_position(pos, Decimal("102.1"), NOW, S)
        assert action.kind == "none"  # не сдвинут — ждём биржу
        pos2 = _pos(Side.SHORT, trail_active=True,
                    local_sl_price=Decimal("99"))
        action2 = check_position(pos2, Decimal("99.1"), NOW, S)
        assert action2.exit_reason is ExitReason.TRAIL_SL