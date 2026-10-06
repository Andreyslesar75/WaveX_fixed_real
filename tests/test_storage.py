"""Тесты storage: dirty-check позиций, роль ордера, win_rate, equity."""
from decimal import Decimal
from pathlib import Path

from trading.storage import (
    OrderRow,
    Storage,
    StorageReader,
    StoredPosition,
    TradeRecord,
)
from trading.types import Mode
from dataclasses import replace

POS = StoredPosition(
    symbol="RLCUSDT", side="LONG", entry_ts=1, entry_price=Decimal("100"),
    qty=Decimal("10"), size_usdt=Decimal("20"), score=8.0,
    sl_price=Decimal("98"), signal_id=1, iron_sl_price=Decimal("97.7"),
    sl_client_id="wx1-sl", updated_ms=1,
)


class TestStorage:
    def setup_method(self) -> None:
        self.storage = Storage(Path("test_v2.db"))
        self.storage.initialize()

    def teardown_method(self) -> None:
        self.storage.close()
        Path("test_v2.db").unlink(missing_ok=True)

    def test_position_upsert_dirty_check(self) -> None:
        assert self.storage.upsert_position(POS) is True   # insert
        assert self.storage.upsert_position(POS) is False  # no-change: нет записи
        changed = replace(POS, qty=Decimal("5"))
        assert self.storage.upsert_position(changed) is True
        loaded = self.storage.load_positions()
        assert len(loaded) == 1 and loaded[0].qty == Decimal("5")

    def test_order_role_lookup(self) -> None:
        row_id = self.storage.insert_order(OrderRow(
            ts_ms=1, mode=Mode.REAL, symbol="RLCUSDT",
            client_order_id="wx1-sl", side="SELL", type="STOP_MARKET",
            role="SL", qty=None, stop_price=Decimal("98"),
            reduce_only=False, close_position=True, status="NEW",
            position_ref="wx1-in",
        ))
        self.storage.update_order_status(row_id, "FILLED", exchange_order_id=777)
        assert self.storage.find_order_role_by_exchange_id(Mode.REAL, 777) == "SL"

    def test_status_update_noop(self) -> None:
        row_id = self.storage.insert_order(OrderRow(
            ts_ms=1, mode=Mode.PAPER, symbol="X", client_order_id="c1",
            side="BUY", type="MARKET", role="ENTRY", qty=Decimal("1"),
            stop_price=None, reduce_only=False, close_position=False,
            status="NEW", position_ref=None,
        ))
        assert self.storage.update_order_status(row_id, "NEW") is False
        assert self.storage.update_order_status(row_id, "FILLED") is True

    def test_trade_and_win_rate(self) -> None:
        for i, pnl in enumerate((Decimal("5"), Decimal("-2"), Decimal("3"))):
            self.storage.insert_trade(TradeRecord(
                signal_id=i, symbol="RLCUSDT", side="LONG", entry_ts=0,
                exit_ts=1000 + i, entry_price=Decimal("100"),
                exit_price=Decimal("101"), qty=Decimal("10"),
                gross_pnl=pnl, fees=Decimal("0.1"), net_pnl=pnl,
                pnl_pct=float(pnl), exit_reason="TP2", mfe=1.0, mae=0.0,
                sl_pct=2.0, tp_pct=3.0, tp1_done=True, tp2_done=True,
                breakeven_done=True,
            ))
        reader = StorageReader(Path("test_v2.db"))
        _wr, total, wins, losses = reader.get_win_rate()
        assert total == 3 and wins == 2 and losses == 1
        trades = reader.get_trades(2)
        assert len(trades) == 2 and "T" in trades[0]["exit_time"]
        reader.close()
