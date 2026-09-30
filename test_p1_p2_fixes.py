#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# ФАЙЛ: test_p1_p2_fixes.py
# СОХРАНИТЬ КАК: test_p1_p2_fixes.py

Проверки правок П1 и П2 (Вариант 2 + 3):
  П1: _validate_order_side + side в retry-пересборке после -1013;
  П2: диспетчеризация ORDER_TRADE_UPDATE без TypeError,
      очередь событий ws_pending_events, дренаж в update_positions,
      защита от двойного закрытия (флаг closing).

Без сети, без API-ключей, без БД — все внешние зависимости замоканы.
Тесты регрессионные: на коде ДО правок падают, ПОСЛЕ — проходят.

Запуск:
    python test_p1_p2_fixes.py

Код выхода 1 — если есть FAIL (удобно для CI).
"""

import asyncio
import sys
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from api import BinanceFuturesRestClient, FilterFailureError
from position_tracker import PositionTracker
from risk_manager import PositionManager
from ws_client import BinanceWsClient

# ----------------------------------------------------------------
# Мини-фреймворк в стиле репо (plain asyncio, без pytest)
# ----------------------------------------------------------------
RESULTS: list = []   # [(имя, bool)]
TESTS: list = []     # [(имя, coroutine_fn)]


def test(name: str):
    """Регистрирует тест-функцию."""
    def deco(fn):
        TESTS.append((name, fn))
        return fn
    return deco


def check(name: str, ok: bool) -> None:
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}")
    RESULTS.append((name, ok))


# ----------------------------------------------------------------
# Хелперы
# ----------------------------------------------------------------
def make_rest_client() -> BinanceFuturesRestClient:
    """REST-клиент с фиктивными ключами. Сеть не используется — все
    сетевые методы замокаются в тестах."""
    return BinanceFuturesRestClient(
        api_key="test-key",
        api_secret="test-secret",
        session=None,
    )


def make_position() -> dict:
    """Минимально полная позиция для handle_order_update/_close_position."""
    return {
        "symbol": "SOL_USDT", "side": "SHORT",
        "entry_price": 100.0, "entry_time": 1.0,
        "quantity": 1.0, "remaining_qty": 1.0,
        "sl_price": 110.0, "tp1_price": 90.0, "tp2_price": 80.0,
        "sl_order_id": 111, "sl_client_id": "wavexsl1",   # int! (см. правку П2-типы)
        "tp_order_id": 222, "tp_client_id": "wavextp2",
        "tp1_done": True, "tp2_done": False,
        "closing": False, "size_usdt": 20.0, "score": 10.0,
        "mfe": 0.0, "mae": 0.0, "sl_pct": 10.0, "tp1_pct": 10.0,
        "entry_order_id": 1, "client_order_id": "wavexentry",
        "highest": 100.0, "trail_active": False, "breakeven_set": False,
        "tp1_closed_qty": 0.0, "realized_pnl": 0.0, "closed_qty": 0.0,
        "last_watch_price": 100.0,
    }


async def run_open_with_filter_failure(kind: str) -> dict:
    """
    Прогоняет открытие позиции (kind="buy"/"sell") через симуляцию
    -1013 Filter failure на ПЕРВОМ POST-ордере:
      1-й POST -> FilterFailureError, 2-й (retry) -> успешный ответ.
    Возвращает {"order": ответ метода, "posts": параметры всех POST}.
    """
    client = make_rest_client()
    # Все несетевые зависимости — на моки
    client.ensure_leverage = AsyncMock(return_value=True)
    client._wait_order_fill = AsyncMock(side_effect=lambda s, c, o: o)  # identity
    client._round_qty = AsyncMock(return_value=0.2)
    client._get_symbol_info = AsyncMock(return_value={
        "minNotional": 5.0, "stepSize": 0.001, "tickSize": 0.0001,
        "minQty": 0.0, "maxQty": 1e9, "pricePrecision": 4,
        "quantityPrecision": 3, "status": "TRADING", "market_lot_size": None,
    })
    client.filters_cache.refresh_symbol = AsyncMock(return_value=True)

    posts: list = []

    async def fake_request(method, path, params=None, signed=False, is_order=False):
        if path == "/fapi/v1/ticker/price":
            return {"price": "100.0"}
        if method == "POST" and path == "/fapi/v1/order":
            posts.append(dict(params))
            if len(posts) == 1:
                # Первый ордер — симуляция -1013
                raise FilterFailureError(params.get("symbol", "SOLUSDT"))
            return {  # Retry — успешный ответ биржи
                "symbol": params.get("symbol"),
                "orderId": 777,
                "clientOrderId": params.get("newClientOrderId"),
                "status": "FILLED",
                "executedQty": "0.2",
                "cumQuote": "20.0",
                "avgPrice": "100.0",
            }
        return None

    client._request = fake_request

    method = client.place_market_sell_open if kind == "sell" else client.place_market_buy
    order = await method("SOL_USDT", 20.0)
    return {"order": order, "posts": posts}


# ----------------------------------------------------------------
# П1: барьер _validate_order_side
# ----------------------------------------------------------------
@test("П1: _validate_order_side — совпадение направления и регистр")
async def t_validate_ok():
    client = make_rest_client()
    assert client._validate_order_side({"side": "BUY"}, "BUY") is True
    assert client._validate_order_side({"side": "SELL"}, "SELL") is True
    # Регистр не важен (проверка приводит к upper)
    assert client._validate_order_side({"side": "buy"}, "BUY") is True


@test("П1: _validate_order_side — чужой/отсутствующий side блокируется")
async def t_validate_block():
    client = make_rest_client()
    assert client._validate_order_side({"side": "SELL"}, "BUY") is False
    assert client._validate_order_side({"side": "BUY"}, "SELL") is False
    assert client._validate_order_side({}, "BUY") is False  # ключа нет


# ----------------------------------------------------------------
# П1: side в retry-пересборке после -1013 (главный регрессионный тест)
# ----------------------------------------------------------------
@test("П1: retry после -1013 отправляет SELL (place_market_sell_open)")
async def t_retry_sell_side():
    res = await run_open_with_filter_failure("sell")
    posts = res["posts"]
    assert len(posts) == 2, f"ожидались 2 POST (первый -1013 + retry), получено {len(posts)}"
    assert posts[0]["side"] == "SELL", f"1-й POST side={posts[0]['side']}"
    # КЛЮЧЕВОЙ assert: на старом коде здесь "BUY" — тест красный
    assert posts[1]["side"] == "SELL", f"RETRY side={posts[1]['side']} (баг П1!)"
    # clientOrderId на retry должен быть новым
    assert posts[1]["newClientOrderId"] != posts[0]["newClientOrderId"]
    assert res["order"] is not None and res["order"].get("filled_amount", 0) > 0


@test("П1: retry после -1013 отправляет BUY (place_market_buy, контроль)")
async def t_retry_buy_side():
    res = await run_open_with_filter_failure("buy")
    posts = res["posts"]
    assert len(posts) == 2
    assert posts[0]["side"] == "BUY"
    assert posts[1]["side"] == "BUY"
    assert res["order"] is not None and res["order"].get("filled_amount", 0) > 0


# ----------------------------------------------------------------
# П2/В1: ORDER_TRADE_UPDATE доходит до tracker без TypeError
# ----------------------------------------------------------------
@test("П2: ORDER_TRADE_UPDATE диспетчеризируется в tracker корректно")
async def t_ws_dispatch():
    ws = BinanceWsClient.__new__(BinanceWsClient)  # без __init__ — он не нужен
    ws._tracker = MagicMock()
    ws._tracker.handle_order_update = AsyncMock()
    ws._tracker.positions = {"SOL_USDT": {"symbol": "SOL_USDT"}}
    ws._last_user_data_time = 0.0

    msg = {
        "e": "ORDER_TRADE_UPDATE",
        "o": {
            "s": "SOLUSDT",
            "i": 111,            # int — как реально шлёт Binance
            "c": "wavexsl1",     # str
            "X": "FILLED",
            "z": "0.5",
            "q": "0.5",
        },
    }
    await ws._handle_user_data(msg)

    # На старом коде здесь TypeError (5 аргументов при сигнатуре из 2)
    ws._tracker.handle_order_update.assert_awaited_once()
    args = ws._tracker.handle_order_update.await_args.args
    assert args == ("SOL_USDT", 111, "wavexsl1", "FILLED", 0.5), f"args={args}"


# ----------------------------------------------------------------
# П2/В2: события закрытия из WS-пути попадают в очередь
# ----------------------------------------------------------------
@test("П2: SL FILLED через WS — событие уходит в очередь")
async def t_sl_filled_queues_event():
    tr = PositionTracker(exchange=SimpleNamespace(), db=None)
    tr.positions["SOL_USDT"] = make_position()
    fake_event = {"symbol": "SOL_USDT", "reason": "SL", "pnl": -10.0}
    tr._close_position = AsyncMock(return_value=fake_event)

    await tr.handle_order_update("SOL_USDT", 111, "wavexsl1", "FILLED", 1.0)

    assert tr.ws_pending_events == [fake_event], \
        f"очередь={tr.ws_pending_events}"


@test("П2: TP2 FILLED через WS — событие в очереди")
async def t_tp2_filled_queues_event():
    tr = PositionTracker(exchange=SimpleNamespace(), db=None)
    tr.positions["SOL_USDT"] = make_position()
    fake_event = {"symbol": "SOL_USDT", "reason": "TP2", "pnl": 10.0}
    tr._close_position = AsyncMock(return_value=fake_event)

    await tr.handle_order_update("SOL_USDT", 222, "wavextp2", "FILLED", 1.0)

    assert tr.ws_pending_events == [fake_event]


@test("П2: TP1 FILLED — событие НЕ ставится (обработает position_watcher)")
async def t_tp1_not_queued():
    tr = PositionTracker(exchange=SimpleNamespace(), db=None)
    pos = make_position()
    pos["tp1_done"] = False  # TP1 ещё не закрыт — значит это TP1-ордер
    tr.positions["SOL_USDT"] = pos
    tr._close_position = AsyncMock()

    await tr.handle_order_update("SOL_USDT", 222, "wavextp2", "FILLED", 0.6)

    tr._close_position.assert_not_awaited()
    assert tr.ws_pending_events == []


@test("П2: SL CANCELED через WS — запускает аварийную ветку")
async def t_sl_canceled_emergency():
    tr = PositionTracker(exchange=SimpleNamespace(), db=None)
    tr.positions["SOL_USDT"] = make_position()
    tr._emergency_restore_sl = AsyncMock()

    await tr.handle_order_update("SOL_USDT", 111, "wavexsl1", "CANCELED", 0.0)

    tr._emergency_restore_sl.assert_awaited_once()


# ----------------------------------------------------------------
# П2/В2в: дренаж очереди в PositionManager.update_positions
# ----------------------------------------------------------------
@test("П2: update_positions дренит WS-очередь ДО вызова update_prices")
async def t_drain_queue():
    pm = PositionManager.__new__(PositionManager)  # без __init__: без БД/биржи
    pm._update_lock = asyncio.Lock()

    ev1 = {"symbol": "SOL_USDT", "reason": "SL", "pnl": -1.0}
    ev2 = {"symbol": "BTC_USDT", "reason": "TP2", "pnl": 2.0}
    calls: list = []

    async def fake_update_prices(prices):
        calls.append("update_prices")
        return []

    async def fake_handle(event):
        calls.append(("handle", event["symbol"], event["reason"]))

    pm.tracker = SimpleNamespace(
        ws_pending_events=[ev1, ev2],
        update_prices=fake_update_prices,
    )
    pm._handle_position_event = fake_handle

    await pm.update_positions({})

    assert calls == [
        ("handle", "SOL_USDT", "SL"),
        ("handle", "BTC_USDT", "TP2"),
        "update_prices",
    ], f"порядок вызовов: {calls}"
    assert pm.tracker.ws_pending_events == [], "очередь должна опустеть"


# ----------------------------------------------------------------
# П2: защита от двойного закрытия (гонка WS vs REST)
# ----------------------------------------------------------------
@test("П2: повторное _close_position не порождает второе событие")
async def t_no_double_close():
    exchange = MagicMock()
    # Ветка «позиция уже закрыта на бирже» (has_position=False)
    exchange.get_position_info = AsyncMock(return_value={"position_amt": 0})
    exchange.get_user_trades = AsyncMock(return_value=[])
    exchange.cancel_sl_tp = AsyncMock(return_value={"sl": True, "tp": True})

    tr = PositionTracker(exchange=exchange, db=None)
    tr.positions["SOL_USDT"] = make_position()

    ev1 = await tr._close_position("SOL_USDT", 95.0, "SL")
    ev2 = await tr._close_position("SOL_USDT", 95.0, "SL")

    assert ev1 is not None and ev1.get("reason") == "SL", f"ev1={ev1}"
    assert ev2 is None, f"второй вызов должен вернуть None, получено {ev2}"
    assert "SOL_USDT" not in tr.positions


# ----------------------------------------------------------------
# Запуск
# ----------------------------------------------------------------
async def main() -> None:
    print("=" * 64)
    print("ПРОВЕРКИ ПРАВОК П1 / П2 (Вариант 2 + 3)")
    print("без сети, без ключей, без БД")
    print("=" * 64)

    for name, fn in TESTS:
        try:
            await fn()
            check(name, True)
        except AssertionError as e:
            print(f"        assert: {e}")
            check(name, False)
        except Exception as e:
            print(f"        неожиданная ошибка: {type(e).__name__}: {e}")
            check(name, False)

    print("\n" + "=" * 64)
    fails = [n for n, ok in RESULTS if not ok]
    print(f"ИТОГ: {len(RESULTS) - len(fails)}/{len(RESULTS)} PASS")
    if fails:
        print("ПРОВАЛЕНЫ:")
        for n in fails:
            print(f"  - {n}")
    print("=" * 64)
    sys.exit(1 if fails else 0)


if __name__ == "__main__":
    asyncio.run(main())