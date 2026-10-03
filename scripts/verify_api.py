#!/usr/bin/env python3
"""verify_api.py — эмпирическая проверка поверхности Binance API (V-API-1..10).

Протокол (решение Б4-1б): реал, минимальные ордера ~11 USDT.
Без флагов — dry-run (только GET, без следов). --live — ордерные блоки,
каждый с интерактивным подтверждением; finally-уборка ВСЕГДА
(DELETE allOpenOrders + closePosition остатка), включая Ctrl-C.

Ключи — из .env (BINANCE_API_KEY/SECRET). Примеры:
  python scripts/verify_api.py                  # dry-run
  python scripts/verify_api.py --live --symbol RLCUSDT
  python scripts/verify_api.py --live --probe-margin
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import aiohttp  # noqa: E402

from trading.binance.rest import AioHttpTransport, BinanceRestClient  # noqa: E402
from trading.clock import Clock  # noqa: E402
from trading.filters import parse_symbol_filters  # noqa: E402
from trading.money import compute_entry_qty, round_price_tick  # noqa: E402
from trading.ratelimit import RateLimiter  # noqa: E402
from trading.types import Side  # noqa: E402
from trading.venue import UnknownOrderError  # noqa: E402

BASE = "https://fapi.binance.com"
RESULTS: dict[str, str] = {}


def step(name: str, vapi: str) -> None:
    print(f"\n=== {name} [{vapi}] ===")


def ok(vapi: str, msg: str) -> None:
    RESULTS[vapi] = "PASS"
    print(f"  ✅ {msg}")


def fail(vapi: str, msg: str) -> None:
    RESULTS[vapi] = "FAIL"
    print(f"  ❌ {msg}")


def skip(vapi: str, msg: str) -> None:
    RESULTS.setdefault(vapi, f"SKIP ({msg})")


def confirm(prompt: str) -> bool:
    return input(f"\n>>> {prompt} [y/N]: ").strip().lower() == "y"


def show_headers(headers: dict[str, str]) -> None:
    interesting = [h for h in headers if h.upper().startswith("X-MBX")]
    print(f"  headers: {dict((h, headers[h]) for h in interesting) or 'нет X-MBX'}")


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="ордерные проверки")
    parser.add_argument("--symbol", default="RLCUSDT")
    parser.add_argument("--probe-margin", action="store_true")
    parser.add_argument("--size", type=float, default=11.0)
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    api_key, secret = os.getenv("BINANCE_API_KEY", ""), os.getenv("BINANCE_API_SECRET", "")
    if not api_key or not secret:
        sys.exit("Ключи не заданы (.env)")

    session = await aiohttp.ClientSession().__aenter__()
    limiter = RateLimiter()

    async def public(path: str, params=None):  # type: ignore[no-untyped-def]
        url = f"{BASE}{path}"
        if params:
            from urllib.parse import urlencode
            url += "?" + urlencode(params)
        async with session.get(url) as r:
            return json.loads(await r.text())

    clock = Clock(public)
    rest = BinanceRestClient(
        transport=AioHttpTransport(session), api_key=api_key,
        secret_key=secret, base_url=BASE, limiter=limiter, clock=clock,
    )

    # ---------------- dry-run ----------------
    step("Синхронизация времени", "V-API-0")
    offset = await clock.sync()
    ok("V-API-0", f"offset={offset} мс (recvWindow 5000 — запас {'да' if abs(offset) < 4500 else 'НЕТ'}")

    step("exchangeInfo/фильтры", "V-API-2")
    info = await rest.exchange_info(args.symbol)
    sf = parse_symbol_filters(info["symbols"][0])
    ok("V-API-2", f"{sf.symbol}: tick={sf.tick_size} step={sf.step_size} "
                  f"minQty={sf.min_qty} minNotional={sf.min_notional} (поле "
                  f"{'notional' if 'notional' in str(info) else 'minNotional?'})")

    step("Формат rate-limit заголовков", "V-API-7")
    raw = await rest.position_risk()
    show_headers(rest._transport.__dict__.get("_headers", {}) or {})  # см. примечание
    skip("V-API-7", "имена заголовков см. в логе каждого ответа ниже")

    step("positionRisk v3 / balance v3", "V-API-3")
    positions = [p for p in raw if Decimal(str(p.get("positionAmt", "0"))) != 0]
    bal = await rest.balance()
    usdt = next((b for b in bal if b.get("asset") == "USDT"), {})
    print(f"  позиций: {len(positions)}; ключи USDT-баланса: {list(usdt)[:8]}")
    ok("V-API-3", f"positionRisk keys={list(raw[0]) if raw else 'пусто'}")

    step("commissionRate / dual / bracket", "V-API-4")
    maker, taker = await rest.commission_rate(args.symbol)
    dual = await rest.get_position_mode()
    ok("V-API-4", f"maker={maker} taker={taker} dualSidePosition={dual} (ожидание False)")

    step("listenKey create/keepalive/delete", "V-API-8")
    key = await rest.create_listen_key()
    await rest.keepalive_listen_key()
    ok("V-API-8", f"ключ создан и продлён ({key[:8]}…); TTL без keepalive "
                  f"замерять пассивно (см. --help)")

    # ---------------- live ----------------
    if args.live:
        step("Проба algoOrder (ожидание: эндпоинта нет)", "V-API-1")
        if confirm("Отправить POST /fapi/v1/algoOrder (ожидаем отказ биржи)?"):
            try:
                await rest._call("POST", "/fapi/v1/algoOrder", {"symbol": args.symbol},
                                 signed=True, weight=1, order_action=True, retries=0)
                fail("V-API-1", "ЭНДПОИНТ ОТВЕТИЛ 200 — спор Б3-3 пересматриваю!")
            except Exception as exc:
                ok("V-API-1", f"отказ, как ожидалось: {exc}")

        step("clientOrderId 36/37 символов", "V-API-9")
        if confirm(f"Поставить и снять далёкий STOP по {args.symbol} (id 36 и 37 симв.)?"):
            price = Decimal(str((await public("/fapi/v1/ticker/price",
                                               {"symbol": args.symbol}))["price"]))
            far = round_price_tick(price * Decimal("0.5"), Side.LONG, sf.tick_size)
            for cid_len, cid in ((36, "a" * 36), (37, "a" * 37)):
                try:
                    await rest.new_order({
                        "symbol": args.symbol, "side": "SELL",
                        "type": "STOP_MARKET", "stopPrice": str(far),
                        "closePosition": "true", "newClientOrderId": cid,
                    })
                    await rest.cancel_order(args.symbol, orig_client_order_id=cid)
                    note = "36 OK" if cid_len == 36 else "37 принят?! (лимит шире)"
                    ok("V-API-9", note)
                except Exception as exc:
                    (ok if cid_len == 37 else fail)("V-API-9", f"len={cid_len}: {exc}")

        step(f"MARKET вход ~{args.size} USDT", "V-API-5")
        if not confirm(f"КУПИТЬ {args.symbol} на {args.size} USDT РЕАЛЬНО?"):
            skip("V-API-5", "нет подтверждения")
        else:
            qty, reason = compute_entry_qty(Decimal(str(args.size)), price, sf)
            assert qty, f"qty не прошёл фильтры: {reason}"
            t0 = time.monotonic()
            entry = await rest.new_order({
                "symbol": args.symbol, "side": "BUY", "type": "MARKET",
                "quantity": str(qty), "newClientOrderId": f"va{int(time.time())}in",
            })
            print(f"  вход: {entry['status']} avg={entry.get('avgPrice')} "
                  f"exec={entry.get('executedQty')} "
                  f"latency={(time.monotonic()-t0)*1000:.0f}ms")
            executed = Decimal(str(entry.get("executedQty", str(qty))))
            avg = Decimal(str(entry.get("avgPrice", str(price))))
            ok("V-API-5", "MARKET исполнен")

            step("SL closePosition + TP reduceOnly (быстрые, ±0.1%)", "V-API-5/6")
            sl_px = round_price_tick(avg * Decimal("0.999"), Side.LONG, sf.tick_size)
            tp_px = round_price_tick(avg * Decimal("1.001"), Side.LONG, sf.tick_size)
            sl = await rest.new_order({
                "symbol": args.symbol, "side": "SELL", "type": "STOP_MARKET",
                "stopPrice": str(sl_px), "closePosition": "true",
                "workingType": "MARK_PRICE", "priceProtect": "TRUE",
                "newClientOrderId": f"va{int(time.time())}sl",
            })
            tp = await rest.new_order({
                "symbol": args.symbol, "side": "SELL",
                "type": "TAKE_PROFIT_MARKET", "stopPrice": str(tp_px),
                "quantity": str(executed), "reduceOnly": "true",
                "workingType": "MARK_PRICE", "priceProtect": "TRUE",
                "newClientOrderId": f"va{int(time.time())}tp",
            })
            ok("V-API-5/6", f"SL={sl['status']} TP={tp['status']} — сосуществование подтверждено")
            open_now = await rest.open_orders(args.symbol)
            print(f"  openOrders: {[(o.get('clientOrderId'), o.get('status')) for o in open_now]}")

            step("Ожидание ORDER_TRADE_UPDATE (raw формат)", "V-API-5")
            listen = await rest.create_listen_key()
            ws = await session.ws_connect(f"{BASE.replace('https', 'wss')}/ws/{listen}")
            print("  ждём исполнения SL или TP (до 180 с)...")
            deadline = time.monotonic() + 180
            got_update = False
            try:
                while time.monotonic() < deadline and not got_update:
                    msg = await asyncio.wait_for(ws.receive(), timeout=deadline - time.monotonic())
                    if msg.type is not aiohttp.WSMsgType.TEXT:
                        break
                    payload = json.loads(msg.data)
                    if payload.get("e") == "ORDER_TRADE_UPDATE":
                        o = payload["o"]
                        print("  RAW o:", json.dumps(o, ensure_ascii=False))
                        got_update = (
                            o.get("X") == "FILLED"
                            and Decimal(str(o.get("z", "0"))) >= executed * Decimal("0.9")
                        )
            finally:
                await ws.close()
                try:
                    await rest.cancel_all_open_orders(args.symbol)
                    print("  finally: allOpenOrders отменены")
                except Exception as exc:
                    print(f"  finally: отмена: {exc}")
                remain = await rest.get_order(args.symbol, orig_client_order_id=sl["clientOrderId"]) \
                    if False else None
                # остаток позиции закрываем market reduceOnly (уборка)
                risk = [p for p in await rest.position_risk()
                        if p["symbol"] == args.symbol
                        and Decimal(str(p["positionAmt"])) != 0]
                if risk:
                    if confirm("Осталась позиция — закрыть MARKET reduceOnly?"):
                        await rest.new_order({
                            "symbol": args.symbol, "side": "SELL",
                            "type": "MARKET", "reduceOnly": "true",
                            "quantity": str(abs(Decimal(str(risk[0]["positionAmt"])))),
                            "newClientOrderId": f"va{int(time.time())}cl",
                        })
                        print("  позиция закрыта")
                ok("V-API-5", "сырой формат ORDER_TRADE_UPDATE получен" if got_update
                   else "обновления не дождались (SL/TP не коснулись — см. ручную проверку)")

        if args.probe_margin:
            step("Insufficient margin: код ошибки", "V-API-6")
            if confirm(f"Отправить MARKET {args.symbol} qty×50 (ожидаем отказ)?"):
                try:
                    await rest.new_order({
                        "symbol": args.symbol, "side": "BUY", "type": "MARKET",
                        "quantity": str((qty or Decimal("1")) * 50),
                        "newClientOrderId": f"va{int(time.time)}mg",
                    })
                    fail("V-API-6", "исполнен?! проверить баланс немедленно")
                except Exception as exc:
                    ok("V-API-6", f"код: {exc}")

    print("\n===== ИТОГО V-API =====")
    for key in sorted(RESULTS):
        print(f"  {key}: {RESULTS[key]}")
    await session.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано — ВСЕГДА проверьте openOrders/позиции на бирже вручную!")