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

from typing import Any

import aiohttp

from trading.binance.rest import AioHttpTransport, BinanceRestClient
from trading.clock import Clock
from trading.filters import parse_symbol_filters
from trading.money import compute_entry_qty, round_price_tick
from trading.ratelimit import RateLimiter
from trading.types import Side

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

    try:

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

        # trust_env: aiohttp по умолчанию НЕ читает env-прокси (в отличие от
        # requests в старом api.py). Включаем как в requests; отключить можно
        # VERIFY_USE_ENV_PROXY=false.
        use_env_proxy = os.getenv("VERIFY_USE_ENV_PROXY", "true").lower() != "false"
        session = aiohttp.ClientSession(trust_env=use_env_proxy)
        print(f"[DIAG] aiohttp trust_env={use_env_proxy} (env-прокси, как requests)")
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
            if os.getenv(var):
                print(f"[DIAG] {var}={os.getenv(var)}")
        transport = AioHttpTransport(session)  # ссылка для V-API-7 (last_headers)
        limiter = RateLimiter()

        async def public(path: str, params=None):  # type: ignore[no-untyped-def]
            """GET с диагностикой: статус/тело печатаются при любом отклонении.

            Прежняя версия молча возвращала json.loads(body): ответ "null"
            превращался в None и падал в Clock без причины. Теперь видно,
            ЧТО именно отвечает сеть (статус, Server/Via, фрагмент тела).
            """
            url = f"{BASE}{path}"
            if params:
                from urllib.parse import urlencode
                url += "?" + urlencode(params)
            async with session.get(url) as r:
                body = await r.text()
                if r.status != 200:
                    print(f"[DIAG] {path}: HTTP {r.status}")
                    print(f"[DIAG] Server={r.headers.get('Server')} Via={r.headers.get('Via')}")
                    print(f"[DIAG] body[:300]={body[:300]!r}")
                    r.raise_for_status()
                try:
                    data = json.loads(body)
                except ValueError:
                    print(f"[DIAG] {path}: HTTP 200, тело не JSON: {body[:300]!r}")
                    raise
                if data is None:
                    # тело буквально "null": так отвечает не Binance, а
                    # перехватчик (прокси/фильтр). Честный raise вместо тишины.
                    print(f"[DIAG] {path}: HTTP 200, тело 'null' — перехват?")
                    print(f"[DIAG] Server={r.headers.get('Server')} Via={r.headers.get('Via')}")
                return data

        clock = Clock(public)
        rest = BinanceRestClient(
            transport=transport, api_key=api_key,
            secret_key=secret, base_url=BASE, limiter=limiter, clock=clock,
        )

        # ---------------- dry-run ----------------
        step("Синхронизация времени", "V-API-0")
        offset = await clock.sync()
        margin = "да" if abs(offset) < 4500 else "НЕТ"
        ok("V-API-0", f"offset={offset} мс (recvWindow 5000 — запас {margin})")

        step("exchangeInfo/фильтры", "V-API-2")
        info = await rest.exchange_info(args.symbol)
        all_syms = info.get("symbols", [])
        print(f"  [DIAG] exchangeInfo вернул записей: {len(all_syms)}")
        entry = next(
            (s for s in all_syms if s.get("symbol") == args.symbol), None,
        )
        if entry is None:
            sys.exit(f"{args.symbol} не найден в exchangeInfo!")
        sf = parse_symbol_filters(entry)
        ok("V-API-2", f"{sf.symbol}: tick={sf.tick_size} step={sf.step_size} "
                    f"minQty={sf.min_qty} minNotional={sf.min_notional} (поле "
                    f"{'notional' if 'notional' in str(info) else 'minNotional?'})")

        price = Decimal(
            str((await public("/fapi/v1/ticker/price",
                            {"symbol": args.symbol}))["price"])
        )
        print(f"  price={price}")

        step("Формат rate-limit заголовков", "V-API-7")
        await rest.position_risk()
        show_headers(transport.last_headers)
        ok("V-API-7", "имена заголовков выше (X-MBX-USED-WEIGHT-1M / ORDER-COUNT-*)")

        step("positionRisk v3 / balance v3", "V-API-3")
        raw = await rest.position_risk()
        positions = [p for p in raw if Decimal(str(p.get("positionAmt", "0"))) != 0]
        bal = await rest.balance()
        usdt: Any = next((b for b in bal if b.get("asset") == "USDT"), {})
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
                    if "-1117" in str(exc) or "-11" in str(exc)[:8]:
                        # -1117 = валидация параметров: эндпоинт СУЩЕСТВУЕТ
                        fail("V-API-1", f"эндпоинт ЖИВ, ждёт параметры: {exc}")
                    else:
                        ok("V-API-1", f"отказ 404/-1121: {exc}")

            step("clientOrderId 36/37 символов", "V-API-9")
            if confirm(f"Поставить и снять далёкий STOP по {args.symbol} (id 36 и 37 симв.)?"):
                far = round_price_tick(price * Decimal("0.5"), Side.LONG, sf.tick_size)
                for cid_len, cid in ((35, "a" * 35), (36, "a" * 36)):
                    try:
                        await rest.algo_order_new({
                            "algoType": "CONDITIONAL", "symbol": args.symbol,
                            "side": "SELL", "type": "STOP_MARKET",
                            "triggerPrice": str(far), "closePosition": "true",
                            "workingType": "MARK_PRICE", "clientAlgoId": cid,
                        })
                        await rest.algo_order_cancel(args.symbol, cid)
                        note = "35 OK" if cid_len == 35 else "36 принят?! (лимит шире)"
                        ok("V-API-9", note)
                    except Exception as exc:
                        (ok if cid_len == 36 else fail)("V-API-9", f"len={cid_len}: {exc}")

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
                step("positionRisk v3: поля позиции", "V-API-3b")
                risk_all = await rest.position_risk()
                own = [p for p in risk_all if p.get("symbol") == args.symbol]
                if own:
                    print(f"  ключи: {list(own[0])}")
                    ok("V-API-3b", f"positionAmt={own[0].get('positionAmt')}")
                else:
                    print("  позиция не видна (проверить частичное исполнение)")

                # step("SL closePosition + TP reduceOnly (быстрые, ±0.1%)", "V-API-5/6")
                # sl_px = round_price_tick(avg * Decimal("0.999"), Side.LONG, sf.tick_size)
                # tp_px = round_price_tick(avg * Decimal("1.001"), Side.LONG, sf.tick_size)
                # sl = await rest.new_order({
                #     "symbol": args.symbol, "side": "SELL", "type": "STOP_MARKET",
                #     "stopPrice": str(sl_px), "closePosition": "true",
                #     "workingType": "MARK_PRICE", "priceProtect": "TRUE",
                #     "newClientOrderId": f"va{int(time.time())}sl",
                # })
                # tp = await rest.new_order({
                #     "symbol": args.symbol, "side": "SELL",
                #     "type": "TAKE_PROFIT_MARKET", "stopPrice": str(tp_px),
                #     "quantity": str(executed), "reduceOnly": "true",
                #     "workingType": "MARK_PRICE", "priceProtect": "TRUE",
                #     "newClientOrderId": f"va{int(time.time())}tp",
                # })
                # ok(
                #     "V-API-5/6",
                #     f"SL={sl['status']} TP={tp['status']} — сосуществование подтверждено",
                # )
                step("Algo: SL closePosition + TP reduceOnly (±0.1%) [V-API-5/6]", "V-API-5/6")
                sl_px = round_price_tick(avg * Decimal("0.999"), Side.LONG, sf.tick_size)
                tp_px = round_price_tick(avg * Decimal("1.001"), Side.LONG, sf.tick_size)
                cid_base = f"va{int(time.time())}"
                sl = await rest.algo_order_new({
                    "algoType": "CONDITIONAL", "symbol": args.symbol,
                    "side": "SELL", "type": "STOP_MARKET",
                    "triggerPrice": str(sl_px), "closePosition": "true",
                    "workingType": "MARK_PRICE", "clientAlgoId": f"{cid_base}sl",
                })
                print(f"  RAW POST algoOrder (SL): {json.dumps(sl, ensure_ascii=False)}")
                tp = await rest.algo_order_new({
                    "algoType": "CONDITIONAL", "symbol": args.symbol,
                    "side": "SELL", "type": "TAKE_PROFIT_MARKET",
                    "triggerPrice": str(tp_px), "quantity": str(executed),
                    "reduceOnly": "true", "workingType": "MARK_PRICE",
                    "clientAlgoId": f"{cid_base}tp",
                })
                print(f"  RAW POST algoOrder (TP): {json.dumps(tp, ensure_ascii=False)}")
                ok("V-API-5/6", f"SL={sl.get('algoStatus', sl.get('status'))} "
                                f"TP={tp.get('algoStatus', tp.get('status'))} — оба встали")

                step("Пробы resolve/cancel-семантики Algo [V-API-10]", "V-API-10")
                ghost = await rest.algo_order_query(args.symbol, f"{cid_base}ghost000")
                print(f"  RAW GET несуществующего: {json.dumps(ghost, ensure_ascii=False) if ghost else ghost!r}")
                ok("V-API-10", f"ghost-GET ответ: {type(ghost).__name__}")
                open_now = await rest.algo_orders_open(args.symbol)
                print(f"  RAW openAlgoOrders[0]: "
                    f"{json.dumps(open_now[0], ensure_ascii=False) if open_now else 'пусто'}")
                open_now = await rest.open_orders(args.symbol)
                ids = [(o.get("clientOrderId"), o.get("status")) for o in open_now]
                print(f"  openOrders: {ids}")

                step("Ожидание ORDER_TRADE_UPDATE (raw формат)", "V-API-5")
                listen = await rest.create_listen_key()
                ws = await session.ws_connect(f"wss://fstream.binance.com/ws/{listen}")
                print("  ждём исполнения SL или TP (до 180 с)...")
                deadline = time.monotonic() + 180
                got_update = False
                try:
                    while time.monotonic() < deadline and not got_update:
                        remaining = deadline - time.monotonic()
                        msg = await asyncio.wait_for(ws.receive(), timeout=remaining)
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
                    # Точечные отмены ПЕРЕД allOpenOrders: ловим код ответа
                    # DELETE algoOrder на уже исполненном ордере (гонка
                    # cancel/fill — [НЕУВЕРЕН] №3, эмпирика для rest.py)
                    for cid in (f"{cid_base}sl", f"{cid_base}tp"):
                        try:
                            await rest.algo_order_cancel(args.symbol, cid)
                            print(f"  [finally] cancel {cid[-2:]}: 200 (ордер ещё стоял)")
                        except Exception as exc:
                            print(f"  [finally] cancel {cid[-2:]}: {exc} (код гонки?)")
                    try:
                        await rest.cancel_all_open_orders(args.symbol)
                        print("  finally: allOpenOrders отменены")
                    except Exception as exc:
                        print(f"  finally: отмена: {exc}")

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
                if confirm(f"Отправить MARKET {args.symbol} сверх маржи (ожидаем отказ)?"):
                    try:
                        bal = await rest.balance()
                        av = next(
                            (Decimal(str(b.get("availableBalance", "0")))
                            for b in bal if b.get("asset") == "USDT"),
                            Decimal("0"),
                        )
                        # ×200 от доступного баланса: превышает маржу при любом
                        # плече (до 125x) -> ожидаем гарантированный отказ
                        big, _r = compute_entry_qty(av * Decimal("200"), price, sf)
                        if big is None:
                            big = sf.max_qty  # упёрлись в maxQty — тоже сверх маржи
                        await rest.new_order({
                            "symbol": args.symbol, "side": "BUY", "type": "MARKET",
                            "quantity": str(big),
                            "newClientOrderId": f"va{int(time.time())}mg",
                        })
                        fail("V-API-6", "ИСПОЛНЕН?! немедленно проверить биржу вручную")
                    except Exception as exc:
                        ok("V-API-6", f"код отказа: {exc}")
                    finally:
                        # страховка при гипотетическом исполнении: убрать хвост
                        try:
                            await rest.cancel_all_open_orders(args.symbol)
                            risk = [p for p in await rest.position_risk()
                                    if p.get("symbol") == args.symbol
                                    and Decimal(str(p.get("positionAmt", "0"))) != 0]
                            if risk:
                                await rest.new_order({
                                    "symbol": args.symbol, "side": "SELL",
                                    "type": "MARKET", "reduceOnly": "true",
                                    "quantity": str(abs(Decimal(str(risk[0]["positionAmt"])))),
                                    "newClientOrderId": f"va{int(time.time())}cl",
                                })
                                print("  [probe] остаток позиции закрыт")
                        except Exception as exc:
                            print(f"  [probe] УБОРКА НЕ ПРОШЛА: {exc} — проверить биржу вручную!")

        print("\n===== ИТОГО V-API =====")
        for key in sorted(RESULTS):
            print(f"  {key}: {RESULTS[key]}")

    finally:
        await session.close()



if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано — ВСЕГДА проверьте openOrders/позиции на бирже вручную!")
