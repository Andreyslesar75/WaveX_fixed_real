#!/usr/bin/env python3
"""setup_symbol_env.py — разовая служебная утилита окружения (§4 черновика).

Выставляет: One-way (dualSidePosition=false — один раз на аккаунт),
плечо и (опционально) тип маржи по символам. Торговый бот это только
читает, никогда не меняет.

Пример:
  python scripts/setup_symbol_env.py --symbols RLCUSDT,SOLUSDT --leverage 1
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import aiohttp

from trading.binance.rest import AioHttpTransport, BinanceRestClient
from trading.ratelimit import RateLimiter

BASE = os.getenv("SETUP_TESTNET", "false").lower() == "true" and \
    "https://testnet.binancefuture.com" or "https://fapi.binance.com"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbols", required=True, help="через запятую")
    parser.add_argument("--leverage", type=int, default=1)
    parser.add_argument("--margin", choices=["ISOLATED", "CROSSED"], default=None)
    args = parser.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    key, secret = os.getenv("BINANCE_API_KEY", ""), os.getenv("BINANCE_API_SECRET", "")
    if not key or not secret:
        sys.exit("Ключи не заданы")

    session = aiohttp.ClientSession()
    

    class _Clock:
        def now_ms(self) -> int:
            return int(time.time() * 1000)

    rest = BinanceRestClient(
        transport=AioHttpTransport(session), api_key=key, secret_key=secret,
        base_url=BASE, limiter=RateLimiter(), clock=_Clock(),
    )

    print("== One-way (dualSidePosition=false) — один раз на аккаунт ==")
    dual = await rest.get_position_mode()
    if dual:
        await rest.set_dual_side(False)
        print("  переключено на One-way")
    else:
        print("  уже One-way")

    for symbol in [s.strip().upper() for s in args.symbols.split(",") if s.strip()]:
        print(f"\n== {symbol} ==")
        await rest.set_leverage(symbol, args.leverage)
        print(f"  плечо -> x{args.leverage}")
        if args.margin:
            try:
                await rest._call("POST", "/fapi/v1/marginType",
                                 {"symbol": symbol, "marginType": args.margin},
                                 signed=True, weight=1, order_action=True, retries=0)
                print(f"  маржа -> {args.margin}")
            except Exception as exc:
                print(f"  маржа: {exc} (если 'No need to change' — уже так)")
        cfg = await rest._call("GET", "/fapi/v1/symbolConfig", {"symbol": symbol},
                               signed=True, weight=1, order_action=False, retries=2)
        print(f"  проверка: {json.dumps(cfg, ensure_ascii=False)}")

    await session.close()


if __name__ == "__main__":
    asyncio.run(main())