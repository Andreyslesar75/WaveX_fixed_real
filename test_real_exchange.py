#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# ФАЙЛ: test_real_exchange.py
# СОХРАНИТЬ КАК: test_real_exchange.py

Тест RealExchange через единый интерфейс ExchangeAdapter.
Проверяет, что все методы адаптера корректно работают с реальной биржей.

ВАЖНО:
- Использует МИНИМАЛЬНЫЕ объёмы (~10 USDT)
- Требует API-ключей в config.py
- Перед запуском убедитесь, что понимаете, что делает программа
"""
import asyncio
import aiohttp
from config import Config
from api import BinanceFuturesRestClient
from position_manager import PositionManager
from exchange_adapter import RealExchange


async def main():
    # ====== БЛОК ПОДТВЕРЖДЕНИЯ ======
    print("=" * 60)
    print("ВНИМАНИЕ: ТЕСТ RealExchange (РЕАЛЬНАЯ БИРЖА)")
    print("=" * 60)
    print("Эта программа будет:")
    print("  1. Открывать реальную позицию (~10 USDT)")
    print("  2. Ставить SL и TP через адаптер")
    print("  3. Проверять статус SL через адаптер")
    print("  4. Отменять SL и TP через адаптер")
    print("  5. Закрывать позицию через адаптер")
    print()

    confirm = input("Продолжить? (да/нет): ").strip().lower()
    if confirm != "да":
        print("Отменено пользователем.")
        return

    test_symbol = input(
        "Введите символ для теста (например, RLC_USDT): "
    ).strip()
    if not test_symbol:
        test_symbol = "RLC_USDT"

    # ====== ИНИЦИАЛИЗАЦИЯ ======
    async with aiohttp.ClientSession() as session:
        api = BinanceFuturesRestClient(
            api_key=Config.BINANCE_API_KEY,
            api_secret=Config.BINANCE_API_SECRET,
            session=session,
        )
        pm = PositionManager(api)
        
        # Создаём RealExchange — наш адаптер
        exchange = RealExchange(api, pm)

        # [1] Баланс
        print("\n[1] Баланс через адаптер:")
        balance = await exchange.get_balance("USDT")
        print(f"  ✓ Доступно: {balance:.4f} USDT")

        # [2] Открытие LONG-позиции
        print(f"\n[2] Открытие LONG-позиции по {test_symbol} через адаптер...")
        order = await exchange.place_market_buy(test_symbol, 10.0)
        if not order or order.get("filled_amount", 0) <= 0:
            print("  ✗ ОШИБКА: не удалось открыть позицию")
            return
        
        filled_qty = order.get("filled_amount")
        avg_price = order.get("avg_price")
        print(f"  ✓ Позиция открыта: qty={filled_qty}, avg_price={avg_price}")

        await asyncio.sleep(1.5)

        # [3] Количество в позиции через адаптер
        print("\n[3] Количество в позиции через адаптер:")
        qty = await exchange.get_position_qty(test_symbol)
        print(f"  ✓ qty={qty}")
        if qty <= 0:
            print("  ✗ ОШИБКА: позиция не найдена")
            return

        # [4] Постановка SL через адаптер
        sl_price = round(avg_price * 0.98, 8)  # -2%
        print(f"\n[4] Постановка SL через адаптер (цена={sl_price})...")
        sl_order = await exchange.place_sl(test_symbol, sl_price)
        if sl_order:
            print(f"  ✓ SL установлен: order_id={sl_order.get('order_id')}")
        else:
            print("  ✗ ОШИБКА: SL не установлен")
            return

        await asyncio.sleep(1.0)

        # [5] Постановка TP через адаптер
        tp_price = round(avg_price * 1.03, 8)  # +3%
        print(f"\n[5] Постановка TP через адаптер (цена={tp_price}, qty={qty})...")
        tp_order = await exchange.place_tp(test_symbol, tp_price, qty)
        if tp_order:
            print(f"  ✓ TP установлен: order_id={tp_order.get('order_id')}")
        else:
            print("  ✗ ОШИБКА: TP не установлен")
            return

        await asyncio.sleep(1.0)

        # [6] Проверка статуса SL через адаптер
        print("\n[6] Проверка статуса SL через адаптер:")
        sl_status = await exchange.get_sl_status(test_symbol)
        print(f"  ✓ Статус SL: {sl_status}")
        if sl_status != "active":
            print("  ✗ ОШИБКА: SL не активен!")
            return

        # [7] Отмена SL/TP через адаптер
        print("\n[7] Отмена SL/TP через адаптер...")
        cancel_result = await exchange.cancel_sl_tp(test_symbol)
        print(f"  SL отменён: {'✓' if cancel_result.get('sl') else '✗'}")
        print(f"  TP отменён: {'✓' if cancel_result.get('tp') else '✗'}")

        await asyncio.sleep(1.0)

        # [8] Проверка статуса SL после отмены
        print("\n[8] Проверка статуса SL после отмены:")
        sl_status = await exchange.get_sl_status(test_symbol)
        print(f"  ✓ Статус SL: {sl_status}")
        if sl_status != "missing":
            print("  ✗ ОШИБКА: SL должен быть 'missing' после отмены!")
            return

        # [9] Закрытие позиции через адаптер
        print("\n[9] Закрытие позиции через адаптер...")
        close_order = await exchange.close_position(test_symbol, qty)
        if close_order and close_order.get("filled_amount", 0) > 0:
            print(f"  ✓ Позиция закрыта: qty={close_order.get('filled_amount')}")
        else:
            print("  ✗ ОШИБКА: не удалось закрыть позицию")
            return

        await asyncio.sleep(1.5)

        # [10] Финальная проверка
        print("\n[10] Финальная проверка:")
        final_qty = await exchange.get_position_qty(test_symbol)
        print(f"  ✓ Количество в позиции: {final_qty}")
        if final_qty > 0:
            print("  ✗ ОШИБКА: позиция всё ещё открыта!")
            return
        
        final_sl_status = await exchange.get_sl_status(test_symbol)
        print(f"  ✓ Статус SL: {final_sl_status}")

        print("\n" + "=" * 60)
        print("✅ ТЕСТ RealExchange ПРОЙДЕН УСПЕШНО")
        print("=" * 60)
        print("\nВсе методы адаптера работают корректно:")
        print("  ✓ place_market_buy")
        print("  ✓ place_market_sell (через close_position)")
        print("  ✓ place_sl")
        print("  ✓ place_tp")
        print("  ✓ get_sl_status")
        print("  ✓ cancel_sl_tp")
        print("  ✓ get_position_qty")
        print("  ✓ get_balance")
        print("\nТеперь можно строить PositionTracker поверх ExchangeAdapter.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nПрервано пользователем.")
        print("ВНИМАНИЕ: проверьте, не осталась ли открытая позиция!")
    except Exception as e:
        print(f"\nОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        print("\nВНИМАНИЕ: проверьте состояние позиции на бирже!")