#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# ФАЙЛ: test_exchange_adapter.py
# СОХРАНИТЬ КАК: test_exchange_adapter.py

Тест адаптеров биржи.
"""
import asyncio
from exchange_adapter import PaperExchange


async def test_paper_exchange():
    """Тестирует PaperExchange."""
    print("=" * 60)
    print("ТЕСТ PaperExchange")
    print("=" * 60)
    
    exchange = PaperExchange()
    
    # 1. Проверяем баланс
    balance = await exchange.get_balance()
    print(f"\n[1] Баланс: {balance} USDT")
    
    # 2. Открываем LONG-позицию
    print("\n[2] Открытие LONG-позиции...")
    order = await exchange.place_market_buy("BTC_USDT", 0.1, price=50000.0)
    print(f"  Ордер: {order}")
    
    # Устанавливаем цену входа
    exchange.set_entry_price("BTC_USDT", 50000.0)
    
    # 3. Проверяем позицию
    qty = await exchange.get_position_qty("BTC_USDT")
    print(f"\n[3] Количество в позиции: {qty}")
    
    # 4. Ставим SL
    print("\n[4] Постановка SL...")
    sl_order = await exchange.place_sl("BTC_USDT", 48000.0)
    print(f"  SL ордер: {sl_order}")
    
    # 5. Проверяем статус SL
    sl_status = await exchange.get_sl_status("BTC_USDT")
    print(f"\n[5] Статус SL: {sl_status}")
    
    # 6. Ставим TP
    print("\n[6] Постановка TP...")
    tp_order = await exchange.place_tp("BTC_USDT", 55000.0, 0.1)
    print(f"  TP ордер: {tp_order}")
    
    # 7. Отменяем SL/TP
    print("\n[7] Отмена SL/TP...")
    cancel_result = await exchange.cancel_sl_tp(
        "BTC_USDT",
        sl_order_id=sl_order["order_id"],
        tp_order_id=tp_order["order_id"],
    )
    print(f"  Результат отмены: {cancel_result}")
    
    # 8. Проверяем статус SL после отмены
    sl_status = await exchange.get_sl_status("BTC_USDT")
    print(f"\n[8] Статус SL после отмены: {sl_status}")
    
    # 9. Закрываем позицию (ИСПРАВЛЕНО: передаём цену выхода)
    print("\n[9] Закрытие позиции...")
    exit_price = 50500.0  # Цена выхода (прибыль)
    close_order = await exchange.close_position("BTC_USDT", 0.1, exit_price)
    print(f"  Ордер закрытия: {close_order}")
    
    # Проверяем, что PnL рассчитан корректно
    expected_pnl = (exit_price - 50000.0) * 0.1
    print(f"  Ожидаемый PnL: {expected_pnl:.2f} USDT")
    
    # 10. Проверяем, что позиция закрыта
    qty = await exchange.get_position_qty("BTC_USDT")
    print(f"\n[10] Количество в позиции после закрытия: {qty}")
    
    # 11. Проверяем обновлённый баланс
    final_balance = await exchange.get_balance()
    print(f"\n[11] Финальный баланс: {final_balance:.2f} USDT")
    print(f"  Изменение баланса: {final_balance - 1000.0:+.2f} USDT")
    
    print("\n" + "=" * 60)
    print("ТЕСТ ЗАВЕРШЁН")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(test_paper_exchange())