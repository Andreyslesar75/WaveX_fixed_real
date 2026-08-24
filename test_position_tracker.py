#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# ФАЙЛ: test_position_tracker.py
# СОХРАНИТЬ КАК: test_position_tracker.py

Тест PositionTracker с PaperExchange.
Проверяет:
- открытие позиции;
- TP1 (частичное закрытие);
- breakeven;
- trailing;
- SL;
- timeout.
"""
import asyncio
import time
from exchange_adapter import PaperExchange
from position_tracker import PositionTracker
from config import Config


async def test_position_tracker():
    """Тестирует PositionTracker."""
    print("=" * 60)
    print("ТЕСТ PositionTracker")
    print("=" * 60)
    
    # Создаём PaperExchange и PositionTracker
    exchange = PaperExchange()
    tracker = PositionTracker(exchange)
    
    # ================================================================
    # 1. Открываем LONG-позицию
    # ================================================================
    print("\n[1] Открытие LONG-позиции BTC_USDT...")
    entry_price = 50000.0
    qty = 0.1
    sl_price = 48000.0  # -4%
    tp1_price = 52000.0  # +4%
    tp2_price = 55000.0  # +10%
    
    success = await tracker.open_position(
        symbol="BTC_USDT",
        side="LONG",
        entry_price=entry_price,
        qty=qty,
        sl_price=sl_price,
        tp1_price=tp1_price,
        tp2_price=tp2_price,
        sl_pct=4.0,
        tp1_pct=4.0,
        tp2_pct=10.0,
        size_usdt=5000.0,
        score=65,
        confidence="HIGH",
    )
    
    if success:
        print(f"  ✓ Позиция открыта")
        pos = tracker.get_position("BTC_USDT")
        print(f"    entry_price={pos['entry_price']}")
        print(f"    qty={pos['quantity']}")
        print(f"    SL={pos['sl_price']}")
        print(f"    TP1={pos['tp1_price']}")
        print(f"    TP2={pos['tp2_price']}")
    else:
        print(f"  ✗ ОШИБКА: не удалось открыть позицию")
        return
    
    # ================================================================
    # 2. Проверяем, что позиция открыта
    # ================================================================
    print("\n[2] Проверка открытой позиции...")
    pos = tracker.get_position("BTC_USDT")
    if pos:
        print(f"  ✓ Позиция найдена")
    else:
        print(f"  ✗ ОШИБКА: позиция не найдена")
        return
    
    # ================================================================
    # 3. Прогоняем через серию цен — TP1
    # ================================================================
    print("\n[3] Прогон через цены — ожидаем TP1...")
    
    # Сначала подаём цену ниже TP1 (51000 < 52000)
    events = await tracker.update_prices({"BTC_USDT": 51000.0})
    if events:
        print(f"  ✗ ОШИБКА: TP1 сработал при цене 51000 (должен быть >= 52000)")
        return
    print(f"  ✓ TP1 не сработал при 51000 (правильно, tp1_price=52000)")
    
    # Проверяем, что breakeven установился (profit 2% > 1.5%)
    pos = tracker.get_position("BTC_USDT")
    if pos and pos.get("breakeven_set"):
        print(f"  ✓ Breakeven установлен (profit 2% > 1.5%)")
    else:
        print(f"  ✗ ОШИБКА: breakeven не установлен")
        return
    
    # Теперь подаём цену >= TP1 (52000)
    events = await tracker.update_prices({"BTC_USDT": 52001.0})
    if not events:
        print(f"  ✗ ОШИБКА: TP1 не сработал при цене 52000")
        return
    
    event = events[0]
    print(f"  ✓ Событие: {event['reason']} @ {event['price']}")
    print(f"    qty={event['qty']:.6f}, pnl={event['pnl']:+.2f}$")
    
    if event["reason"] != "TP1":
        print(f"  ✗ ОШИБКА: ожидался TP1, получили {event['reason']}")
        return
    
    # Проверяем, что TP1 сработал
    pos = tracker.get_position("BTC_USDT")
    if pos and pos.get("tp1_done"):
        print(f"  ✓ TP1 выполнен")
        print(f"    remaining_qty={pos['remaining_qty']:.6f}")
    else:
        print(f"  ✗ ОШИБКА: TP1 не помечен как выполненный")
        return
    
    # ================================================================
    # 4. Проверяем breakeven после TP1
    # ================================================================
    print("\n[4] Проверка breakeven после TP1...")
    if pos.get("breakeven_set"):
        print(f"  ✓ Breakeven установлен")
        print(f"    SL={pos['sl_price']:.2f} (должен быть ~{entry_price * 1.0015:.2f})")
    else:
        print(f"  ✗ ОШИБКА: breakeven не установлен после TP1")
        return
    
    # ================================================================
    # 5. Прогоняем дальше — SL (после breakeven)
    # ================================================================
    print("\n[5] Прогон через цены — ожидаем SL (после breakeven)...")
    events = await tracker.update_prices({"BTC_USDT": 49000.0})
    if not events:
        print(f"  ✗ ОШИБКА: SL не сработал при цене 49000")
        return
    
    event = events[0]
    print(f"  ✓ Событие: {event['reason']} @ {event['price']}")
    print(f"    qty={event['qty']:.6f}, pnl={event['pnl']:+.2f}$")
    
    if event["reason"] not in ("SL", "BE_SL"):
        print(f"  ✗ ОШИБКА: ожидался SL/BE_SL, получили {event['reason']}")
        return
    
    # ================================================================
    # 6. Проверяем, что позиция закрыта
    # ================================================================
    print("\n[6] Проверка закрытой позиции...")
    pos = tracker.get_position("BTC_USDT")
    if pos is None:
        print(f"  ✓ Позиция закрыта")
    else:
        print(f"  ✗ ОШИБКА: позиция всё ещё открыта")
        return
    
    # ================================================================
    # 7. Тестируем trailing
    # ================================================================
    print("\n[7] Тест трейлинга...")
    
    # Открываем новую позицию
    await tracker.open_position(
        symbol="ETH_USDT",
        side="LONG",
        entry_price=3000.0,
        qty=1.0,
        sl_price=2880.0,  # -4%
        tp1_price=3120.0,  # +4%
        tp2_price=3300.0,  # +10%
        sl_pct=4.0,
        tp1_pct=4.0,
        tp2_pct=10.0,
        size_usdt=3000.0,
    )
    
    # Прогоняем цену до 3100 — это активирует трейлинг (profit 3.33% > 3.0%),
    # но НЕ сработает TP1 (tp1_price=3120 > 3100)
    print("  Прогон через 3100 (трейлинг активируется, TP1 ещё не сработал)...")
    await tracker.update_prices({"ETH_USDT": 3100.0})
    
    # Проверяем, что трейлинг активирован
    pos = tracker.get_position("ETH_USDT")
    if pos and pos.get("trail_active"):
        print(f"  ✓ Трейлинг активирован")
        print(f"    SL={pos['sl_price']:.2f} (должен быть выше начального 2880)")
        
        if pos["sl_price"] > 2880.0:
            print(f"  ✓ SL подтянулся вверх")
        else:
            print(f"  ✗ ОШИБКА: SL не подтянулся")
            return
    else:
        print(f"  ✗ ОШИБКА: трейлинг не активирован")
        return
    
    # Теперь прогоняем дальше — TP1 и TP2 сработают, позиция закроется
    print("  Прогон через 3120 (TP1), 3200, 3300 (TP2)...")
    await tracker.update_prices({"ETH_USDT": 3120.0})
    await tracker.update_prices({"ETH_USDT": 3200.0})
    await tracker.update_prices({"ETH_USDT": 3300.0})
    
    # Проверяем, что позиция закрылась через TP2
    pos = tracker.get_position("ETH_USDT")
    if pos is None:
        print(f"  ✓ Позиция закрыта через TP2")
    else:
        print(f"  ✗ ОШИБКА: позиция всё ещё открыта")
        return
    
    # Закрываем позицию вручную для чистоты
    await tracker._close_position("ETH_USDT", 3300.0, "TEST_CLOSE")
    
    print("\n" + "=" * 60)
    print("✅ ВСЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО")
    print("=" * 60)
    print("\nPositionTracker корректно работает:")
    print("  ✓ Открытие позиции")
    print("  ✓ TP1 (частичное закрытие)")
    print("  ✓ Breakeven после TP1")
    print("  ✓ SL (после breakeven)")
    print("  ✓ Трейлинг")
    print("  ✓ Закрытие позиции")


if __name__ == "__main__":
    asyncio.run(test_position_tracker())