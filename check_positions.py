#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для проверки текущих открытых позиций.
"""

import asyncio
import os
import sys
from config import Config
from logger import log
from api import BinanceFuturesRestClient
from risk_manager import PositionManager

async def main():
    # Проверяем API-ключи
    if not Config.BINANCE_API_KEY or not Config.BINANCE_API_SECRET:
        print("❌ API-ключи не установлены в .env файле!")
        return
    
    # Создаём REST-клиент
    rest_client = BinanceFuturesRestClient(
        api_key=Config.BINANCE_API_KEY,
        api_secret=Config.BINANCE_API_SECRET,
        base_url=Config.BINANCE_BASE,
    )
    
    # Создаём manager позиций
    pm = PositionManager(rest_client, is_real=Config.REAL_TRADING)
    
    # Получаем открытые позиции
    print("\n" + "="*60)
    print("ОТКРЫТЫЕ ПОЗИЦИИ")
    print("="*60)
    
    open_positions = pm.get_open_positions()
    
    if not open_positions:
        print("✓ Нет открытых позиций")
    else:
        print(f"✓ Всего открыто: {len(open_positions)} позиций\n")
        
        for i, pos in enumerate(open_positions, 1):
            print(f"Позиция #{i}:")
            print(f"  Символ: {pos.get('symbol', 'N/A')}")
            print(f"  Сторона: {pos.get('side', 'N/A')}")
            print(f"  Размер: {pos.get('size_usdt', 0):.2f} USDT")
            print(f"  Цена входа: {pos.get('entry_price', 0):.8f}")
            print(f"  Stop Loss: {pos.get('sl_price', 0):.8f}")
            print(f"  Take Profit 1: {pos.get('tp1_price', 0):.8f}")
            print(f"  Take Profit 2: {pos.get('tp2_price', 0):.8f}")
            print(f"  Score: {pos.get('score', 'N/A')}")
            print()
    
    print("="*60)
    
    # Закрываем PM
    pm.close()

if __name__ == "__main__":
    asyncio.run(main())
