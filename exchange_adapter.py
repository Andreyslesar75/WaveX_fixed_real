#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# ФАЙЛ: exchange_adapter.py
# СОХРАНИТЬ КАК: exchange_adapter.py

Абстракция биржи для управления позициями.
Позволяет PositionTracker работать одинаково с paper и real режимами.

Архитектура:
- ExchangeAdapter — абстрактный интерфейс
- PaperExchange — эмуляция биржи (для paper-режима)
- RealExchange — обёртка над api.py + position_manager.py (для real-режима)
"""
import asyncio
import time
import uuid
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any
from logger import log


class ExchangeAdapter(ABC):
    """
    Абстрактный интерфейс биржи.
    Все методы асинхронные.
    """
    
    @abstractmethod
    async def place_market_buy(self, symbol: str, qty: float) -> Optional[dict]:
        """Открывает LONG рыночным ордером."""
        pass
    
    @abstractmethod
    async def place_market_sell(self, symbol: str, qty: float) -> Optional[dict]:
        """Открывает SHORT рыночным ордером."""
        pass
    
    @abstractmethod
    async def close_position(self, symbol: str, qty: float) -> Optional[dict]:
        """Закрывает позицию рыночным ордером."""
        pass
    
    @abstractmethod
    async def place_sl(self, symbol: str, price: float) -> Optional[dict]:
        """Ставит Stop-Loss ордер."""
        pass
    
    @abstractmethod
    async def place_tp(self, symbol: str, price: float, qty: float) -> Optional[dict]:
        """Ставит Take-Profit ордер."""
        pass
    
    @abstractmethod
    async def cancel_sl_tp(
        self, 
        symbol: str, 
        sl_order_id: Optional[int] = None,
        tp_order_id: Optional[int] = None,
        sl_client_id: Optional[str] = None,
        tp_client_id: Optional[str] = None,
    ) -> Dict[str, bool]:
        """Отменяет SL и/или TP."""
        pass
    
    @abstractmethod
    async def get_last_price(self, symbol: str) -> Optional[float]:
        """Возвращает последнюю цену символа."""
        pass
    
    @abstractmethod
    async def get_position_qty(self, symbol: str) -> float:
        """Возвращает количество монет в позиции (0 если позиции нет)."""
        pass
    
    @abstractmethod
    async def get_sl_status(self, symbol: str) -> Optional[str]:
        """
        Проверяет статус SL на бирже.
        Возвращает: "active", "missing", None (если позиции нет)
        """
        pass
    
    @abstractmethod
    async def get_balance(self, asset: str = "USDT") -> float:
        """Возвращает доступный баланс."""
        pass


class PaperExchange(ExchangeAdapter):
    """
    Эмуляция биржи для paper-режима.
    Хранит виртуальные позиции и ордера в памяти.
    """
    
    def __init__(self):
        # Виртуальные позиции: symbol -> {qty, side, entry_price}
        self._positions: Dict[str, dict] = {}
        # Виртуальные SL/TP: symbol -> {sl_price, tp_price, sl_id, tp_id}
        self._orders: Dict[str, dict] = {}
        # Виртуальный баланс
        self._balance = 1000.0  # Начальный баланс из config
        # Счётчик ордеров
        self._order_counter = 1000
    
    async def place_market_buy(self, symbol: str, qty: float) -> Optional[dict]:
        """Эмулирует открытие LONG."""
        # Для paper-режима нужна цена входа — её передаст PositionTracker
        # Здесь просто регистрируем позицию
        self._positions[symbol] = {
            "qty": qty,
            "side": "LONG",
            "entry_price": 0.0,  # Будет установлено PositionTracker
        }
        self._order_counter += 1
        return {
            "status": "filled",
            "filled_amount": qty,
            "avg_price": 0.0,  # PositionTracker установит реальную цену
            "order_id": self._order_counter,
            "client_order_id": f"paper_{uuid.uuid4().hex[:16]}",
        }
    
    async def place_market_sell(self, symbol: str, qty: float) -> Optional[dict]:
        """Эмулирует открытие SHORT."""
        self._positions[symbol] = {
            "qty": qty,
            "side": "SHORT",
            "entry_price": 0.0,
        }
        self._order_counter += 1
        return {
            "status": "filled",
            "filled_amount": qty,
            "avg_price": 0.0,
            "order_id": self._order_counter,
            "client_order_id": f"paper_{uuid.uuid4().hex[:16]}",
        }
    
    async def close_position(self, symbol: str, qty: float) -> Optional[dict]:
        """Эмулирует закрытие позиции."""
        if symbol not in self._positions:
            return None
        
        pos = self._positions[symbol]
        actual_qty = min(qty, pos["qty"])
        pos["qty"] -= actual_qty
        
        if pos["qty"] <= 1e-12:
            del self._positions[symbol]
        
        self._order_counter += 1
        return {
            "status": "filled",
            "filled_amount": actual_qty,
            "avg_price": 0.0,  # PositionTracker установит реальную цену
            "order_id": self._order_counter,
            "client_order_id": f"paper_{uuid.uuid4().hex[:16]}",
        }
    
    async def place_sl(self, symbol: str, price: float) -> Optional[dict]:
        """Ставит виртуальный SL."""
        if symbol not in self._orders:
            self._orders[symbol] = {}
        
        self._order_counter += 1
        sl_id = self._order_counter
        self._orders[symbol]["sl_price"] = price
        self._orders[symbol]["sl_id"] = sl_id
        self._orders[symbol]["sl_client_id"] = f"paper_sl_{uuid.uuid4().hex[:16]}"
        
        return {
            "status": "new",
            "order_id": sl_id,
            "client_order_id": self._orders[symbol]["sl_client_id"],
        }
    
    async def place_tp(self, symbol: str, price: float, qty: float) -> Optional[dict]:
        """Ставит виртуальный TP."""
        if symbol not in self._orders:
            self._orders[symbol] = {}
        
        self._order_counter += 1
        tp_id = self._order_counter
        self._orders[symbol]["tp_price"] = price
        self._orders[symbol]["tp_qty"] = qty
        self._orders[symbol]["tp_id"] = tp_id
        self._orders[symbol]["tp_client_id"] = f"paper_tp_{uuid.uuid4().hex[:16]}"
        
        return {
            "status": "new",
            "order_id": tp_id,
            "client_order_id": self._orders[symbol]["tp_client_id"],
        }
    
    async def cancel_sl_tp(
        self, 
        symbol: str, 
        sl_order_id: Optional[int] = None,
        tp_order_id: Optional[int] = None,
        sl_client_id: Optional[str] = None,
        tp_client_id: Optional[str] = None,
    ) -> Dict[str, bool]:
        """Отменяет виртуальные SL/TP."""
        result = {"sl": False, "tp": False}
        
        if symbol in self._orders:
            if sl_order_id is not None or sl_client_id is not None:
                if "sl_price" in self._orders[symbol]:
                    del self._orders[symbol]["sl_price"]
                    del self._orders[symbol]["sl_id"]
                    del self._orders[symbol]["sl_client_id"]
                    result["sl"] = True
            
            if tp_order_id is not None or tp_client_id is not None:
                if "tp_price" in self._orders[symbol]:
                    del self._orders[symbol]["tp_price"]
                    del self._orders[symbol]["tp_qty"]
                    del self._orders[symbol]["tp_id"]
                    del self._orders[symbol]["tp_client_id"]
                    result["tp"] = True
        
        return result
    
    async def get_last_price(self, symbol: str) -> Optional[float]:
        """Для paper-режима цена всегда приходит извне (через PositionTracker)."""
        return None
    
    async def get_position_qty(self, symbol: str) -> float:
        """Возвращает количество в виртуальной позиции."""
        if symbol in self._positions:
            return self._positions[symbol]["qty"]
        return 0.0
    
    async def get_sl_status(self, symbol: str) -> Optional[str]:
        """Проверяет статус виртуального SL."""
        if symbol not in self._positions:
            return None
        
        if symbol in self._orders and "sl_price" in self._orders[symbol]:
            return "active"
        return "missing"
    
    async def get_balance(self, asset: str = "USDT") -> float:
        """Возвращает виртуальный баланс."""
        return self._balance
    
    def set_entry_price(self, symbol: str, price: float):
        """Устанавливает цену входа для виртуальной позиции."""
        if symbol in self._positions:
            self._positions[symbol]["entry_price"] = price
    
    def update_balance(self, delta: float):
        """Обновляет виртуальный баланс."""
        self._balance += delta


class RealExchange(ExchangeAdapter):
    """
    Обёртка над реальной биржей через api.py + position_manager.py.
    Хранит в памяти ID созданных algo-ордеров (SL/TP),
    потому что Binance не возвращает их через обычный /fapi/v1/openOrders.
    """
    
    def __init__(self, api_client, position_manager):
        """
        api_client — экземпляр BinanceFuturesRestClient из api.py
        position_manager — экземпляр PositionManager из position_manager.py
        """
        self.api = api_client
        self.pm = position_manager
        # Локальный кэш algo-ордеров: symbol -> {sl: {...}, tp: {...}}
        self._algo_orders: Dict[str, dict] = {}
    
    async def place_market_buy(self, symbol: str, qty: float) -> Optional[dict]:
        """Открывает LONG через api.py. qty — это quote_qty (USDT)."""
        return await self.api.place_market_buy(symbol, qty)
    
    async def place_market_sell(self, symbol: str, qty: float) -> Optional[dict]:
        """Открывает SHORT через api.py. qty — это quote_qty (USDT)."""
        return await self.api.place_market_sell_open(symbol, qty)
    
    async def close_position(self, symbol: str, qty: float) -> Optional[dict]:
        """Закрывает позицию через api.py."""
        pos_info = await self.pm.get_position_info(symbol)
        if not pos_info:
            return None
        
        if pos_info["position_amt"] > 0:
            # LONG — закрываем продажей
            return await self.api.place_market_sell(symbol, qty)
        else:
            # SHORT — закрываем покупкой
            return await self.api.place_market_buy_close(symbol, qty)
    
    async def place_sl(self, symbol: str, price: float) -> Optional[dict]:
        """Ставит SL через position_manager.py и сохраняет ID в кэш."""
        order = await self.pm.set_sl_order(symbol, price)
        if order:
            # Сохраняем ID algo-ордера в локальном кэше
            if symbol not in self._algo_orders:
                self._algo_orders[symbol] = {}
            self._algo_orders[symbol]["sl"] = {
                "order_id": order.get("order_id"),
                "client_order_id": order.get("client_order_id"),
            }
        return order
    
    async def place_tp(self, symbol: str, price: float, qty: float) -> Optional[dict]:
        """Ставит TP через position_manager.py и сохраняет ID в кэш."""
        order = await self.pm.set_tp_order(symbol, price, qty)
        if order:
            if symbol not in self._algo_orders:
                self._algo_orders[symbol] = {}
            self._algo_orders[symbol]["tp"] = {
                "order_id": order.get("order_id"),
                "client_order_id": order.get("client_order_id"),
            }
        return order
    
    async def cancel_sl_tp(
        self, 
        symbol: str, 
        sl_order_id: Optional[int] = None,
        tp_order_id: Optional[int] = None,
        sl_client_id: Optional[str] = None,
        tp_client_id: Optional[str] = None,
    ) -> Dict[str, bool]:
        """Отменяет SL/TP через position_manager.py и удаляет из кэша."""
        # Если ID не переданы явно, берём из кэша
        cached = self._algo_orders.get(symbol, {})
        if sl_order_id is None and "sl" in cached:
            sl_order_id = cached["sl"]["order_id"]
            sl_client_id = cached["sl"]["client_order_id"]
        if tp_order_id is None and "tp" in cached:
            tp_order_id = cached["tp"]["order_id"]
            tp_client_id = cached["tp"]["client_order_id"]
        
        result = await self.pm.cancel_sl_tp(
            symbol, sl_order_id, tp_order_id, sl_client_id, tp_client_id
        )
        
        # Удаляем из кэша успешно отменённые ордера
        if result.get("sl") and symbol in self._algo_orders:
            self._algo_orders[symbol].pop("sl", None)
        if result.get("tp") and symbol in self._algo_orders:
            self._algo_orders[symbol].pop("tp", None)
        
        # Если кэш символа пустой, удаляем его целиком
        if symbol in self._algo_orders and not self._algo_orders[symbol]:
            del self._algo_orders[symbol]
        
        return result
    
    async def get_last_price(self, symbol: str) -> Optional[float]:
        """Получает последнюю цену через api.py."""
        return await self.api.get_last_price(symbol)
    
    async def get_position_qty(self, symbol: str) -> float:
        """Получает количество в позиции через position_manager.py."""
        pos_info = await self.pm.get_position_info(symbol)
        if pos_info:
            return abs(pos_info["position_amt"])
        return 0.0
    
    async def get_sl_status(self, symbol: str) -> Optional[str]:
        """
        Проверяет статус SL на бирже.
        Использует локальный кэш algo-ордеров + get_algo_order_status.
        """
        pos_info = await self.pm.get_position_info(symbol)
        if not pos_info:
            return None  # Позиции нет
        
        cached = self._algo_orders.get(symbol, {})
        sl_info = cached.get("sl")
        
        if not sl_info:
            # В кэше нет SL — значит мы его не ставили или он уже отменён
            return "missing"
        
        # Проверяем реальный статус на бирже
        status_resp = await self.api.get_algo_order_status(
            symbol,
            algo_id=sl_info["order_id"],
            client_algo_id=sl_info["client_order_id"],
        )
        
        if not status_resp:
            # Ордер не найден на бирже — значит он исполнился или был отменён
            # Удаляем из кэша
            self._algo_orders[symbol].pop("sl", None)
            return "missing"
        
        algo_status = status_resp.get("status")
        if algo_status == "NEW":
            return "active"
        elif algo_status in ("CANCELED", "EXPIRED"):
            # Ордер отменён/истёк — удаляем из кэша
            self._algo_orders[symbol].pop("sl", None)
            return "missing"
        elif algo_status == "TRIGGERED":
            # SL сработал — удаляем из кэша
            self._algo_orders[symbol].pop("sl", None)
            return "triggered"
        else:
            return "unknown"
    
    async def get_balance(self, asset: str = "USDT") -> float:
        """Получает баланс через api.py."""
        return await self.api.get_balance(asset)