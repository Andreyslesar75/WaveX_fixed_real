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
from typing import List, Optional, Dict, Any
from logger import log


class ExchangeAdapter(ABC):
    """
    Абстрактный интерфейс биржи.
    Все методы асинхронные.
    """
    
    @abstractmethod
    async def place_market_buy(self, symbol: str, qty: float, price: float = 0.0) -> Optional[dict]:
        """
        Открывает LONG рыночным ордером.
        qty — количество монет.
        price — ожидаемая цена входа (используется для paper-режима).
        """
        pass

    @abstractmethod
    async def place_market_sell(self, symbol: str, qty: float, price: float = 0.0) -> Optional[dict]:
        """
        Открывает SHORT рыночным ордером.
        qty — количество монет.
        price — ожидаемая цена входа (используется для paper-режима).
        """
        pass
    
    @abstractmethod
    async def close_position(self, symbol: str, qty: float, price: float) -> Optional[dict]:
        """Закрывает позицию по указанной цене."""
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
    async def get_position_info(self, symbol: str) -> Optional[dict]:
        """
        Возвращает информацию о позиции на бирже.
        Если позиции нет — возвращает None.
        """
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

    @abstractmethod
    async def get_klines(self, symbol: str, interval: str, limit: int) -> Optional[list]:
        """Возвращает свечи для технического анализа."""
        pass

    @abstractmethod
    async def place_market_buy(
        self, 
        symbol: str, 
        qty: float, 
        price: float, 
        sl_price: float = 0.0, 
        tp_price: float = 0.0
    ) -> Optional[dict]:
        """Открывает LONG рыночным ордером и (в real-режиме) ставит защиту."""
        pass

    @abstractmethod
    async def place_market_sell(
        self, 
        symbol: str, 
        qty: float, 
        price: float, 
        sl_price: float = 0.0, 
        tp_price: float = 0.0
    ) -> Optional[dict]:
        """Открывает SHORT рыночным ордером и (в real-режиме) ставит защиту."""
        pass

    @abstractmethod
    async def get_min_notional(self, symbol: str) -> float:
        """Возвращает minNotional для символа."""
        pass

    @abstractmethod
    async def get_user_trades(self, symbol: str, limit: int = 50) -> List[dict]:
        """Возвращает историю сделок по символу."""
        pass


class PaperExchange(ExchangeAdapter):
    """
    Эмуляция биржи для paper-режима.
    Хранит виртуальные позиции и ордера в памяти.
    """

    async def get_user_trades(self, symbol: str, limit: int = 50) -> List[dict]:
        """Для paper-режима возвращаем пустой список."""
        return []

    def __init__(self):
        # Виртуальные позиции: symbol -> {qty, side, entry_price}
        self._positions: Dict[str, dict] = {}
        # Виртуальные SL/TP: symbol -> {sl_price, tp_price, sl_id, tp_id}
        self._orders: Dict[str, dict] = {}
        # Виртуальный баланс
        self._balance = 1000.0  # Начальный баланс из config
        # Счётчик ордеров
        self._order_counter = 1000

    async def get_klines(self, symbol: str, interval: str, limit: int) -> Optional[list]:
        """Для paper-режима возвращаем None (проверка объёма пропускается) или мок."""
        return None

    async def get_min_notional(self, symbol: str) -> float:
        """Для paper-режима возвращаем дефолтное значение."""
        return 5.0  # Дефолтный minNotional
    
    async def place_market_buy(self, symbol: str, qty: float, price: float = 0.0, sl_price: float = 0.0, tp_price: float = 0.0) -> Optional[dict]:
        """Эмулирует открытие LONG."""
        self._positions[symbol] = {
            "qty": qty,
            "side": "LONG",
            "entry_price": price if price > 0 else 0.0,  # [ИСПРАВЛЕНО] используем переданную цену
        }
        self._order_counter += 1
        return {
            "status": "filled",
            "filled_amount": qty,
            "avg_price": price if price > 0 else 0.0,
            "order_id": self._order_counter,
            "client_order_id": f"paper_{uuid.uuid4().hex[:16]}",
        }

    async def place_market_sell(self, symbol: str, qty: float, price: float = 0.0, sl_price: float = 0.0, tp_price: float = 0.0) -> Optional[dict]:
        """Эмулирует открытие SHORT."""
        self._positions[symbol] = {
            "qty": qty,
            "side": "SHORT",
            "entry_price": price if price > 0 else 0.0,  # [ИСПРАВЛЕНО]
        }
        self._order_counter += 1
        return {
            "status": "filled",
            "filled_amount": qty,
            "avg_price": price if price > 0 else 0.0,
            "order_id": self._order_counter,
            "client_order_id": f"paper_{uuid.uuid4().hex[:16]}",
        }
    
    async def close_position(self, symbol: str, qty: float, price: float) -> Optional[dict]:
        """Эмулирует закрытие позиции по указанной цене."""
        if symbol not in self._positions:
            return None
        
        pos = self._positions[symbol]
        actual_qty = min(qty, pos["qty"])
        pos["qty"] -= actual_qty
        
        if pos["qty"] <= 1e-12:
            del self._positions[symbol]
        
        # Рассчитываем PnL для эмуляции
        entry_price = pos.get("entry_price", price)
        side = pos.get("side", "LONG")
        if side == "SHORT":
            pnl = (entry_price - price) * actual_qty
        else:
            pnl = (price - entry_price) * actual_qty
        
        self._balance += pnl
        
        self._order_counter += 1
        return {
            "status": "filled",
            "filled_amount": actual_qty,
            "avg_price": price,
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

    async def get_position_info(self, symbol: str) -> Optional[dict]:
        """Возвращает информацию о виртуальной позиции."""
        if symbol in self._positions:
            return {
                "symbol": symbol,
                "position_amt": self._positions[symbol]["qty"],
                "entry_price": self._positions[symbol]["entry_price"],
            }
        return None
    
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

    async def get_user_trades(self, symbol: str, limit: int = 50) -> List[dict]:
        """Получает историю сделок через api.py."""
        return await self.api.get_user_trades(symbol, limit=limit)

    async def get_klines(self, symbol: str, interval: str, limit: int) -> Optional[list]:
        """Делегирует запрос к api.py."""
        return await self.api.get_klines(symbol, interval, limit)

    async def get_min_notional(self, symbol: str) -> float:
        """Получает minNotional через api."""
        from api import to_binance_symbol
        bsym = to_binance_symbol(symbol)
        info = await self.api._get_symbol_info(bsym)
        return info.get("minNotional", 5.0)
    
    async def place_market_buy(
        self, 
        symbol: str, 
        qty: float, 
        price: float = 0.0, 
        sl_price: float = 0.0, 
        tp_price: float = 0.0
    ) -> Optional[dict]:
        """Открывает LONG и сразу ставит SL/TP. При неудаче — аварийное закрытие."""
        quote_qty = qty * price if price > 0 else qty
        order = await self.api.place_market_buy(symbol, quote_qty)
        
        if not order or order.get("filled_amount", 0) <= 0:
            return None

        executed_qty = order.get("filled_amount")
        avg_price = order.get("avg_price", price)

        # --- РЕАЛЬНАЯ ЗАЩИТА (Аварийная ветка) ---
        if sl_price > 0 and tp_price > 0:
            protect_result = await self.pm.set_protective_orders(
                symbol=symbol,
                sl_price=sl_price,
                tp_price=tp_price,
                quantity=executed_qty,
            )
            sl_order = protect_result.get("sl")
            tp_order = protect_result.get("tp")

            if not sl_order or not tp_order:
                log.error(
                    f"{symbol}: АВАРИЙНАЯ ВЕТКА — защитные ордера не поставились "
                    f"(SL={'ok' if sl_order else 'FAIL'}, TP={'ok' if tp_order else 'FAIL'}). "
                    f"Форс-закрытие позиции."
                )
                # Отменяем то, что успело поставиться
                if sl_order or tp_order:
                    await self.pm.cancel_sl_tp(
                        symbol,
                        sl_order_id=sl_order.get("order_id") if sl_order else None,
                        tp_order_id=tp_order.get("order_id") if tp_order else None,
                        sl_client_id=sl_order.get("client_order_id") if sl_order else None,
                        tp_client_id=tp_order.get("client_order_id") if tp_order else None,
                    )
                # Форс-закрытие LONG позиции (продажа)
                await self.api.place_market_sell(symbol, executed_qty)
                return None  # Возвращаем None, чтобы tracker знал, что открытие провалилось

            # Сохраняем ID ордеров в ответ, чтобы tracker мог их запомнить
            order["sl_order_id"] = sl_order.get("order_id")
            order["sl_client_id"] = sl_order.get("client_order_id")
            order["tp_order_id"] = tp_order.get("order_id")
            order["tp_client_id"] = tp_order.get("client_order_id")

            # [ИСПРАВЛЕНО] Сохраняем ID в локальный кэш algo-ордеров
            # Это нужно для runtime-проверки статуса SL в _check_sl_health()
            if symbol not in self._algo_orders:
                self._algo_orders[symbol] = {}
            if sl_order:
                self._algo_orders[symbol]["sl"] = {
                    "order_id": sl_order.get("order_id"),
                    "client_order_id": sl_order.get("client_order_id"),
                }
            if tp_order:
                self._algo_orders[symbol]["tp"] = {
                    "order_id": tp_order.get("order_id"),
                    "client_order_id": tp_order.get("client_order_id"),
                }

        return order

    async def place_market_sell(
        self, 
        symbol: str, 
        qty: float, 
        price: float = 0.0, 
        sl_price: float = 0.0, 
        tp_price: float = 0.0
    ) -> Optional[dict]:
        """Открывает SHORT и сразу ставит SL/TP. При неудаче — аварийное закрытие."""
        quote_qty = qty * price if price > 0 else qty
        order = await self.api.place_market_sell_open(symbol, quote_qty)
        
        if not order or order.get("filled_amount", 0) <= 0:
            return None

        executed_qty = order.get("filled_amount")
        avg_price = order.get("avg_price", price)

        # --- РЕАЛЬНАЯ ЗАЩИТА (Аварийная ветка) ---
        if sl_price > 0 and tp_price > 0:
            protect_result = await self.pm.set_protective_orders(
                symbol=symbol,
                sl_price=sl_price,
                tp_price=tp_price,
                quantity=executed_qty,
            )
            sl_order = protect_result.get("sl")
            tp_order = protect_result.get("tp")

            if not sl_order or not tp_order:
                log.error(
                    f"{symbol}: АВАРИЙНАЯ ВЕТКА — защитные ордера не поставились. Форс-закрытие."
                )
                if sl_order or tp_order:
                    await self.pm.cancel_sl_tp(
                        symbol,
                        sl_order_id=sl_order.get("order_id") if sl_order else None,
                        tp_order_id=tp_order.get("order_id") if tp_order else None,
                        sl_client_id=sl_order.get("client_order_id") if sl_order else None,
                        tp_client_id=tp_order.get("client_order_id") if tp_order else None,
                    )
                # Форс-закрытие SHORT позиции (покупка)
                await self.api.place_market_buy_close(symbol, executed_qty)
                return None

            order["sl_order_id"] = sl_order.get("order_id")
            order["sl_client_id"] = sl_order.get("client_order_id")
            order["tp_order_id"] = tp_order.get("order_id")
            order["tp_client_id"] = tp_order.get("client_order_id")

            # [ИСПРАВЛЕНО] Сохраняем ID в локальный кэш algo-ордеров
            if symbol not in self._algo_orders:
                self._algo_orders[symbol] = {}
            if sl_order:
                self._algo_orders[symbol]["sl"] = {
                    "order_id": sl_order.get("order_id"),
                    "client_order_id": sl_order.get("client_order_id"),
                }
            if tp_order:
                self._algo_orders[symbol]["tp"] = {
                    "order_id": tp_order.get("order_id"),
                    "client_order_id": tp_order.get("client_order_id"),
                }

        return order
    
    async def close_position(self, symbol: str, qty: float, price: float) -> Optional[dict]:
        """
        Закрывает позицию через api.py.
        Цена используется только для логирования, реальное исполнение — рыночное.
        """
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
        """Отменяет SL и/или TP по их ID."""
        result = {"sl": False, "tp": False}
        
        if sl_order_id is not None and sl_client_id is not None:
            try:
                ok = await self.api.cancel_order(
                    symbol, sl_order_id, client_order_id=sl_client_id, is_algo=True
                )
                result["sl"] = ok
                if ok:
                    log.info(f"{symbol}: SL ордер {sl_order_id} отменён")
                else:
                    # [ИСПРАВЛЕНО] Если ордер не найден, считаем его уже исполненным
                    log.debug(f"{symbol}: SL ордер {sl_order_id} уже не существует (исполнен или отменён)")
                    result["sl"] = True  # Считаем успешным
            except Exception as e:
                # [ИСПРАВЛЕНО] Игнорируем ошибку -2011 (ордер уже не существует)
                if "-2011" in str(e):
                    log.debug(f"{symbol}: SL ордер {sl_order_id} уже не существует")
                    result["sl"] = True
                else:
                    log.error(f"{symbol}: ошибка отмены SL: {e}")

        if tp_order_id is not None and tp_client_id is not None:
            try:
                ok = await self.api.cancel_order(
                    symbol, tp_order_id, client_order_id=tp_client_id, is_algo=True
                )
                result["tp"] = ok
                if ok:
                    log.info(f"{symbol}: TP ордер {tp_order_id} отменён")
                else:
                    log.debug(f"{symbol}: TP ордер {tp_order_id} уже не существует")
                    result["tp"] = True
            except Exception as e:
                if "-2011" in str(e):
                    log.debug(f"{symbol}: TP ордер {tp_order_id} уже не существует")
                    result["tp"] = True
                else:
                    log.error(f"{symbol}: ошибка отмены TP: {e}")

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

    async def get_position_info(self, symbol: str) -> Optional[dict]:
        """Возвращает информацию о позиции через position_manager."""
        return await self.pm.get_position_info(symbol)
    
    async def get_sl_status(self, symbol: str) -> Optional[str]:
        """
        [УПРОЩЕНО]
        Проверяет статус SL по ID из кэша.
        Возвращает: "active" | "missing" | "unknown" | None
        - "active" — ордер существует (NEW или TRIGGERED)
        - "missing" — ордера нет на бирже
        - "unknown" — ошибка API (не удаляем позицию)
        - None — позиции нет на бирже
        """
        # 1. Проверяем, есть ли позиция на бирже
        pos_info = await self.pm.get_position_info(symbol)
        if not pos_info:
            return None  # Позиции нет
        
        # 2. Берём ID из локального кэша
        cached = self._algo_orders.get(symbol, {})
        sl_info = cached.get("sl")
        if not sl_info:
            return "missing"  # ID нет в кэше
        
        # 3. Просто проверяем: есть ордер или нет
        try:
            status_resp = await self.api.get_algo_order_status(
                symbol,
                algo_id=sl_info["order_id"],
                client_algo_id=sl_info["client_order_id"],
            )
            
            if status_resp:
                status = status_resp.get("status")
                # NEW или TRIGGERED — ордер существует
                if status in ("NEW", "TRIGGERED"):
                    return "active"
                else:
                    return "missing"
            else:
                return "missing"
        except Exception as e:
            log.error(f"{symbol}: ошибка проверки SL: {e}")
            return "unknown"
    
    async def get_balance(self, asset: str = "USDT") -> float:
        """Получает баланс через api.py."""
        return await self.api.get_balance(asset)