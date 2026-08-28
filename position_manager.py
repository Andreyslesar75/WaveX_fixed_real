#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
# ФАЙЛ: position_manager.py
# СОХРАНИТЬ КАК: position_manager.py
Модуль управления позициями и защитными ордерами (SL/TP).
Высокоуровневая бизнес-логика поверх низкоуровневого api.py.

Соответствует разделам 6 и 11 алгоритма реальной торговли:
- SL: STOP_MARKET + closePosition=true
- TP: TAKE_PROFIT_MARKET + reduceOnly=true + явный qty
- Аварийная логика восстановления SL — задел на будущее
"""
import uuid
from typing import Optional, Dict, List, Any
from api import BinanceFuturesRestClient
from logger import log

def _safe_client_id(symbol: str, prefix: str) -> str:
    """
    Создаёт clientAlgoId, допустимый для Binance.
    Binance разрешает только: [.A-Z:/a-z0-9_-]{1,36}
    Китайские и другие не-ASCII символы заменяются на 'X'.
    """
    # Заменяем все не-ASCII символы на 'X'
    safe_symbol = "".join(
        c if c.isascii() and (c.isalnum() or c in "._:/-") else "X"
        for c in symbol
    )
    # Обрезаем до безопасной длины (36 - длина префикса - 16 для uuid)
    max_sym_len = 36 - len(prefix) - 1 - 16
    if max_sym_len > 0:
        safe_symbol = safe_symbol[:max_sym_len]
    return f"{prefix}_{safe_symbol}_{uuid.uuid4().hex[:16]}"


class PositionManager:
    """
    Управляет позициями и защитными ордерами.
    Все методы асинхронные, используют api.py для REST-вызовов.
    """

    def __init__(self, api: BinanceFuturesRestClient):
        self.api = api

    # ================================================================
    # ПОЛУЧЕНИЕ ИНФОРМАЦИИ
    # ================================================================
    async def get_position_info(self, symbol: str) -> Optional[dict]:
        """
        Возвращает информацию о текущей позиции по символу.
        Если позиции нет — возвращает None.
        """
        positions = await self.api.get_position_risk(symbol=symbol)
        for p in positions:
            if p["symbol"] == symbol and p["position_amt"] != 0:
                return p
        return None

    async def get_symbol_open_orders(self, symbol: str) -> List[dict]:
        """Возвращает открытые ордера по символу."""
        return await self.api.get_open_orders(symbol=symbol)

    async def get_full_position_state(self, symbol: str) -> Dict[str, Any]:
        """
        Возвращает полное состояние позиции:
        - информация о позиции
        - список открытых ордеров
        """
        position = await self.get_position_info(symbol)
        open_orders = await self.get_symbol_open_orders(symbol)
        return {
            "position": position,
            "open_orders": open_orders,
        }

    # ================================================================
    # ПОСТАНОВКА SL / TP
    # ================================================================
    async def set_sl_order(
        self,
        symbol: str,
        sl_price: float,
    ) -> Optional[dict]:
        """
        Ставит Stop-Loss ордер (STOP_MARKET + closePosition=true).
        Сторона определяется автоматически из данных биржи.
        """
        # Автоматически определяем сторону позиции
        pos_info = await self.get_position_info(symbol)
        if not pos_info:
            log.error(f"{symbol}: позиция не найдена, не могу поставить SL")
            return None
        
        position_side = "LONG" if pos_info["position_amt"] > 0 else "SHORT"
        order_side = "SELL" if position_side == "LONG" else "BUY"
        
        client_order_id = _safe_client_id(symbol, "sl")

        order = await self.api.place_stop_market(
            symbol=symbol,
            side=order_side,
            stop_price=sl_price,
            close_position=True,
            client_order_id=client_order_id,
        )

        if order:
            log.info(
                f"{symbol}: SL установлен на {sl_price} "
                f"(side={order_side}, position={position_side}, order_id={order.get('order_id')})"
            )
        else:
            log.error(f"{symbol}: не удалось установить SL на {sl_price}")

        return order

    async def set_tp_order(
        self,
        symbol: str,
        tp_price: float,
        quantity: Optional[float] = None,
    ) -> Optional[dict]:
        """
        Ставит Take-Profit ордер (TAKE_PROFIT_MARKET + reduceOnly=true).
        Сторона и количество определяются автоматически из данных биржи.
        """
        # Автоматически определяем сторону и количество позиции
        pos_info = await self.get_position_info(symbol)
        if not pos_info:
            log.error(f"{symbol}: позиция не найдена, не могу поставить TP")
            return None
        
        position_side = "LONG" if pos_info["position_amt"] > 0 else "SHORT"
        order_side = "SELL" if position_side == "LONG" else "BUY"
        
        # Если quantity не передан, используем весь объём позиции
        if quantity is None:
            quantity = abs(pos_info["position_amt"])
        
        client_order_id = _safe_client_id(symbol, "tp")

        order = await self.api.place_take_profit_market(
            symbol=symbol,
            side=order_side,
            stop_price=tp_price,
            quantity=quantity,
            reduce_only=True,
            client_order_id=client_order_id,
        )

        if order:
            log.info(
                f"{symbol}: TP установлен на {tp_price}, qty={quantity} "
                f"(side={order_side}, position={position_side}, order_id={order.get('order_id')})"
            )
        else:
            log.error(f"{symbol}: не удалось установить TP на {tp_price}")

        return order

    async def set_protective_orders(
        self,
        symbol: str,
        sl_price: float,
        tp_price: float,
        quantity: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Ставит SL и TP последовательно.
        Сторона определяется автоматически из данных биржи.
        Возвращает результат с флагом success.
        """
        sl_order = await self.set_sl_order(
            symbol=symbol,
            sl_price=sl_price,
        )

        tp_order = await self.set_tp_order(
            symbol=symbol,
            tp_price=tp_price,
            quantity=quantity,
        )

        success = sl_order is not None and tp_order is not None
        if not success:
            log.warning(
                f"{symbol}: защитные ордера установлены не полностью "
                f"(SL={'ok' if sl_order else 'FAIL'}, "
                f"TP={'ok' if tp_order else 'FAIL'})"
            )

        return {
            "sl": sl_order,
            "tp": tp_order,
            "success": success,
        }

    # ================================================================
    # ОТМЕНА SL / TP
    # ================================================================
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
                    log.warning(f"{symbol}: не удалось отменить SL ордер {sl_order_id}")
            except Exception as e:
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
                    log.warning(f"{symbol}: не удалось отменить TP ордер {tp_order_id}")
            except Exception as e:
                log.error(f"{symbol}: ошибка отмены TP: {e}")

        return result

    
    async def cancel_all_protective_orders(self, symbol: str) -> bool:
        """
        Отменяет все открытые ордера по символу.
        Используется при форс-закрытии позиции (раздел 9 алгоритма).
        """
        ok = await self.api.cancel_all_orders(symbol)
        if ok:
            log.info(f"{symbol}: все ордера отменены")
        else:
            log.warning(f"{symbol}: не удалось отменить все ордера")
        return ok