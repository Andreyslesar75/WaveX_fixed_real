"""Алерты: интерфейс + заглушка (Telegram вне поставки, Б2-5).

Все крит-события дополнительно пишутся в incidents (storage) —
Notifier не является каналом хранения, только канал доставки.
"""
from __future__ import annotations

import logging
from typing import Protocol

logger = logging.getLogger(__name__)


class Notifier(Protocol):
    """Контракт доставки алертов (реализация Telegram — позже)."""

    def alert(self, level: str, event: str, details: str) -> None:
        """level: critical|warning|info; доставки синхронны и быстры."""


class LogNotifier:
    """Заглушка: алерт = строка в лог с тегом [ALERT]."""

    def alert(self, level: str, event: str, details: str) -> None:
        logger.log(
            logging.ERROR if level == "critical" else logging.WARNING,
            "[ALERT][%s] %s: %s", level, event, details,
        )