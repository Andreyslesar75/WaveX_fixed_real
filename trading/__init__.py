"""Торговая часть WaveX v2 (real + paper).

Заменяет: risk_manager.py, position_tracker.py, position_manager.py,
exchange_adapter.py, reconciliation.py, order-часть api.py.

Не входит (граница компетенции): signals.py, анализ scanner.py,
calculations.py, gui.py — вызываются/читаются, но не изменяются.

Требования окружения: Python >= 3.10, pydantic >= 2.6.
"""

