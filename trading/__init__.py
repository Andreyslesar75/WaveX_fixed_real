"""Торговая часть WaveX v2 (real + paper). См. trading/bootstrap.py."""
from .bootstrap import build_position_manager
from .facade import PositionManager

__all__ = ["PositionManager", "build_position_manager"]
