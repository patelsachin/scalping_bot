"""Strategy factory — reads config and returns the right strategy instance.

Usage (engine.py):
    from src.strategy.factory import StrategyFactory
    self.strategy = StrategyFactory.create()

To switch strategies, change one line in config/settings.yaml:
    strategy:
      type: ichimoku   # was: scalping
"""
from __future__ import annotations

from src.strategy.base import StrategyBase
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


class StrategyFactory:

    @staticmethod
    def create(strategy_type: str | None = None) -> StrategyBase:
        """Instantiate and return the configured strategy.

        Falls back to 'scalping' (TwoCandleStrategy) when the type is
        unknown or not specified.
        """
        stype = strategy_type or str(config.get("strategy.type", "scalping"))

        if stype == "ichimoku":
            from src.strategy.ichimoku_strategy import IchimokuStrategy
            log.info("Strategy loaded: Ichimoku (timeframe driven by ichimoku.timeframe config)")
            return IchimokuStrategy()

        if stype != "scalping":
            log.warning(
                f"Unknown strategy type '{stype}' — falling back to 'scalping'. "
                f"Valid options: scalping | ichimoku"
            )

        from src.strategy.two_candle import TwoCandleStrategy
        log.info("Strategy loaded: Two-Candle Scalping (timeframe driven by scalping.timeframe config)")
        return TwoCandleStrategy()
