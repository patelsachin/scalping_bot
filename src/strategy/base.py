"""Abstract base class for all trading strategies.

Every strategy must implement this interface. The engine is strategy-agnostic —
it calls these methods and never cares which implementation is loaded.
Switching strategies is a one-line change in config/settings.yaml:
    strategy.type: scalping   # or: ichimoku
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd

from src.core.models import ExitReason, Signal, Trade


class StrategyBase(ABC):

    # ------------------------------------------------------------------
    # Identity & timeframe (required)
    # ------------------------------------------------------------------
    @property
    @abstractmethod
    def name(self) -> str:
        """Short identifier written to the strategy column in trade logs.
        e.g. 'scalping', 'ichimoku'
        """

    @property
    @abstractmethod
    def timeframe_minutes(self) -> int:
        """Candle interval this strategy operates on (1, 3, 5, 15 …).
        The engine passes this to CandleAggregator at startup so the right
        candle size is built from live ticks.
        """

    @property
    def timeframe_str(self) -> str:
        """Kite-compatible interval string derived from timeframe_minutes.
        Kite uses "minute" (not "1minute") for the 1-min interval.
        e.g. 1 → 'minute', 3 → '3minute', 15 → '15minute'
        """
        if self.timeframe_minutes == 1:
            return "minute"
        return f"{self.timeframe_minutes}minute"

    @property
    def seed_lookback_minutes(self) -> int:
        """Minutes of history to fetch from the tail of the previous trading
        session for indicator warmup. Override to request more history when
        the strategy needs a long warm-up period (e.g. Ichimoku 52-period).
        Default: 90 minutes (covers most standard indicators on any timeframe).
        """
        return 90

    # ------------------------------------------------------------------
    # Indicator computation (required)
    # ------------------------------------------------------------------
    @abstractmethod
    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add all strategy-specific indicator columns to df.

        Called on every candle close (WebSocket mode) and on the historical
        seed dataframe at startup. Must return an enriched copy — do not
        mutate the input dataframe in place.
        """

    # ------------------------------------------------------------------
    # Entry signal (required)
    # ------------------------------------------------------------------
    @abstractmethod
    def evaluate(self, df: pd.DataFrame, underlying: str) -> Optional[Signal]:
        """Evaluate entry conditions on the most recent completed candles.

        Returns a Signal if conditions are met, None otherwise.
        The engine filters WEAK signals after this call.
        """

    # ------------------------------------------------------------------
    # Candle-close exit (optional)
    # ------------------------------------------------------------------
    def exit_signal(self, trade: Trade, df: pd.DataFrame) -> Optional[ExitReason]:
        """Check whether an open trade should be closed based on candle-close
        indicator state (e.g. SuperTrend flip, Kijun cross, cloud re-entry).

        Called once per candle close for every open trade.
        Return an ExitReason to trigger exit, or None to let the trade run.
        Tick-level SL / target exits are handled separately by the engine
        and are NOT affected by this method.

        Default implementation: no candle-close exit.
        """
        return None
