"""Builds N-minute OHLCV candles from a stream of tick prices.

Tick data from KiteTicker provides cumulative day volume (volume_traded), not
per-tick volume. This module computes the intra-candle delta so each completed
candle carries only the volume traded during its window.
"""
from __future__ import annotations

import threading
from datetime import datetime
from typing import Callable, Optional

import pandas as pd

from src.utils.logger import get_logger
from src.utils.market_calendar import candle_start_time, now_ist

log = get_logger(__name__)


class _LiveCandle:
    """Accumulates tick data for one candle period."""

    __slots__ = ("open_time", "open", "high", "low", "close", "volume", "oi", "_last_vol_traded")

    def __init__(self, ts: datetime, price: float, volume_traded: int, oi: int) -> None:
        self.open_time = ts
        self.open = price
        self.high = price
        self.low = price
        self.close = price
        self.volume = 0
        self.oi = oi
        self._last_vol_traded = volume_traded

    def update(self, price: float, volume_traded: int, oi: int) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        # volume_traded is cumulative; compute delta to get candle volume
        delta = max(0, volume_traded - self._last_vol_traded)
        self.volume += delta
        self._last_vol_traded = volume_traded
        self.oi = oi

    def to_series(self) -> pd.Series:
        return pd.Series(
            {
                "open": float(self.open),
                "high": float(self.high),
                "low": float(self.low),
                "close": float(self.close),
                "volume": float(self.volume),
                "open_interest": float(self.oi),
            },
            name=pd.Timestamp(self.open_time),
        )


class CandleAggregator:
    """Thread-safe N-minute candle builder from a tick stream.

    Candle boundaries are aligned to the market session open (09:15 IST) so
    candles match Kite historical data exactly.

    Usage:
        agg = CandleAggregator(interval_minutes=3)
        agg.on_candle_close(my_handler)   # handler(token, pd.Series)
        agg.process_tick(token, price, volume_traded, oi, timestamp)
    """

    def __init__(self, interval_minutes: int = 3) -> None:
        self.interval = interval_minutes
        self._lock = threading.Lock()
        self._state: dict[int, _LiveCandle] = {}   # token -> live candle
        self._close_handlers: list[Callable[[int, pd.Series], None]] = []

    def on_candle_close(self, handler: Callable[[int, pd.Series], None]) -> None:
        """Register callback(token, series) fired when a candle period closes."""
        self._close_handlers.append(handler)

    def process_tick(
        self,
        token: int,
        price: float,
        volume_traded: int = 0,
        oi: int = 0,
        timestamp: Optional[datetime] = None,
    ) -> None:
        """Feed one tick into the aggregator.

        timestamp should be the exchange_timestamp from the tick dict. If None,
        the current IST wall clock is used.
        """
        ts = timestamp if timestamp is not None else now_ist().replace(tzinfo=None)
        # candle_start_time returns an IST-aware datetime; strip tz for tz-naive storage
        candle_ts = candle_start_time(ts, self.interval).replace(tzinfo=None)

        completed: Optional[pd.Series] = None

        with self._lock:
            current = self._state.get(token)
            if current is None:
                self._state[token] = _LiveCandle(candle_ts, price, volume_traded, oi)
                return

            if candle_ts == current.open_time:
                current.update(price, volume_traded, oi)
                return

            # New candle period — snapshot the completed candle before starting a new one
            completed = current.to_series()
            self._state[token] = _LiveCandle(candle_ts, price, volume_traded, oi)

        # Fire handlers outside the lock to avoid deadlock if handlers call back into us
        if completed is not None:
            log.debug(
                f"Candle closed [{token}] {completed.name} "
                f"O={completed['open']:.2f} H={completed['high']:.2f} "
                f"L={completed['low']:.2f} C={completed['close']:.2f} "
                f"V={completed['volume']:.0f}"
            )
            for handler in self._close_handlers:
                try:
                    handler(token, completed)
                except Exception as e:
                    log.exception(f"on_candle_close handler error: {e}")

    def get_live_candle(self, token: int) -> Optional[pd.Series]:
        """Return the currently forming (incomplete) candle for a token, or None."""
        with self._lock:
            c = self._state.get(token)
            return c.to_series() if c is not None else None
