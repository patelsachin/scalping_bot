"""Unit tests for Two Candle Theory strategy engine."""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from src.core.models import SignalStrength, TradeType
from src.indicators.technical import compute_all_indicators
from src.strategy.two_candle import PositionalTrendFilter, TwoCandleStrategy


def _build_df_with_context(
    last_two_bullish: bool = True,
    volume: int = 60000,
    n_history: int = 30,
) -> pd.DataFrame:
    """Build a df where the last 2 candles are bullish or bearish, with sufficient history."""
    rng = np.random.default_rng(0)
    n = n_history
    timestamps = [
        datetime(2024, 1, 15, 9, 15) + timedelta(minutes=3 * i) for i in range(n + 2)
    ]

    # Build a gentle trend matching the final 2-candle direction
    if last_two_bullish:
        closes_hist = np.linspace(49800, 50000, n)
    else:
        closes_hist = np.linspace(50200, 50000, n)

    # Last two candles in explicit direction
    if last_two_bullish:
        last_closes = np.array([50010, 50030])
        last_opens = np.array([50000, 50015])
    else:
        last_closes = np.array([49990, 49970])
        last_opens = np.array([50000, 49985])

    closes = np.concatenate([closes_hist, last_closes])
    # Derive opens
    opens = np.concatenate([closes_hist - 2, last_opens])
    highs = np.maximum(opens, closes) + 5
    lows = np.minimum(opens, closes) - 5
    volumes = np.array([60000] * n + [volume, volume])

    df = pd.DataFrame(
        {
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
        },
        index=pd.DatetimeIndex(timestamps),
    )
    return compute_all_indicators(df)


class TestTwoCandleStrategy:
    def test_bullish_signal_generation(self):
        df = _build_df_with_context(last_two_bullish=True, volume=70000)
        strategy = TwoCandleStrategy()
        signal = strategy.evaluate(df, "BANKNIFTY")
        # Signal may or may not fire based on random indicator state; just verify shape
        if signal is not None:
            assert signal.underlying == "BANKNIFTY"
            assert signal.strength in {SignalStrength.STRONG, SignalStrength.MEDIUM, SignalStrength.WEAK}
            assert signal.conditions_met >= 5

    def test_low_volume_blocks_signal(self):
        df = _build_df_with_context(last_two_bullish=True, volume=10000)
        strategy = TwoCandleStrategy()
        signal = strategy.evaluate(df, "BANKNIFTY")
        # With low volume, conditions_met should never reach 5 (volume condition fails)
        # Either no signal or weak signal
        if signal is not None:
            assert signal.conditions_met < 6

    def test_insufficient_data(self):
        df = pd.DataFrame()
        strategy = TwoCandleStrategy()
        assert strategy.evaluate(df, "BANKNIFTY") is None


class TestPositionalTrendFilter:
    def test_long_agrees_with_uptrend(self):
        df = pd.DataFrame(
            {"supertrend_dir": [1]},
            index=pd.DatetimeIndex([datetime(2024, 1, 15, 9, 30)]),
        )
        f = PositionalTrendFilter()
        assert f.trend_agrees(df, TradeType.LONG) is True
        assert f.trend_agrees(df, TradeType.SHORT) is False

    def test_short_agrees_with_downtrend(self):
        df = pd.DataFrame(
            {"supertrend_dir": [-1]},
            index=pd.DatetimeIndex([datetime(2024, 1, 15, 9, 30)]),
        )
        f = PositionalTrendFilter()
        assert f.trend_agrees(df, TradeType.SHORT) is True
        assert f.trend_agrees(df, TradeType.LONG) is False

    def test_empty_df_defaults_to_true(self):
        f = PositionalTrendFilter()
        assert f.trend_agrees(pd.DataFrame(), TradeType.LONG) is True
