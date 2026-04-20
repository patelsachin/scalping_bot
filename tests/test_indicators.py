"""Unit tests for technical indicators."""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from src.indicators.technical import (
    atr,
    compute_all_indicators,
    psar,
    rsi,
    supertrend,
    volume_avg,
    vwap,
)


def make_test_df(n: int = 50, seed: int = 42) -> pd.DataFrame:
    """Generate synthetic OHLCV data."""
    rng = np.random.default_rng(seed)
    base = 50000.0
    timestamps = [
        datetime(2024, 1, 15, 9, 15) + timedelta(minutes=3 * i) for i in range(n)
    ]
    closes = base + np.cumsum(rng.normal(0, 20, n))
    opens = closes + rng.normal(0, 5, n)
    highs = np.maximum(opens, closes) + np.abs(rng.normal(0, 10, n))
    lows = np.minimum(opens, closes) - np.abs(rng.normal(0, 10, n))
    volumes = rng.integers(40000, 100000, n)

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
    return df


class TestRSI:
    def test_rsi_output_range(self):
        df = make_test_df(50)
        out = rsi(df["close"], period=14)
        assert out.min() >= 0
        assert out.max() <= 100

    def test_rsi_length(self):
        df = make_test_df(50)
        out = rsi(df["close"], period=14)
        assert len(out) == len(df)


class TestVWAP:
    def test_vwap_bounded(self):
        df = make_test_df(30)
        out = vwap(df)
        # VWAP should be between min and max of price
        assert out.min() >= df["low"].min() * 0.95
        assert out.max() <= df["high"].max() * 1.05


class TestATR:
    def test_atr_positive(self):
        df = make_test_df(30)
        out = atr(df, period=10)
        # After warmup, ATR should be > 0
        assert (out.dropna() > 0).all()


class TestSupertrend:
    def test_supertrend_direction_values(self):
        df = make_test_df(50)
        st, direction = supertrend(df, period=10, multiplier=2.0)
        # Direction should be +1 or -1
        assert set(direction.unique()).issubset({1.0, -1.0})
        assert len(st) == len(df)

    def test_supertrend_trending_up(self):
        # Strongly trending-up series -> direction should mostly be +1
        timestamps = [datetime(2024, 1, 15, 9, 15) + timedelta(minutes=3 * i) for i in range(50)]
        closes = np.linspace(50000, 50500, 50)
        df = pd.DataFrame({
            "open": closes - 5,
            "high": closes + 10,
            "low": closes - 10,
            "close": closes,
            "volume": [60000] * 50,
        }, index=pd.DatetimeIndex(timestamps))

        _, direction = supertrend(df, period=10, multiplier=2.0)
        # After warmup, mostly uptrend
        assert (direction.iloc[20:] == 1.0).sum() > (direction.iloc[20:] == -1.0).sum()


class TestPSAR:
    def test_psar_output_shape(self):
        df = make_test_df(50)
        psar_vals, direction = psar(df, acceleration=0.02, max_acceleration=0.2)
        assert len(psar_vals) == len(df)
        assert set(direction.unique()).issubset({1.0, -1.0, 1, -1})


class TestComputeAllIndicators:
    def test_all_columns_present(self):
        df = make_test_df(60)
        out = compute_all_indicators(df)
        for col in [
            "rsi",
            "vwap",
            "supertrend",
            "supertrend_dir",
            "psar",
            "psar_dir",
            "volume_avg",
            "volume_ratio",
        ]:
            assert col in out.columns, f"Missing column: {col}"

    def test_empty_df(self):
        df = pd.DataFrame()
        out = compute_all_indicators(df)
        assert out.empty
