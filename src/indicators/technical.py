"""Technical indicator calculators - VWAP, SuperTrend, RSI, PSAR, Volume avg."""
from __future__ import annotations

import numpy as np
import pandas as pd


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Relative Strength Index using Wilder's smoothing."""
    delta = series.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi_series = 100.0 - (100.0 / (1.0 + rs))
    return rsi_series.fillna(50.0)


def vwap(df: pd.DataFrame) -> pd.Series:
    """Session-anchored VWAP. Resets every trading day.
    Expects columns: high, low, close, volume, and a DatetimeIndex.
    """
    tp = (df["high"] + df["low"] + df["close"]) / 3.0
    tpv = tp * df["volume"]

    # Group by date for daily reset
    grouped = df.groupby(df.index.date)
    cum_tpv = grouped["volume"].apply(lambda s: (tpv.loc[s.index]).cumsum())
    cum_vol = grouped["volume"].cumsum()

    # Align indices
    if isinstance(cum_tpv.index, pd.MultiIndex):
        cum_tpv = cum_tpv.reset_index(level=0, drop=True)

    vwap_series = cum_tpv / cum_vol.replace(0, np.nan)
    return vwap_series.fillna(method="ffill").fillna(df["close"])


def atr(df: pd.DataFrame, period: int = 10) -> pd.Series:
    """Average True Range."""
    high = df["high"]
    low = df["low"]
    prev_close = df["close"].shift(1)

    tr1 = high - low
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    return true_range.ewm(alpha=1.0 / period, min_periods=period, adjust=False).mean()


def supertrend(
    df: pd.DataFrame, period: int = 10, multiplier: float = 2.0
) -> tuple[pd.Series, pd.Series]:
    """SuperTrend indicator.
    Returns (supertrend_values, direction) where direction = 1 for uptrend, -1 for downtrend.
    """
    atr_values = atr(df, period)
    hl2 = (df["high"] + df["low"]) / 2.0

    upper_basic = hl2 + multiplier * atr_values
    lower_basic = hl2 - multiplier * atr_values

    upper_band = upper_basic.copy()
    lower_band = lower_basic.copy()
    direction = pd.Series(index=df.index, dtype=float)
    st = pd.Series(index=df.index, dtype=float)

    close = df["close"]
    n = len(df)

    for i in range(n):
        if i == 0:
            direction.iloc[i] = 1.0
            st.iloc[i] = lower_band.iloc[i]
            continue

        # Adjust bands
        if close.iloc[i - 1] > upper_band.iloc[i - 1]:
            upper_band.iloc[i] = min(upper_basic.iloc[i], upper_band.iloc[i - 1])
        else:
            upper_band.iloc[i] = upper_basic.iloc[i]

        if close.iloc[i - 1] < lower_band.iloc[i - 1]:
            lower_band.iloc[i] = max(lower_basic.iloc[i], lower_band.iloc[i - 1])
        else:
            lower_band.iloc[i] = lower_basic.iloc[i]

        # Determine direction
        prev_dir = direction.iloc[i - 1]
        if prev_dir == 1.0:
            if close.iloc[i] < lower_band.iloc[i]:
                direction.iloc[i] = -1.0
                st.iloc[i] = upper_band.iloc[i]
            else:
                direction.iloc[i] = 1.0
                st.iloc[i] = lower_band.iloc[i]
        else:
            if close.iloc[i] > upper_band.iloc[i]:
                direction.iloc[i] = 1.0
                st.iloc[i] = lower_band.iloc[i]
            else:
                direction.iloc[i] = -1.0
                st.iloc[i] = upper_band.iloc[i]

    return st, direction


def psar(
    df: pd.DataFrame, acceleration: float = 0.02, max_acceleration: float = 0.2
) -> tuple[pd.Series, pd.Series]:
    """Parabolic SAR (Stop and Reverse).
    Returns (psar_values, trend) where trend = 1 for up (dots below), -1 for down (dots above).
    """
    high = df["high"].values
    low = df["low"].values
    n = len(df)

    psar_arr = np.zeros(n)
    trend = np.zeros(n)
    ep = np.zeros(n)
    af = np.zeros(n)

    # Initialize
    trend[0] = 1
    psar_arr[0] = low[0]
    ep[0] = high[0]
    af[0] = acceleration

    for i in range(1, n):
        prev_trend = trend[i - 1]
        prev_psar = psar_arr[i - 1]
        prev_ep = ep[i - 1]
        prev_af = af[i - 1]

        # Tentative PSAR
        new_psar = prev_psar + prev_af * (prev_ep - prev_psar)

        if prev_trend == 1:
            # Uptrend: PSAR can't be above recent lows
            new_psar = min(new_psar, low[i - 1], low[max(i - 2, 0)])
            if low[i] < new_psar:
                # Reversal to down
                trend[i] = -1
                psar_arr[i] = prev_ep
                ep[i] = low[i]
                af[i] = acceleration
            else:
                trend[i] = 1
                psar_arr[i] = new_psar
                if high[i] > prev_ep:
                    ep[i] = high[i]
                    af[i] = min(prev_af + acceleration, max_acceleration)
                else:
                    ep[i] = prev_ep
                    af[i] = prev_af
        else:
            # Downtrend
            new_psar = max(new_psar, high[i - 1], high[max(i - 2, 0)])
            if high[i] > new_psar:
                trend[i] = 1
                psar_arr[i] = prev_ep
                ep[i] = high[i]
                af[i] = acceleration
            else:
                trend[i] = -1
                psar_arr[i] = new_psar
                if low[i] < prev_ep:
                    ep[i] = low[i]
                    af[i] = min(prev_af + acceleration, max_acceleration)
                else:
                    ep[i] = prev_ep
                    af[i] = prev_af

    return pd.Series(psar_arr, index=df.index), pd.Series(trend, index=df.index)


def volume_avg(series: pd.Series, period: int = 20) -> pd.Series:
    """Rolling average volume."""
    return series.rolling(window=period, min_periods=1).mean()


def compute_all_indicators(
    df: pd.DataFrame,
    st_period: int = 10,
    st_multiplier: float = 2.0,
    rsi_period: int = 14,
    psar_acc: float = 0.02,
    psar_max: float = 0.2,
    volume_avg_period: int = 20,
) -> pd.DataFrame:
    """Add all indicator columns to the dataframe.
    Expected input columns: open, high, low, close, volume (and optionally open_interest).
    """
    if df.empty:
        return df

    out = df.copy()
    out["rsi"] = rsi(out["close"], rsi_period)
    out["vwap"] = vwap(out)

    st_vals, st_dir = supertrend(out, st_period, st_multiplier)
    out["supertrend"] = st_vals
    out["supertrend_dir"] = st_dir  # 1 up, -1 down

    psar_vals, psar_dir = psar(out, psar_acc, psar_max)
    out["psar"] = psar_vals
    out["psar_dir"] = psar_dir  # 1 dots below, -1 dots above

    out["volume_avg"] = volume_avg(out["volume"], volume_avg_period)
    out["volume_ratio"] = out["volume"] / out["volume_avg"].replace(0, np.nan)
    out["volume_ratio"] = out["volume_ratio"].fillna(1.0)

    return out
