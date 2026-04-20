"""Two Candle Theory - core signal generation engine.

Based on Sivakumar Jayachandran's scalping system.

LONG signal:
  - 2 consecutive green candles
  - Volume > threshold on both
  - RSI between 50 and 80
  - Price above VWAP
  - SuperTrend green (direction = 1)
  - PSAR dots below candle (dir = 1)

SHORT signal:
  - 2 consecutive red candles
  - Volume > threshold on both
  - RSI between 20 and 50
  - Price below VWAP
  - SuperTrend red (direction = -1)
  - PSAR dots above candle (dir = -1)
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from src.core.models import Signal, SignalStrength, TradeType
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


class TwoCandleStrategy:
    """Evaluates the Two Candle Theory on a candle dataframe."""

    def __init__(self) -> None:
        ind = config.get("indicators.intraday", {})
        self.rsi_overbought = ind.get("rsi", {}).get("overbought", 80)
        self.rsi_oversold = ind.get("rsi", {}).get("oversold", 20)
        self.rsi_long_min = ind.get("rsi", {}).get("long_min", 50)
        self.rsi_short_max = ind.get("rsi", {}).get("short_max", 50)

        self.vol_threshold_bn = ind.get("volume", {}).get("banknifty_threshold", 50000)
        self.vol_threshold_nifty = ind.get("volume", {}).get("nifty_threshold", 125000)

    def _volume_threshold(self, underlying: str) -> int:
        if "NIFTY" in underlying.upper() and "BANK" not in underlying.upper():
            return self.vol_threshold_nifty
        return self.vol_threshold_bn

    def evaluate(self, df: pd.DataFrame, underlying: str = "BANKNIFTY") -> Optional[Signal]:
        """Evaluate the last 2 candles. Returns a Signal if conditions met, else None.
        Requires df with indicator columns (see technical.compute_all_indicators).
        """
        if len(df) < 3:
            return None

        # We use the last two COMPLETED candles. The very last row may be the current forming candle.
        # Convention: df[-1] is the most recent COMPLETED candle.
        c1 = df.iloc[-2]  # first of the two
        c2 = df.iloc[-1]  # second (more recent)

        vol_threshold = self._volume_threshold(underlying)

        # ---------- LONG evaluation ----------
        long_conditions = {
            "two_green": c1["close"] > c1["open"] and c2["close"] > c2["open"],
            "volume_ok": c1["volume"] >= vol_threshold and c2["volume"] >= vol_threshold,
            "rsi_range": self.rsi_long_min <= c2["rsi"] < self.rsi_overbought,
            "above_vwap": c2["close"] > c2["vwap"],
            "supertrend_buy": c2["supertrend_dir"] == 1,
            "psar_below": c2["psar_dir"] == 1,
        }

        long_met = sum(long_conditions.values())

        # ---------- SHORT evaluation ----------
        short_conditions = {
            "two_red": c1["close"] < c1["open"] and c2["close"] < c2["open"],
            "volume_ok": c1["volume"] >= vol_threshold and c2["volume"] >= vol_threshold,
            "rsi_range": self.rsi_oversold < c2["rsi"] <= self.rsi_short_max,
            "below_vwap": c2["close"] < c2["vwap"],
            "supertrend_sell": c2["supertrend_dir"] == -1,
            "psar_above": c2["psar_dir"] == -1,
        }

        short_met = sum(short_conditions.values())

        signal: Optional[Signal] = None

        # Directional exclusivity: pick whichever direction has more conditions met
        if long_met >= 5 and long_met > short_met:
            signal = self._build_signal(
                c2, underlying, TradeType.LONG, long_conditions, long_met
            )
        elif short_met >= 5 and short_met > long_met:
            signal = self._build_signal(
                c2, underlying, TradeType.SHORT, short_conditions, short_met
            )

        return signal

    def _build_signal(
        self,
        candle: pd.Series,
        underlying: str,
        trade_type: TradeType,
        conditions: dict[str, bool],
        count: int,
    ) -> Signal:
        # Strength grading
        vol_ratio = float(candle.get("volume_ratio", 1.0))
        if count == 6 and vol_ratio >= 1.5:
            strength = SignalStrength.STRONG
        elif count == 6:
            strength = SignalStrength.STRONG
        elif count == 5:
            strength = SignalStrength.MEDIUM
        else:
            strength = SignalStrength.WEAK

        reasons = [k for k, v in conditions.items() if v]
        failed = [k for k, v in conditions.items() if not v]

        sig = Signal(
            timestamp=candle.name if isinstance(candle.name, datetime) else datetime.now(),
            trade_type=trade_type,
            strength=strength,
            underlying=underlying,
            underlying_price=float(candle["close"]),
            reasons=reasons,
            conditions_met=count,
            volume_ratio=vol_ratio,
        )

        log.info(
            f"SIGNAL {trade_type.value} [{strength.value}] {underlying} @ {sig.underlying_price:.2f} "
            f"| {count}/6 conditions | vol_ratio={vol_ratio:.2f} | failed={failed}"
        )
        return sig


class PositionalTrendFilter:
    """Checks the 15-min SuperTrend for re-entry confirmation."""

    def trend_agrees(self, df_15min: pd.DataFrame, trade_type: TradeType) -> bool:
        """Returns True if the 15-min SuperTrend direction agrees with the intended trade."""
        if df_15min is None or df_15min.empty:
            log.warning("No 15-min data for trend filter; allowing trade.")
            return True

        last = df_15min.iloc[-1]
        st_dir = last.get("supertrend_dir", 0)

        if trade_type == TradeType.LONG and st_dir == 1:
            return True
        if trade_type == TradeType.SHORT and st_dir == -1:
            return True
        return False
