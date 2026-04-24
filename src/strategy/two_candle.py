"""Two Candle Theory - core signal generation engine (scalping strategy).

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

Exit: SuperTrend direction flip on candle close.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from src.core.models import ExitReason, Signal, SignalStrength, Trade, TradeType
from src.indicators.technical import compute_all_indicators
from src.strategy.base import StrategyBase
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)

_INTERVAL_MAP = {
    "1minute": 1, "3minute": 3, "5minute": 5,
    "15minute": 15, "60minute": 60,
}


class TwoCandleStrategy(StrategyBase):
    """Evaluates the Two Candle Theory on a candle dataframe."""

    # ------------------------------------------------------------------
    # StrategyBase interface
    # ------------------------------------------------------------------
    @property
    def name(self) -> str:
        return "scalping"

    @property
    def timeframe_minutes(self) -> int:
        tf = config.get("scalping.timeframe", "3minute")
        return _INTERVAL_MAP.get(tf, 3)

    @property
    def seed_lookback_minutes(self) -> int:
        return 90  # ~30 candles on 3-min — ample for SuperTrend / PSAR warmup

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Compute SuperTrend, RSI, VWAP, PSAR and volume averages."""
        cfg      = config.get("scalping", {})
        st_cfg   = cfg.get("supertrend", {})
        psar_cfg = cfg.get("psar", {})
        rsi_cfg  = cfg.get("rsi", {})
        return compute_all_indicators(
            df,
            st_period     = int(st_cfg.get("period", 10)),
            st_multiplier = float(st_cfg.get("multiplier", 3)),
            rsi_period    = int(rsi_cfg.get("period", 14)),
            psar_acc      = float(psar_cfg.get("acceleration", 0.02)),
            psar_max      = float(psar_cfg.get("max_acceleration", 0.2)),
        )

    def exit_signal(self, trade: Trade, df: pd.DataFrame) -> Optional[ExitReason]:
        """Exit when SuperTrend flips against the open trade direction."""
        if not config.get("stop_loss.trailing.exit_on_supertrend_flip", True):
            return None
        if df.empty:
            return None

        st_dir = int(df.iloc[-1].get("supertrend_dir", 0))
        if trade.trade_type == TradeType.LONG and st_dir == -1:
            log.info(f"SuperTrend flip exit triggered for {trade.trade_id} (LONG → bearish)")
            return ExitReason.SUPERTREND_FLIP
        if trade.trade_type == TradeType.SHORT and st_dir == 1:
            log.info(f"SuperTrend flip exit triggered for {trade.trade_id} (SHORT → bullish)")
            return ExitReason.SUPERTREND_FLIP
        return None

    # ------------------------------------------------------------------
    # Signal evaluation
    # ------------------------------------------------------------------
    def __init__(self) -> None:
        self._refresh_config()

    def _refresh_config(self) -> None:
        """Read signal-gate parameters from config. Called at init and can be
        called again after a hot-reload to pick up new RSI/volume thresholds."""
        cfg = config.get("scalping", {})
        rsi = cfg.get("rsi", {})
        vol = cfg.get("volume", {})

        self.rsi_overbought    = float(rsi.get("overbought", 80))
        self.rsi_oversold      = float(rsi.get("oversold", 20))
        self.rsi_long_min      = float(rsi.get("long_min", 50))
        self.rsi_short_max     = float(rsi.get("short_max", 50))
        self.vol_threshold_bn  = int(vol.get("banknifty_threshold", 0))
        self.vol_threshold_nifty = int(vol.get("nifty_threshold", 0))

    def _volume_threshold(self, underlying: str) -> int:
        if "NIFTY" in underlying.upper() and "BANK" not in underlying.upper():
            return self.vol_threshold_nifty
        return self.vol_threshold_bn

    def evaluate(self, df: pd.DataFrame, underlying: str = "BANKNIFTY") -> Optional[Signal]:
        """Evaluate the last 2 candles. Returns a Signal if conditions met, else None.
        Requires df with indicator columns (see compute_indicators above).
        """
        if len(df) < 3:
            return None

        # We use the last two COMPLETED candles.
        c1 = df.iloc[-2]  # first of the two
        c2 = df.iloc[-1]  # second (more recent)

        vol_threshold = self._volume_threshold(underlying)
        close2  = float(c2["close"])
        vwap2   = float(c2.get("vwap", close2))

        # --- VWAP hard directional gate ---
        # Trading a CE (LONG) while price is below VWAP means we're fighting the
        # intraday trend. Trading a PE (SHORT) while price is above VWAP is the
        # same mistake. Block both unconditionally — do not just score them.
        long_vwap_ok  = close2 > vwap2
        short_vwap_ok = close2 < vwap2

        if not long_vwap_ok and not short_vwap_ok:
            # Price exactly at VWAP — ambiguous, skip
            log.debug(f"VWAP gate: price {close2:.2f} == VWAP {vwap2:.2f} — no trade")
            return None

        # ---------- LONG evaluation ----------
        long_conditions = {
            "two_green":      c1["close"] > c1["open"] and c2["close"] > c2["open"],
            "volume_ok":      c1["volume"] >= vol_threshold and c2["volume"] >= vol_threshold,
            "rsi_range":      self.rsi_long_min <= c2["rsi"] < self.rsi_overbought,
            "above_vwap":     long_vwap_ok,
            "supertrend_buy": c2["supertrend_dir"] == 1,
            "psar_below":     c2["psar_dir"] == 1,
        }

        long_met = sum(long_conditions.values())

        # ---------- SHORT evaluation ----------
        short_conditions = {
            "two_red":         c1["close"] < c1["open"] and c2["close"] < c2["open"],
            "volume_ok":       c1["volume"] >= vol_threshold and c2["volume"] >= vol_threshold,
            "rsi_range":       self.rsi_oversold < c2["rsi"] <= self.rsi_short_max,
            "below_vwap":      short_vwap_ok,
            "supertrend_sell": c2["supertrend_dir"] == -1,
            "psar_above":      c2["psar_dir"] == -1,
        }

        short_met = sum(short_conditions.values())

        # Hard VWAP gate: block the wrong direction entirely regardless of score
        if not long_vwap_ok:
            long_met = 0   # LONG blocked — price below VWAP
        if not short_vwap_ok:
            short_met = 0  # SHORT blocked — price above VWAP

        signal: Optional[Signal] = None

        # Directional exclusivity: pick whichever direction has more conditions met
        if long_met >= 5 and long_met > short_met:
            signal = self._build_signal(c2, underlying, TradeType.LONG, long_conditions, long_met)
        elif short_met >= 5 and short_met > long_met:
            signal = self._build_signal(c2, underlying, TradeType.SHORT, short_conditions, short_met)

        return signal

    def _build_signal(
        self,
        candle: pd.Series,
        underlying: str,
        trade_type: TradeType,
        conditions: dict[str, bool],
        count: int,
    ) -> Signal:
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
        failed  = [k for k, v in conditions.items() if not v]

        sig = Signal(
            timestamp       = candle.name if isinstance(candle.name, datetime) else datetime.now(),
            trade_type      = trade_type,
            strength        = strength,
            underlying      = underlying,
            underlying_price= float(candle["close"]),
            reasons         = reasons,
            conditions_met  = count,
            volume_ratio    = vol_ratio,
        )

        log.info(
            f"SIGNAL {trade_type.value} [{strength.value}] {underlying} @ {sig.underlying_price:.2f} "
            f"| {count}/6 conditions | vol_ratio={vol_ratio:.2f} | failed={failed}"
        )
        return sig


class PositionalTrendFilter:
    """Checks the 15-min SuperTrend for re-entry confirmation.
    Shared across all strategies — not strategy-specific.
    """

    def trend_agrees(self, df_15min: pd.DataFrame, trade_type: TradeType) -> bool:
        """Returns True if the 15-min SuperTrend direction agrees with the intended trade."""
        if df_15min is None or df_15min.empty:
            log.warning("No 15-min data for trend filter; allowing trade.")
            return True

        last   = df_15min.iloc[-1]
        st_dir = last.get("supertrend_dir", 0)

        if trade_type == TradeType.LONG  and st_dir == 1:
            return True
        if trade_type == TradeType.SHORT and st_dir == -1:
            return True
        return False
