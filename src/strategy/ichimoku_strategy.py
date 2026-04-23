"""Ichimoku Kinko Hyo strategy for BankNifty options scalping.

Discussion C — Analysis Summary
================================
Applied to the BankNifty futures 1-min chart. Signals are generated on
the underlying (futures) price action and translated into CE/PE option buys.

Entry rules:
  LONG (buy CE — bullish underlying view):
    1. Price ABOVE the Ichimoku cloud (above both cloud_a_now and cloud_b_now)
    2. Cloud ahead is GREEN (cloud_a_now > cloud_b_now) → bullish future momentum
    3. Tenkan-sen ABOVE Kijun-sen (bullish TK alignment)
    4. Kijun-sen is NOT flat — slope exceeds kijun_flat_tolerance_pct
       (flat Kijun = magnet effect, price gravitates back regardless of Tenkan position)
    5. Volume confirmation (volume_ratio >= min_volume_ratio when threshold > 0)

  SHORT (buy PE — bearish underlying view):
    Mirror image of LONG rules.

Exit rules (candle close — Discussion C):
  - LONG  exit: Tenkan crosses BELOW Kijun  OR  price falls into / below cloud
  - SHORT exit: Tenkan crosses ABOVE Kijun  OR  price rises into / above cloud
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

from src.core.models import ExitReason, Signal, SignalStrength, Trade, TradeType
from src.indicators.technical import compute_ichimoku_indicators
from src.strategy.base import StrategyBase
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)

_INTERVAL_MAP = {
    "1minute": 1, "3minute": 3, "5minute": 5,
    "15minute": 15, "60minute": 60,
}

# Minimum candles required before signals fire.
# Senkou B needs senkou_b_period (default 52) candles to warm up.
_MIN_CANDLES = 60


class IchimokuStrategy(StrategyBase):
    """Ichimoku-based scalping strategy."""

    # ------------------------------------------------------------------
    # StrategyBase interface
    # ------------------------------------------------------------------
    @property
    def name(self) -> str:
        return "ichimoku"

    @property
    def timeframe_minutes(self) -> int:
        tf = config.get("ichimoku.timeframe", "1minute")
        return _INTERVAL_MAP.get(tf, 1)

    @property
    def seed_lookback_minutes(self) -> int:
        # On 1-min chart, Senkou B needs 52 candles + displacement (26) = 78 candles.
        # Fetch 120 min of previous session tail to ensure full warmup.
        return 120

    def _cfg(self) -> dict:
        return config.get("ichimoku", {})

    def compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        cfg = self._cfg()
        return compute_ichimoku_indicators(
            df,
            tenkan_period    = int(cfg.get("tenkan_period",    9)),
            kijun_period     = int(cfg.get("kijun_period",    26)),
            senkou_b_period  = int(cfg.get("senkou_b_period", 52)),
            displacement     = int(cfg.get("displacement",    26)),
        )

    # ------------------------------------------------------------------
    # Entry signal
    # ------------------------------------------------------------------
    def evaluate(self, df: pd.DataFrame, underlying: str = "BANKNIFTY") -> Optional[Signal]:
        """Evaluate Ichimoku entry conditions on the last completed candle."""
        if len(df) < _MIN_CANDLES:
            log.debug(
                f"Ichimoku: insufficient history ({len(df)} < {_MIN_CANDLES} candles). "
                f"Accumulating…"
            )
            return None

        cfg = self._cfg()
        require_kijun_slope   = bool(cfg.get("require_kijun_slope", True))
        require_outside_cloud = bool(cfg.get("require_price_outside_cloud", True))
        kijun_tol_pct         = float(cfg.get("kijun_flat_tolerance_pct", 0.05))
        min_vol_ratio         = float(cfg.get("min_volume_ratio", 1.0))

        c2 = df.iloc[-1]   # signal candle (most recent completed)

        # --- Extract Ichimoku values ---
        price_vs_cloud = int(c2.get("price_vs_cloud", 0))
        cloud_color    = int(c2.get("cloud_color",    0))
        tenkan         = float(c2.get("tenkan",        0))
        kijun          = float(c2.get("kijun",         0))
        kijun_slope    = float(c2.get("kijun_slope",   0))
        vol_ratio      = float(c2.get("volume_ratio",  1.0))

        # Kijun flat: slope expressed as % of Kijun value
        kijun_is_flat = (abs(kijun_slope) / (abs(kijun) + 1e-9)) * 100 < kijun_tol_pct

        vol_ok = vol_ratio >= min_vol_ratio

        # ---------- LONG conditions ----------
        long_conditions: dict[str, bool] = {
            "above_cloud":   price_vs_cloud == 1,
            "cloud_green":   cloud_color == 1,
            "tk_bullish":    tenkan > kijun,
            "kijun_rising":  (not kijun_is_flat) and (kijun_slope > 0)
                             if require_kijun_slope else True,
            "outside_cloud": price_vs_cloud != 0 if require_outside_cloud else True,
            "volume_ok":     vol_ok,
        }

        # ---------- SHORT conditions ----------
        short_conditions: dict[str, bool] = {
            "below_cloud":   price_vs_cloud == -1,
            "cloud_red":     cloud_color == -1,
            "tk_bearish":    tenkan < kijun,
            "kijun_falling": (not kijun_is_flat) and (kijun_slope < 0)
                             if require_kijun_slope else True,
            "outside_cloud": price_vs_cloud != 0 if require_outside_cloud else True,
            "volume_ok":     vol_ok,
        }

        long_met  = sum(long_conditions.values())
        short_met = sum(short_conditions.values())

        signal: Optional[Signal] = None
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
        strength = SignalStrength.STRONG if count == 6 else SignalStrength.MEDIUM
        reasons  = [k for k, v in conditions.items() if     v]
        failed   = [k for k, v in conditions.items() if not v]
        vol_ratio = float(candle.get("volume_ratio", 1.0))

        sig = Signal(
            timestamp        = candle.name if isinstance(candle.name, datetime) else datetime.now(),
            trade_type       = trade_type,
            strength         = strength,
            underlying       = underlying,
            underlying_price = float(candle["close"]),
            reasons          = reasons,
            conditions_met   = count,
            volume_ratio     = vol_ratio,
        )
        log.info(
            f"ICHIMOKU SIGNAL {trade_type.value} [{strength.value}] {underlying} "
            f"@ {sig.underlying_price:.2f} | {count}/6 conditions | failed={failed} | "
            f"vol_ratio={vol_ratio:.2f}"
        )
        return sig

    # ------------------------------------------------------------------
    # Candle-close exit (Discussion C)
    # ------------------------------------------------------------------
    def exit_signal(self, trade: Trade, df: pd.DataFrame) -> Optional[ExitReason]:
        """Exit when Tenkan crosses Kijun against trade direction, or price enters the cloud.

        Two conditions checked:
        1. TK cross: Tenkan was on the favourable side of Kijun last candle,
           now it has crossed to the unfavourable side.
        2. Cloud re-entry: price was outside the cloud (price_vs_cloud ≠ 0)
           and has now moved inside (price_vs_cloud = 0) or past it.
        """
        if len(df) < 2:
            return None

        c_prev = df.iloc[-2]
        c_curr = df.iloc[-1]

        tenkan_prev = float(c_prev.get("tenkan", 0))
        kijun_prev  = float(c_prev.get("kijun",  0))
        tenkan_curr = float(c_curr.get("tenkan", 0))
        kijun_curr  = float(c_curr.get("kijun",  0))

        price_vs_cloud_curr = int(c_curr.get("price_vs_cloud", 0))

        if trade.trade_type == TradeType.LONG:
            tk_bearish_cross = (tenkan_prev >= kijun_prev) and (tenkan_curr < kijun_curr)
            cloud_adverse    = price_vs_cloud_curr <= 0  # entered cloud or fell below
            if tk_bearish_cross or cloud_adverse:
                reason_str = "TK bearish cross" if tk_bearish_cross else "price entered/below cloud"
                log.info(
                    f"Ichimoku exit [{reason_str}] for {trade.trade_id} "
                    f"(tenkan={tenkan_curr:.2f}, kijun={kijun_curr:.2f}, "
                    f"price_vs_cloud={price_vs_cloud_curr})"
                )
                return ExitReason.SUPERTREND_FLIP  # reuses existing enum value

        elif trade.trade_type == TradeType.SHORT:
            tk_bullish_cross = (tenkan_prev <= kijun_prev) and (tenkan_curr > kijun_curr)
            cloud_adverse    = price_vs_cloud_curr >= 0  # entered cloud or rose above
            if tk_bullish_cross or cloud_adverse:
                reason_str = "TK bullish cross" if tk_bullish_cross else "price entered/above cloud"
                log.info(
                    f"Ichimoku exit [{reason_str}] for {trade.trade_id} "
                    f"(tenkan={tenkan_curr:.2f}, kijun={kijun_curr:.2f}, "
                    f"price_vs_cloud={price_vs_cloud_curr})"
                )
                return ExitReason.SUPERTREND_FLIP

        return None
