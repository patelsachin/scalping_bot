"""Risk manager: position sizing, stop loss, trailing logic, daily loss cap."""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from src.core.models import (
    ExitReason,
    Signal,
    SignalStrength,
    Trade,
    TradeType,
)
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


class RiskManager:
    """Handles position sizing and risk rules based on config."""

    def __init__(self) -> None:
        self.daily_budget = float(config.get("capital.daily_budget", 100000))
        self.max_daily_loss_pct = float(config.get("capital.max_daily_loss_pct", 1.0))
        self.max_daily_loss = self.daily_budget * self.max_daily_loss_pct / 100.0

        alloc = config.get("capital.allocation", {})
        self.alloc_strong = float(alloc.get("strong", 30)) / 100.0
        self.alloc_medium = float(alloc.get("medium", 20)) / 100.0
        self.alloc_weak = float(alloc.get("weak", 0)) / 100.0

        self.lot_size = int(config.get("instrument.lot_size", 15))
        self.max_lots = int(config.get("instrument.max_lots_per_trade", 3))

        self.target_points = float(config.get("stop_loss.target_points", 10))
        self.max_risk_points = float(config.get("stop_loss.max_risk_points", 20))

        trail_cfg = config.get("stop_loss.trailing", {})
        self.trail_enabled = trail_cfg.get("enabled", True)
        self.trail_step = float(trail_cfg.get("points_trail_step", 5))
        self.trail_activation = float(trail_cfg.get("activation_profit_pts", 10))
        self.trail_activation_strong = float(
            trail_cfg.get("strong_activation_profit_pts",
                          trail_cfg.get("activation_profit_pts", 10))
        )
        self.exit_on_supertrend_flip = trail_cfg.get("exit_on_supertrend_flip", True)

    # ------------------------------------------------------------
    # Position sizing
    # ------------------------------------------------------------
    def allocation_pct_for(self, strength: SignalStrength) -> float:
        mapping = {
            SignalStrength.STRONG: self.alloc_strong,
            SignalStrength.MEDIUM: self.alloc_medium,
            SignalStrength.WEAK: self.alloc_weak,
        }
        return mapping.get(strength, 0.0)

    def compute_position_size(
        self,
        signal: Signal,
        option_premium: float,
        available_capital: float,
    ) -> tuple[int, int, float]:
        """Return (quantity, lots, capital_used) based on signal strength and available capital.
        - quantity: total shares (lots * lot_size)
        - lots: number of lots
        - capital_used: entry premium outflow
        """
        alloc_pct = self.allocation_pct_for(signal.strength)
        if alloc_pct <= 0:
            return 0, 0, 0.0

        # Budget allocated for this trade
        trade_budget = min(self.daily_budget * alloc_pct, available_capital)

        if option_premium <= 0:
            log.warning(f"Invalid option premium {option_premium}; cannot size position.")
            return 0, 0, 0.0

        # Cost per lot
        cost_per_lot = option_premium * self.lot_size

        max_lots_budget = int(trade_budget // cost_per_lot)
        if max_lots_budget == 0:
            log.info(
                f"Insufficient capital for even 1 lot at premium {option_premium:.2f} "
                f"(need {cost_per_lot:.2f}, have {trade_budget:.2f})"
            )
            return 0, 0, 0.0

        lots = min(max_lots_budget, self.max_lots)
        quantity = lots * self.lot_size
        capital_used = quantity * option_premium

        log.info(
            f"Position size: {lots} lot(s) = {quantity} qty @ ₹{option_premium:.2f} "
            f"= ₹{capital_used:.2f} (budget: ₹{trade_budget:.2f})"
        )
        return quantity, lots, capital_used

    # ------------------------------------------------------------
    # Stop loss & target
    # ------------------------------------------------------------
    def initial_stop_loss(
        self,
        trade_type: TradeType,
        entry_price: float,
        first_candle_low: float,
        first_candle_high: float,
    ) -> float:
        """Initial SL based on Two Candle Theory:
        - LONG  -> below low of first candle
        - SHORT -> above high of first candle
        Capped by max_risk_points.
        """
        if trade_type == TradeType.LONG:
            structural_sl = first_candle_low
            max_risk_sl = entry_price - self.max_risk_points
            sl = max(structural_sl, max_risk_sl)
        else:
            structural_sl = first_candle_high
            max_risk_sl = entry_price + self.max_risk_points
            sl = min(structural_sl, max_risk_sl)

        return round(sl, 2)

    def initial_target(self, trade_type: TradeType, entry_price: float) -> float:
        if trade_type == TradeType.LONG:
            return round(entry_price + self.target_points, 2)
        return round(entry_price - self.target_points, 2)

    # ------------------------------------------------------------
    # Trailing SL logic
    # ------------------------------------------------------------
    def compute_trailing_sl(self, trade: Trade, current_price: float) -> float:
        """Trail SL upward (for long) / downward (for short) in `trail_step` increments
        once in profit by `trail_activation` points.

        STRONG signals get a wider activation threshold (strong_activation_profit_pts)
        so 3-lot positions have more room to breathe before the trail kicks in.

        Returns the new SL (never worse than current).
        """
        if not self.trail_enabled:
            return trade.stop_loss

        current_sl = trade.trailing_sl if trade.trailing_sl else trade.stop_loss

        # Pick activation threshold based on signal strength
        activation = (
            self.trail_activation_strong
            if trade.signal_strength == SignalStrength.STRONG
            else self.trail_activation
        )

        if trade.trade_type == TradeType.LONG:
            profit_pts = current_price - trade.entry_price
            if profit_pts < activation:
                return current_sl
            # How many `trail_step` increments above the activation point?
            steps = int((profit_pts - activation) // self.trail_step) + 1
            new_sl = trade.entry_price + (steps - 1) * self.trail_step
            return round(max(new_sl, current_sl), 2)
        else:
            profit_pts = trade.entry_price - current_price
            if profit_pts < activation:
                return current_sl
            steps = int((profit_pts - activation) // self.trail_step) + 1
            new_sl = trade.entry_price - (steps - 1) * self.trail_step
            return round(min(new_sl, current_sl), 2)

    # ------------------------------------------------------------
    # Exit evaluation
    # ------------------------------------------------------------
    def check_exit(
        self,
        trade: Trade,
        current_price: float,
        supertrend_dir: Optional[int] = None,
    ) -> Optional[ExitReason]:
        """Decide whether to exit. Returns the ExitReason or None."""
        if not trade.is_open():
            return None

        sl = trade.trailing_sl if trade.trailing_sl else trade.stop_loss

        if trade.trade_type == TradeType.LONG:
            if current_price <= sl:
                return ExitReason.TRAILING_SL if trade.trailing_sl != trade.stop_loss else ExitReason.STOP_LOSS
            if current_price >= trade.target:
                return ExitReason.TARGET_HIT
        else:  # SHORT
            if current_price >= sl:
                return ExitReason.TRAILING_SL if trade.trailing_sl != trade.stop_loss else ExitReason.STOP_LOSS
            if current_price <= trade.target:
                return ExitReason.TARGET_HIT

        # SuperTrend flip exit
        if self.exit_on_supertrend_flip and supertrend_dir is not None:
            if trade.trade_type == TradeType.LONG and supertrend_dir == -1:
                return ExitReason.SUPERTREND_FLIP
            if trade.trade_type == TradeType.SHORT and supertrend_dir == 1:
                return ExitReason.SUPERTREND_FLIP

        return None

    # ------------------------------------------------------------
    # Gap protection
    # ------------------------------------------------------------
    def should_gap_exit(
        self, trade: Trade, market_open_price: float
    ) -> bool:
        """At market open, if the gap has already breached SL, exit immediately."""
        gap_threshold = float(config.get("gap_protection.gap_threshold_pct", 0.5)) / 100.0

        sl = trade.trailing_sl if trade.trailing_sl else trade.stop_loss

        # For options (premium-based), long positions always lose when premium drops
        # regardless of call/put. The trade.trade_type represents view on underlying.
        if trade.trade_type == TradeType.LONG:
            if market_open_price <= sl:
                return True
            gap_pct = (trade.entry_price - market_open_price) / trade.entry_price
            if gap_pct >= gap_threshold:
                return True
        else:
            if market_open_price >= sl:
                return True
            gap_pct = (market_open_price - trade.entry_price) / trade.entry_price
            if gap_pct >= gap_threshold:
                return True

        return False

    # ------------------------------------------------------------
    # Daily loss
    # ------------------------------------------------------------
    def is_daily_loss_breached(self, realised_pnl: float) -> bool:
        """True if realised P&L loss has exceeded the daily cap."""
        if realised_pnl <= -self.max_daily_loss:
            log.warning(
                f"DAILY LOSS LIMIT HIT: realised={realised_pnl:.2f}, max_loss={self.max_daily_loss:.2f}"
            )
            return True
        return False
