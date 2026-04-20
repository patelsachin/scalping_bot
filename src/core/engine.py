"""Main trading engine. Orchestrates signal generation, execution, and risk management."""
from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.broker.base import BrokerBase
from src.broker.kite_broker import KiteBroker
from src.broker.paper_broker import PaperBroker
from src.core.models import (
    ExitReason,
    Signal,
    SignalStrength,
    Trade,
    TradeStatus,
    TradeType,
)
from src.core.state import state
from src.indicators.technical import compute_all_indicators
from src.risk.risk_manager import RiskManager
from src.strategy.two_candle import PositionalTrendFilter, TwoCandleStrategy
from src.utils.config_loader import config
from src.utils.logger import get_logger
from src.utils.market_calendar import (
    is_market_open,
    is_square_off_time,
    is_trading_day,
    now_ist,
)
from src.utils.trade_logger import TradeLogger

log = get_logger(__name__)


class TradingEngine:
    """Main bot loop. Polls candles, evaluates signals, manages trades."""

    def __init__(self) -> None:
        self.strategy = TwoCandleStrategy()
        self.trend_filter = PositionalTrendFilter()
        self.risk = RiskManager()
        self.trade_logger = TradeLogger()
        self.broker: BrokerBase = self._init_broker()

        self.underlying = config.get("instrument.symbol", "BANKNIFTY")
        self.exchange = config.get("instrument.exchange", "NFO")
        self.primary_interval = config.get("timeframe.primary", "3minute")
        self.positional_interval = config.get("timeframe.positional", "15minute")
        self.warmup_candles = int(config.get("timeframe.warmup_candles", 1))

        self.cooldown_candles = int(config.get("trade_rules.cooldown_candles", 2))
        self.min_time_between = int(
            config.get("trade_rules.min_time_between_trades_sec", 30)
        )

        # Tracking
        self._last_trade_time: Optional[datetime] = None
        self._cooldown_until: Optional[datetime] = None
        self._last_evaluated_candle: Optional[datetime] = None

        # Init state
        state.mode = "paper" if config.is_paper_mode() else "live"
        state.daily_budget = self.risk.daily_budget
        state.capital_available = self.risk.daily_budget
        state.started_at = now_ist()

    def _init_broker(self) -> BrokerBase:
        if config.is_paper_mode():
            log.info("Starting in PAPER trading mode.")
            return PaperBroker()
        log.info("Starting in LIVE trading mode.")
        return KiteBroker()

    # ------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------
    def connect(self) -> bool:
        ok = self.broker.connect()
        state.connected = ok
        if not ok and not config.is_paper_mode():
            log.error("Broker connection failed in live mode. Halting.")
            state.halt("Broker connection failed")
        return ok

    # ------------------------------------------------------------
    # Data fetching
    # ------------------------------------------------------------
    def fetch_candles(self, interval: str, lookback_minutes: int = 120) -> pd.DataFrame:
        """Fetch recent candles for the underlying index (NOT the option)."""
        to_dt = now_ist().replace(tzinfo=None)
        from_dt = to_dt - timedelta(minutes=lookback_minutes)

        # For BankNifty underlying we use the index symbol
        symbol = "NIFTY BANK" if self.underlying == "BANKNIFTY" else "NIFTY 50"

        df = self.broker.get_historical_candles(symbol, interval, from_dt, to_dt)
        return df

    def prepare_candles(self, interval_minutes: int = 3) -> Optional[pd.DataFrame]:
        """Fetch and attach indicators to primary timeframe candles."""
        interval_map = {3: "3minute", 5: "5minute", 15: "15minute", 60: "60minute"}
        interval = interval_map.get(interval_minutes, self.primary_interval)

        # Need at least 30 candles of history for indicators to stabilise
        lookback = max(120, interval_minutes * 40)
        df = self.fetch_candles(interval, lookback)

        if df.empty or len(df) < 15:
            log.debug(f"Insufficient candle data for {interval} (got {len(df)}).")
            return None

        intraday_cfg = config.get("indicators.intraday", {})
        st_cfg = intraday_cfg.get("supertrend", {})
        psar_cfg = intraday_cfg.get("psar", {})

        df = compute_all_indicators(
            df,
            st_period=st_cfg.get("period", 10),
            st_multiplier=st_cfg.get("multiplier", 2),
            rsi_period=intraday_cfg.get("rsi", {}).get("period", 14),
            psar_acc=psar_cfg.get("acceleration", 0.02),
            psar_max=psar_cfg.get("max_acceleration", 0.2),
        )
        state.last_candle_time = df.index[-1].to_pydatetime() if len(df) else None
        return df

    def prepare_positional_candles(self) -> Optional[pd.DataFrame]:
        """Fetch 15-min candles for trend filter."""
        df = self.fetch_candles(self.positional_interval, 600)
        if df.empty or len(df) < 10:
            return None
        pos_cfg = config.get("indicators.positional", {})
        st_cfg = pos_cfg.get("supertrend", {})
        df = compute_all_indicators(
            df,
            st_period=st_cfg.get("period", 7),
            st_multiplier=st_cfg.get("multiplier", 3),
            rsi_period=pos_cfg.get("rsi", {}).get("period", 14),
        )
        return df

    # ------------------------------------------------------------
    # Entry / exit execution
    # ------------------------------------------------------------
    def _build_option_symbol(self, signal: Signal) -> tuple[str, float]:
        """Return (option_tradingsymbol, current_premium)."""
        expiry = getattr(self.broker, "get_current_week_expiry", lambda _: None)(
            self.underlying
        )
        if not expiry:
            log.error("No current week expiry found.")
            return "", 0.0

        atm = self.broker.get_atm_strike(self.underlying)
        if atm == 0:
            log.error("ATM strike lookup failed.")
            return "", 0.0

        option_type = "CE" if signal.trade_type == TradeType.LONG else "PE"
        symbol = self.broker.get_option_symbol(self.underlying, expiry, atm, option_type)
        if not symbol:
            log.error(f"Failed to resolve option symbol for {atm} {option_type}")
            return "", 0.0

        premium = self.broker.get_ltp(symbol)
        return symbol, premium

    def enter_trade(self, signal: Signal, primary_df: pd.DataFrame) -> Optional[Trade]:
        # Available capital = budget - currently deployed
        available = state.daily_budget - state.capital_deployed
        if available <= 0:
            log.info("No capital available for new trade.")
            return None

        symbol, premium = self._build_option_symbol(signal)
        if not symbol or premium <= 0:
            return None

        qty, lots, capital_used = self.risk.compute_position_size(
            signal, premium, available
        )
        if qty == 0:
            return None

        # Structural SL from first confirmation candle
        first_candle = primary_df.iloc[-2]
        # For option premium, SL is in premium terms, not underlying.
        # We use a fixed points SL on the premium (max_risk_points).
        sl_price = self.risk.initial_stop_loss(
            TradeType.LONG,  # always long on premium
            premium,
            first_candle_low=premium - self.risk.max_risk_points,
            first_candle_high=premium + self.risk.max_risk_points,
        )
        target_price = self.risk.initial_target(TradeType.LONG, premium)

        # Place order
        order_id = self.broker.place_order(
            symbol=symbol, quantity=qty, transaction_type="BUY"
        )
        if not order_id:
            log.error("Entry order failed.")
            return None

        trade = Trade(
            symbol=symbol,
            underlying=self.underlying,
            trade_type=signal.trade_type,
            signal_strength=signal.strength,
            entry_time=now_ist(),
            entry_price=premium,
            quantity=qty,
            lots=lots,
            stop_loss=sl_price,
            target=target_price,
            trailing_sl=sl_price,
            max_risk_points=self.risk.max_risk_points,
            capital_used=capital_used,
            status=TradeStatus.OPEN,
            entry_order_id=order_id,
            is_paper=self.broker.is_paper(),
            underlying_entry_price=signal.underlying_price,
        )

        state.add_open_trade(trade)
        self._last_trade_time = trade.entry_time
        state.last_signal_time = trade.entry_time

        log.info(
            f"ENTERED {trade.trade_id} {signal.trade_type.value} {symbol} "
            f"qty={qty} @ ₹{premium:.2f} SL={sl_price:.2f} Tgt={target_price:.2f}"
        )
        return trade

    def exit_trade(self, trade: Trade, reason: ExitReason) -> None:
        if not trade.is_open():
            return

        current_premium = self.broker.get_ltp(trade.symbol)
        if current_premium == 0:
            current_premium = trade.entry_price  # fallback

        order_id = self.broker.exit_order(trade)
        trade.exit_order_id = order_id
        trade.exit_time = now_ist()
        trade.exit_price = current_premium
        trade.exit_quantity = trade.quantity
        trade.exit_reason = reason
        trade.status = TradeStatus.CLOSED
        trade.finalise_pnl()

        state.close_trade(trade)
        self.trade_logger.log_trade(trade)

        log.info(
            f"EXITED {trade.trade_id} @ ₹{current_premium:.2f} reason={reason.value} "
            f"P&L=₹{trade.pnl:.2f} ({trade.pnl_points:+.2f} pts)"
        )

        # Start cooldown
        self._cooldown_until = now_ist() + timedelta(
            minutes=self.cooldown_candles * 3
        )

    # ------------------------------------------------------------
    # Trade monitoring
    # ------------------------------------------------------------
    def monitor_open_trades(self, primary_df: pd.DataFrame) -> None:
        """Update trailing SL, check exits, compute unrealised P&L."""
        if not state.open_trades:
            state.update_unrealised(0.0)
            return

        last_candle = primary_df.iloc[-1] if not primary_df.empty else None
        st_dir = int(last_candle["supertrend_dir"]) if last_candle is not None else None

        total_unrealised = 0.0
        for trade in list(state.open_trades):
            current_premium = self.broker.get_ltp(trade.symbol)
            if current_premium == 0:
                continue

            # Update trailing SL
            new_trail = self.risk.compute_trailing_sl(trade, current_premium)
            if new_trail != trade.trailing_sl:
                trade.trailing_sl = new_trail
                log.debug(f"{trade.trade_id} trailing SL -> {new_trail:.2f}")

            # Update unrealised P&L - for long option premium
            trade.pnl_points = current_premium - trade.entry_price
            trade.pnl = trade.pnl_points * trade.quantity
            total_unrealised += trade.pnl

            # Check exits — for options we evaluate premium moves only.
            # Trade type determines the VIEW but we are always long the premium.
            sl_check = trade.trailing_sl if trade.trailing_sl else trade.stop_loss
            exit_reason: Optional[ExitReason] = None

            if current_premium <= sl_check:
                exit_reason = (
                    ExitReason.TRAILING_SL
                    if trade.trailing_sl != trade.stop_loss
                    else ExitReason.STOP_LOSS
                )
            elif current_premium >= trade.target:
                exit_reason = ExitReason.TARGET_HIT
            elif self.risk.exit_on_supertrend_flip and st_dir is not None:
                if trade.trade_type == TradeType.LONG and st_dir == -1:
                    exit_reason = ExitReason.SUPERTREND_FLIP
                elif trade.trade_type == TradeType.SHORT and st_dir == 1:
                    exit_reason = ExitReason.SUPERTREND_FLIP

            if exit_reason:
                self.exit_trade(trade, exit_reason)

        # Recompute from currently open trades only (closed trades already settled into realised_pnl)
        state.update_unrealised(sum(t.pnl for t in state.open_trades))

    # ------------------------------------------------------------
    # Square off / gap checks
    # ------------------------------------------------------------
    def square_off_all(self, reason: ExitReason) -> None:
        for trade in list(state.open_trades):
            self.exit_trade(trade, reason)

    def check_gap_open(self) -> None:
        """At market open, check each open (carried) trade for gap-SL breach.
        Normally no trades carry overnight, but this is a safety net.
        """
        for trade in list(state.open_trades):
            current = self.broker.get_ltp(trade.symbol)
            if current == 0:
                continue
            if self.risk.should_gap_exit(trade, current):
                log.warning(f"Gap protection triggered for {trade.trade_id}")
                self.exit_trade(trade, ExitReason.GAP_PROTECTION)

    # ------------------------------------------------------------
    # Cooldown check
    # ------------------------------------------------------------
    def in_cooldown(self) -> bool:
        if self._cooldown_until and now_ist() < self._cooldown_until:
            return True
        if self._last_trade_time:
            elapsed = (now_ist() - self._last_trade_time).total_seconds()
            if elapsed < self.min_time_between:
                return True
        return False

    # ------------------------------------------------------------
    # Core loop iteration
    # ------------------------------------------------------------
    def iterate(self) -> None:
        """Single iteration of the main loop."""
        # 1. Daily loss check
        if self.risk.is_daily_loss_breached(state.realised_pnl):
            if not state.halted:
                state.halt("Daily loss limit hit")
                self.square_off_all(ExitReason.DAILY_LOSS_HIT)
            return

        # 2. Square-off time check
        if is_square_off_time():
            if state.open_trades:
                log.info("Square-off time reached. Closing all positions.")
                self.square_off_all(ExitReason.SQUARE_OFF_EOD)
            if not state.halted:
                state.halt("End of trading day")
            return

        # 3. Market open check
        if not is_market_open():
            return

        # 4. Fetch candles
        primary_df = self.prepare_candles(3)
        if primary_df is None or len(primary_df) < self.warmup_candles + 2:
            return

        # 5. Monitor existing trades (always runs, even during cooldown)
        self.monitor_open_trades(primary_df)

        # 6. Cooldown / halt guard for new entries
        if state.halted or self.in_cooldown():
            return

        # 7. Only evaluate new signals on freshly completed candles
        last_candle_time = primary_df.index[-1].to_pydatetime()
        if self._last_evaluated_candle == last_candle_time:
            return
        self._last_evaluated_candle = last_candle_time

        # 8. Strategy evaluation
        signal = self.strategy.evaluate(primary_df, self.underlying)
        if signal is None:
            return

        # 9. Skip WEAK signals (per requirement: only STRONG + MEDIUM)
        if signal.strength == SignalStrength.WEAK:
            log.debug("Weak signal ignored.")
            return

        # 10. Trend filter for re-entry
        if self._cooldown_until and now_ist() >= self._cooldown_until:
            positional_df = self.prepare_positional_candles()
            if positional_df is not None:
                if not self.trend_filter.trend_agrees(positional_df, signal.trade_type):
                    log.info(
                        f"Re-entry blocked: 15-min trend doesn't agree with {signal.trade_type.value}"
                    )
                    return
            self._cooldown_until = None  # clear once checked

        # 11. Enter trade
        self.enter_trade(signal, primary_df)

        # 12. Refresh spot price for dashboard
        spot = getattr(self.broker, "get_underlying_ltp", lambda _: 0)(self.underlying)
        if spot > 0:
            state.underlying_ltp = spot
            state.atm_strike = self.broker.get_atm_strike(self.underlying)

    # ------------------------------------------------------------
    # Run
    # ------------------------------------------------------------
    def run(self, poll_interval_sec: int = 5) -> None:
        """Main loop. Runs forever until interrupted."""
        log.info("=" * 60)
        log.info("Siva Scalping Bot starting up")
        log.info(f"Mode: {state.mode.upper()}")
        log.info(f"Daily budget: ₹{state.daily_budget:,.2f}")
        log.info(f"Underlying: {self.underlying}")
        log.info("=" * 60)

        if not self.connect():
            if not config.is_paper_mode():
                return

        if not is_trading_day():
            log.info("Not a trading day. Exiting.")
            return

        # Initial gap check (in case of carried positions - edge case)
        self.check_gap_open()

        try:
            while True:
                try:
                    self.iterate()
                except Exception as e:
                    log.exception(f"Error in main iteration: {e}")
                    state.last_error = str(e)
                time.sleep(poll_interval_sec)
        except KeyboardInterrupt:
            log.info("Received shutdown signal. Squaring off all positions.")
            self.square_off_all(ExitReason.MANUAL)
            log.info("Shutdown complete.")
