"""Main trading engine — WebSocket-driven event model.

Data flow:
  KiteTicker tick  →  _on_ticks()
      ├── underlying token  →  CandleAggregator  →  _on_candle_close()
      │       └── recompute indicators → evaluate strategy → enter trade
      └── option token      →  _check_sl_on_tick()
              └── trailing SL update + exit if breached

SL / trailing SL are evaluated on every option-premium tick (real-time).
SuperTrend-flip exits are evaluated on every completed 3-min candle close.
A poll-mode fallback is available when WebSocket setup fails.
"""
from __future__ import annotations

import hashlib
import threading
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.broker.base import BrokerBase
from src.broker.kite_broker import KiteBroker
from src.broker.kite_ticker import TickerManager
from src.broker.paper_broker import PaperBroker
from src.core.candle_builder import CandleAggregator
from src.core.market_regime import MarketRegime, MarketRegimeFilter
from src.utils.system_logger import sys_log, EVENT_CONNECTED, EVENT_DISCONNECTED, EVENT_HALT, EVENT_KILL_SWITCH, EVENT_STARTUP, EVENT_SHUTDOWN, EVENT_CONFIG_RELOAD
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
    IST,
    is_market_open,
    is_square_off_time,
    is_trading_day,
    market_close_time,
    market_open_time,
    now_ist,
)
from src.utils.trade_logger import TradeLogger

log = get_logger(__name__)


class TradingEngine:
    """Main bot. Seeded from Kite historical REST, then driven by KiteTicker WebSocket."""

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

        # VIX regime gate
        self.regime_filter = MarketRegimeFilter()
        self._vix_token: Optional[int] = None

        # WebSocket components
        self._underlying_token: Optional[int] = None
        self._ticker: Optional[TickerManager] = None
        self._candle_agg = CandleAggregator(interval_minutes=3)
        self._candle_agg.on_candle_close(self._on_candle_close)

        # Live candle dataframe: seeded from history, extended on each candle close
        self._live_df: Optional[pd.DataFrame] = None
        self._df_lock = threading.Lock()

        # option_token -> Trade: for per-tick SL checks
        self._option_token_to_trade: dict[int, Trade] = {}
        self._engine_lock = threading.Lock()

        # Prevents two concurrent calls to exit_trade() for the same trade
        self._exit_lock = threading.Lock()

        # COID guard: symbol+direction hash → last order time (F7)
        self._coid_cache: dict[str, datetime] = {}

        # Kill switch: set True externally (Ctrl+K) → squares off and halts without exiting
        self._kill_switch_triggered: bool = False

        # Signals shutdown to run()
        self._shutdown = threading.Event()

        # Shared state init
        state.mode = "paper" if config.is_paper_mode() else "live"
        state.daily_budget = self.risk.daily_budget
        state.capital_available = self.risk.daily_budget
        state.started_at = now_ist()

    # ------------------------------------------------------------------
    # Broker init
    # ------------------------------------------------------------------
    def _init_broker(self) -> BrokerBase:
        if config.is_paper_mode():
            log.info("Starting in PAPER trading mode.")
            return PaperBroker()
        log.info("Starting in LIVE trading mode.")
        return KiteBroker()

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------
    def connect(self) -> bool:
        ok = self.broker.connect()
        state.connected = ok
        if not ok and not config.is_paper_mode():
            log.error("Broker connection failed in live mode. Halting.")
            state.halt("Broker connection failed")
        return ok

    # ------------------------------------------------------------------
    # Historical data (REST — used for warmup and positional filter)
    # ------------------------------------------------------------------
    def fetch_candles(self, interval: str, lookback_minutes: int = 120) -> pd.DataFrame:
        to_dt = now_ist().replace(tzinfo=None)
        from_dt = to_dt - timedelta(minutes=lookback_minutes)
        symbol = "NIFTY BANK" if self.underlying == "BANKNIFTY" else "NIFTY 50"
        return self.broker.get_historical_candles(symbol, interval, from_dt, to_dt)

    def prepare_candles(self, interval_minutes: int = 3) -> Optional[pd.DataFrame]:
        interval_map = {3: "3minute", 5: "5minute", 15: "15minute", 60: "60minute"}
        interval = interval_map.get(interval_minutes, self.primary_interval)
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
        df = self.fetch_candles(self.positional_interval, 600)
        if df.empty or len(df) < 10:
            return None
        pos_cfg = config.get("indicators.positional", {})
        st_cfg = pos_cfg.get("supertrend", {})
        return compute_all_indicators(
            df,
            st_period=st_cfg.get("period", 7),
            st_multiplier=st_cfg.get("multiplier", 3),
            rsi_period=pos_cfg.get("rsi", {}).get("period", 14),
        )

    # ------------------------------------------------------------------
    # Instrument token resolution (shared between KiteBroker & PaperBroker)
    # ------------------------------------------------------------------
    def _get_instrument_token(self, symbol: str, exchange: str = "NFO") -> Optional[int]:
        if isinstance(self.broker, KiteBroker):
            return self.broker.get_instrument_token(symbol, exchange)
        if isinstance(self.broker, PaperBroker):
            return self.broker._kite.get_instrument_token(symbol, exchange)
        return None

    # ------------------------------------------------------------------
    # Entry / exit execution
    # ------------------------------------------------------------------
    def _build_option_symbol(self, signal: Signal) -> tuple[str, float]:
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
        available = state.daily_budget - state.capital_deployed
        if available <= 0:
            log.info("No capital available for new trade.")
            return None

        symbol, premium = self._build_option_symbol(signal)
        if not symbol or premium <= 0:
            return None

        # F7 — Duplicate order guard (COID)
        # Prevents the same symbol+direction from firing twice within 60 seconds
        coid_key  = f"{symbol}:{signal.trade_type.value}"
        coid_hash = hashlib.md5(coid_key.encode()).hexdigest()[:8]
        now_ts    = now_ist()
        with self._engine_lock:
            last_coid_time = self._coid_cache.get(coid_hash)
            if last_coid_time and (now_ts - last_coid_time).total_seconds() < 60:
                log.warning(
                    f"COID guard: duplicate order for {coid_key} "
                    f"({(now_ts - last_coid_time).total_seconds():.0f}s ago) — skipped"
                )
                return None
            self._coid_cache[coid_hash] = now_ts

        qty, lots, capital_used = self.risk.compute_position_size(
            signal, premium, available
        )
        if qty == 0:
            return None

        sl_price = self.risk.initial_stop_loss(
            TradeType.LONG,
            premium,
            first_candle_low=premium - self.risk.max_risk_points,
            first_candle_high=premium + self.risk.max_risk_points,
        )
        target_price = self.risk.initial_target(TradeType.LONG, premium)

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

        # Subscribe option token for tick-level SL monitoring
        opt_token = self._get_instrument_token(symbol, "NFO")
        if opt_token and self._ticker is not None:
            with self._engine_lock:
                self._option_token_to_trade[opt_token] = trade
            self._ticker.subscribe([opt_token], mode="full")
            log.info(f"Subscribed option {symbol} (token={opt_token}) for tick SL")

        return trade

    def exit_trade(
        self,
        trade: Trade,
        reason: ExitReason,
        exit_price: Optional[float] = None,
    ) -> None:
        # Atomically mark CLOSED to prevent duplicate exits from concurrent tick handlers
        with self._exit_lock:
            if not trade.is_open():
                return
            trade.status = TradeStatus.CLOSED

        current_premium = exit_price or self.broker.get_ltp(trade.symbol)
        if current_premium == 0:
            current_premium = trade.entry_price

        order_id = self.broker.exit_order(trade)
        if not order_id:
            log.error(f"Exit order failed for {trade.trade_id} — position may still be open!")

        trade.exit_order_id = order_id
        trade.exit_time = now_ist()
        trade.exit_price = current_premium
        trade.exit_quantity = trade.quantity
        trade.exit_reason = reason
        trade.finalise_pnl()

        state.close_trade(trade)
        self.trade_logger.log_trade(trade)

        log.info(
            f"EXITED {trade.trade_id} @ ₹{current_premium:.2f} reason={reason.value} "
            f"P&L=₹{trade.pnl:.2f} ({trade.pnl_points:+.2f} pts)"
        )

        # Unsubscribe and remove from option token map
        self._cleanup_option_subscription(trade.symbol)

        self._cooldown_until = now_ist() + timedelta(
            minutes=self.cooldown_candles * 3
        )

    def _cleanup_option_subscription(self, symbol: str) -> None:
        with self._engine_lock:
            token = next(
                (t for t, tr in self._option_token_to_trade.items() if tr.symbol == symbol),
                None,
            )
            if token is not None:
                del self._option_token_to_trade[token]
        if token is not None and self._ticker is not None:
            self._ticker.unsubscribe([token])

    # ------------------------------------------------------------------
    # WebSocket event handlers
    # ------------------------------------------------------------------
    def _setup_websocket(self) -> bool:
        """Resolve underlying token, create and wire up TickerManager."""
        creds = config.credentials.get("kite", {})
        api_key = creds.get("api_key", "")
        access_token = creds.get("access_token", "")
        if not api_key or not access_token:
            log.error("Cannot start WebSocket: missing kite.api_key / access_token in credentials.yaml")
            return False

        index_sym = "NIFTY BANK" if self.underlying == "BANKNIFTY" else "NIFTY 50"
        self._underlying_token = self._get_instrument_token(index_sym, "NSE")
        if not self._underlying_token:
            log.error(f"Could not resolve instrument token for {index_sym}")
            return False

        self._ticker = TickerManager(api_key, access_token)
        self._ticker.on_ticks(self._on_ticks)
        self._ticker.on_connect(self._on_ws_connect)
        self._ticker.on_close(self._on_ws_close)
        self._ticker.on_error(self._on_ws_error)

        # Pre-register the underlying token so it's subscribed on connect
        self._ticker.subscribe([self._underlying_token], mode="full")

        # Resolve India VIX token for the regime gate (ltp mode is enough — no volume/OI needed)
        self._vix_token = self._get_instrument_token("INDIA VIX", "NSE")
        if self._vix_token:
            self._ticker.subscribe([self._vix_token], mode="ltp")
            log.info(f"Subscribed India VIX (token={self._vix_token}) for regime gate")
        else:
            log.warning("India VIX token not found — regime gate will be bypassed (fail-open)")

        log.info(f"WebSocket configured. Underlying token: {self._underlying_token}")
        return True

    # ------------------------------------------------------------------
    # Tick validation (F8)
    # ------------------------------------------------------------------
    def _validate_tick(self, tick: dict) -> bool:
        """Return False for ticks with zero/negative price or stale exchange timestamp."""
        price = tick.get("last_price", 0.0)
        if price <= 0:
            log.debug(f"Tick rejected: price={price} token={tick.get('instrument_token')}")
            return False
        raw_ts = tick.get("exchange_timestamp")
        if isinstance(raw_ts, datetime):
            ts_naive = raw_ts.replace(tzinfo=None) if raw_ts.tzinfo else raw_ts
            age_s = (now_ist().replace(tzinfo=None) - ts_naive).total_seconds()
            if age_s > 3:
                log.debug(f"Tick rejected: stale {age_s:.1f}s token={tick.get('instrument_token')}")
                return False
        return True

    def _on_ws_connect(self) -> None:
        state.ws_connected = True
        sys_log.event(EVENT_CONNECTED, "KiteTicker WebSocket connected")
        log.info("WebSocket ready — streaming ticks for underlying index")

    def _on_ws_close(self, code: int, reason: str) -> None:
        state.ws_connected = False
        sys_log.event(EVENT_DISCONNECTED, f"code={code} reason={reason}")
        log.warning(f"WebSocket disconnected ({code}): {reason}")

    def _on_ws_error(self, exc: Exception) -> None:
        state.last_error = str(exc)
        log.error(f"WebSocket error: {exc}")

    def _on_ticks(self, ticks: list[dict]) -> None:
        """Routes each tick to the candle aggregator or the per-tick SL checker."""
        state.last_tick_time = now_ist()

        for tick in ticks:
            if not self._validate_tick(tick):   # F8: reject bad ticks early
                continue
            token: int = tick.get("instrument_token", 0)
            price: float = tick.get("last_price", 0.0)
            # volume_traded is the cumulative day total provided by KiteTicker full/quote modes
            volume_traded: int = tick.get("volume_traded", 0) or tick.get("volume", 0)
            oi: int = tick.get("oi", 0) or tick.get("open_interest", 0)
            # exchange_timestamp may be a datetime or None
            raw_ts = tick.get("exchange_timestamp")
            ts: Optional[datetime] = raw_ts if isinstance(raw_ts, datetime) else None

            if token == self._underlying_token:
                state.underlying_ltp = price
                self._candle_agg.process_tick(token, price, volume_traded, oi, ts)

            elif self._vix_token and token == self._vix_token:
                self.regime_filter.update_vix(price)
                state.vix = price
                state.market_regime = self.regime_filter.classify().value

            elif token in self._option_token_to_trade:
                self._check_sl_on_tick(token, price)

    def _check_sl_on_tick(self, token: int, price: float) -> None:
        """Per-tick stop-loss and trailing-SL evaluation for an option position."""
        with self._engine_lock:
            trade = self._option_token_to_trade.get(token)
        if trade is None or not trade.is_open():
            return

        # Update trailing SL
        new_trail = self.risk.compute_trailing_sl(trade, price)
        if new_trail != trade.trailing_sl:
            trade.trailing_sl = new_trail
            log.debug(f"{trade.trade_id} trailing SL -> {new_trail:.2f} (tick @ {price:.2f})")

        # Update unrealised P&L in state
        trade.pnl_points = price - trade.entry_price
        trade.pnl = trade.pnl_points * trade.quantity
        state.update_unrealised(sum(t.pnl for t in state.open_trades))

        # Check SL and target (SuperTrend flip is handled on candle close)
        sl = trade.trailing_sl if trade.trailing_sl else trade.stop_loss
        exit_reason: Optional[ExitReason] = None

        if price <= sl:
            exit_reason = (
                ExitReason.TRAILING_SL if trade.trailing_sl != trade.stop_loss else ExitReason.STOP_LOSS
            )
        elif price >= trade.target:
            exit_reason = ExitReason.TARGET_HIT

        if exit_reason:
            self.exit_trade(trade, exit_reason, exit_price=price)

    def _on_candle_close(self, token: int, candle: pd.Series) -> None:
        """Fires when a 3-min candle closes for the underlying.

        Extends the live df, recomputes indicators, checks SuperTrend flip
        for open positions, and evaluates new entry signals.
        """
        if token != self._underlying_token:
            return

        # F32 — Hot-reload settings.yaml so SL/target tweaks take effect without restart
        try:
            config.reload()
            self.risk.target_points    = float(config.get("stop_loss.target_points", 10))
            self.risk.max_risk_points  = float(config.get("stop_loss.max_risk_points", 20))
            self.risk.trail_step       = float(config.get("stop_loss.trailing.points_trail_step", 5))
            self.risk.trail_activation = float(config.get("stop_loss.trailing.activation_profit_pts", 5))
        except Exception as _e:
            log.debug(f"Config hot-reload skipped: {_e}")

        # --- Safety checks ---
        if self.risk.is_daily_loss_breached(state.realised_pnl):
            if not state.halted:
                state.halt("Daily loss limit hit")
                self.square_off_all(ExitReason.DAILY_LOSS_HIT)
            return

        if is_square_off_time():
            if state.open_trades:
                log.info("Square-off time reached. Closing all positions.")
                self.square_off_all(ExitReason.SQUARE_OFF_EOD)
            if not state.halted:
                state.halt("End of trading day")
            return

        if not is_market_open():
            return

        # --- Extend live df and recompute indicators ---
        with self._df_lock:
            if self._live_df is None or self._live_df.empty:
                log.warning("Live df not seeded yet — skipping candle close processing.")
                return

            new_row = candle.to_frame().T
            new_row.index.name = "date"
            self._live_df = pd.concat([self._live_df, new_row])
            # Deduplicate: live candle takes precedence if timestamp overlaps with history
            self._live_df = self._live_df[~self._live_df.index.duplicated(keep="last")]
            # Keep a rolling window of 100 candles (sufficient for all indicators)
            if len(self._live_df) > 100:
                self._live_df = self._live_df.iloc[-100:]
            # Ensure numeric dtypes after concat
            for col in ("open", "high", "low", "close", "volume", "open_interest"):
                if col in self._live_df.columns:
                    self._live_df[col] = pd.to_numeric(self._live_df[col], errors="coerce").fillna(0.0)

            intraday_cfg = config.get("indicators.intraday", {})
            st_cfg = intraday_cfg.get("supertrend", {})
            psar_cfg = intraday_cfg.get("psar", {})
            df = compute_all_indicators(
                self._live_df.copy(),
                st_period=st_cfg.get("period", 10),
                st_multiplier=st_cfg.get("multiplier", 2),
                rsi_period=intraday_cfg.get("rsi", {}).get("period", 14),
                psar_acc=psar_cfg.get("acceleration", 0.02),
                psar_max=psar_cfg.get("max_acceleration", 0.2),
            )
            self._live_df = df
            state.last_candle_time = df.index[-1].to_pydatetime()
            df_snapshot = df.copy()

        log.info(
            f"3-min candle closed @ {candle.name} | "
            f"C={candle['close']:.2f} V={candle['volume']:.0f} | "
            f"open_trades={len(state.open_trades)}"
        )

        # --- SuperTrend flip check for open trades (candle-resolution) ---
        if not df_snapshot.empty:
            st_dir = int(df_snapshot.iloc[-1].get("supertrend_dir", 0))
            for trade in list(state.open_trades):
                if not trade.is_open():
                    continue
                if self.risk.exit_on_supertrend_flip:
                    flip_exit: Optional[ExitReason] = None
                    if trade.trade_type == TradeType.LONG and st_dir == -1:
                        flip_exit = ExitReason.SUPERTREND_FLIP
                    elif trade.trade_type == TradeType.SHORT and st_dir == 1:
                        flip_exit = ExitReason.SUPERTREND_FLIP
                    if flip_exit:
                        self.exit_trade(trade, flip_exit)

        # --- Guard: halt / cooldown ---
        if state.halted or self.in_cooldown():
            return

        # --- Deduplicate: only evaluate each candle once ---
        candle_ts = candle.name.to_pydatetime() if hasattr(candle.name, "to_pydatetime") else candle.name
        if self._last_evaluated_candle == candle_ts:
            return
        self._last_evaluated_candle = candle_ts

        if len(df_snapshot) < self.warmup_candles + 2:
            return

        # --- Strategy evaluation ---
        signal = self.strategy.evaluate(df_snapshot, self.underlying)
        if signal is None or signal.strength == SignalStrength.WEAK:
            return

        # --- VIX regime gate ---
        tradeable, regime_reason = self.regime_filter.is_tradeable()
        if not tradeable:
            log.info(f"Entry blocked by VIX regime gate: {regime_reason}")
            return

        # Re-entry trend filter
        if self._cooldown_until and now_ist() >= self._cooldown_until:
            positional_df = self.prepare_positional_candles()
            if positional_df is not None:
                if not self.trend_filter.trend_agrees(positional_df, signal.trade_type):
                    log.info(
                        f"Re-entry blocked: 15-min trend disagrees with {signal.trade_type.value}"
                    )
                    return
            self._cooldown_until = None

        self.enter_trade(signal, df_snapshot)

        # Refresh ATM for dashboard
        state.atm_strike = self.broker.get_atm_strike(self.underlying)

    # ------------------------------------------------------------------
    # Square-off / gap checks
    # ------------------------------------------------------------------
    def square_off_all(self, reason: ExitReason) -> None:
        for trade in list(state.open_trades):
            self.exit_trade(trade, reason)

    def check_gap_open(self) -> None:
        for trade in list(state.open_trades):
            current = self.broker.get_ltp(trade.symbol)
            if current == 0:
                continue
            if self.risk.should_gap_exit(trade, current):
                log.warning(f"Gap protection triggered for {trade.trade_id}")
                self.exit_trade(trade, ExitReason.GAP_PROTECTION, exit_price=current)

    # ------------------------------------------------------------------
    # Cooldown
    # ------------------------------------------------------------------
    def in_cooldown(self) -> bool:
        if self._cooldown_until and now_ist() < self._cooldown_until:
            return True
        if self._last_trade_time:
            elapsed = (now_ist() - self._last_trade_time).total_seconds()
            if elapsed < self.min_time_between:
                return True
        return False

    # ------------------------------------------------------------------
    # Poll-mode fallback (used if WebSocket setup fails)
    # ------------------------------------------------------------------
    def monitor_open_trades(self, primary_df: pd.DataFrame) -> None:
        """REST-poll fallback: update trailing SL and check exits on each iteration."""
        if not state.open_trades:
            state.update_unrealised(0.0)
            return

        last_candle = primary_df.iloc[-1] if not primary_df.empty else None
        st_dir = int(last_candle["supertrend_dir"]) if last_candle is not None else None

        for trade in list(state.open_trades):
            current_premium = self.broker.get_ltp(trade.symbol)
            if current_premium == 0:
                continue

            new_trail = self.risk.compute_trailing_sl(trade, current_premium)
            if new_trail != trade.trailing_sl:
                trade.trailing_sl = new_trail

            trade.pnl_points = current_premium - trade.entry_price
            trade.pnl = trade.pnl_points * trade.quantity

            sl = trade.trailing_sl if trade.trailing_sl else trade.stop_loss
            exit_reason: Optional[ExitReason] = None

            if current_premium <= sl:
                exit_reason = (
                    ExitReason.TRAILING_SL if trade.trailing_sl != trade.stop_loss else ExitReason.STOP_LOSS
                )
            elif current_premium >= trade.target:
                exit_reason = ExitReason.TARGET_HIT
            elif self.risk.exit_on_supertrend_flip and st_dir is not None:
                if trade.trade_type == TradeType.LONG and st_dir == -1:
                    exit_reason = ExitReason.SUPERTREND_FLIP
                elif trade.trade_type == TradeType.SHORT and st_dir == 1:
                    exit_reason = ExitReason.SUPERTREND_FLIP

            if exit_reason:
                self.exit_trade(trade, exit_reason, exit_price=current_premium)

        state.update_unrealised(sum(t.pnl for t in state.open_trades))

    def iterate(self) -> None:
        """Single REST-poll iteration (poll fallback only)."""
        if self.risk.is_daily_loss_breached(state.realised_pnl):
            if not state.halted:
                state.halt("Daily loss limit hit")
                self.square_off_all(ExitReason.DAILY_LOSS_HIT)
            return

        if is_square_off_time():
            if state.open_trades:
                self.square_off_all(ExitReason.SQUARE_OFF_EOD)
            if not state.halted:
                state.halt("End of trading day")
            return

        if not is_market_open():
            return

        primary_df = self.prepare_candles(3)
        if primary_df is None or len(primary_df) < self.warmup_candles + 2:
            return

        self.monitor_open_trades(primary_df)

        if state.halted or self.in_cooldown():
            return

        last_candle_time = primary_df.index[-1].to_pydatetime()
        if self._last_evaluated_candle == last_candle_time:
            return
        self._last_evaluated_candle = last_candle_time

        signal = self.strategy.evaluate(primary_df, self.underlying)
        if signal is None or signal.strength == SignalStrength.WEAK:
            return

        if self._cooldown_until and now_ist() >= self._cooldown_until:
            positional_df = self.prepare_positional_candles()
            if positional_df is not None:
                if not self.trend_filter.trend_agrees(positional_df, signal.trade_type):
                    return
            self._cooldown_until = None

        self.enter_trade(signal, primary_df)

        spot = getattr(self.broker, "get_underlying_ltp", lambda _: 0)(self.underlying)
        if spot > 0:
            state.underlying_ltp = spot
            state.atm_strike = self.broker.get_atm_strike(self.underlying)

    def _run_poll_fallback(self, poll_interval_sec: int) -> None:
        log.warning("Running in REST-poll fallback mode (no WebSocket). SL checks every ~5s.")
        try:
            while True:
                try:
                    self.iterate()
                except Exception as e:
                    log.exception(f"Error in poll iteration: {e}")
                    state.last_error = str(e)
                time.sleep(poll_interval_sec)
        except KeyboardInterrupt:
            log.info("Poll fallback interrupted.")

    # ------------------------------------------------------------------
    # Main entry point
    # ------------------------------------------------------------------
    def run(self, poll_interval_sec: int = 5) -> None:
        """Seed historical data, start WebSocket streaming, block until shutdown."""
        log.info("=" * 60)
        log.info("Siva Scalping Bot starting up (WebSocket mode)")
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

        # ------------------------------------------------------------------
        # Pre-open wait: hold here until (market_open - ws_pre_open_minutes)
        # This lets you run daily_start.bat at any time (e.g. 8 AM from the
        # US) and the bot will self-schedule its WebSocket start.
        # ------------------------------------------------------------------
        pre_open_min = int(config.get("session.ws_pre_open_minutes", 15))
        ws_start_time = (
            datetime.combine(now_ist().date(), market_open_time(), tzinfo=IST)
            - timedelta(minutes=pre_open_min)
        )
        now = now_ist()
        if now < ws_start_time:
            wait_sec = (ws_start_time - now).total_seconds()
            log.info(
                f"Started early — WebSocket will begin at "
                f"{ws_start_time.strftime('%H:%M:%S')} IST "
                f"({wait_sec / 60:.0f} min from now). Sleeping..."
            )
            _last_log_min = -1
            while not self._shutdown.is_set():
                remaining = (ws_start_time - now_ist()).total_seconds()
                if remaining <= 0:
                    break
                # Log a countdown line once per minute so you can see it's alive
                remaining_min = int(remaining // 60)
                if remaining_min != _last_log_min:
                    log.info(
                        f"Pre-market wait: {remaining_min} min until WebSocket start "
                        f"({ws_start_time.strftime('%H:%M')} IST)."
                    )
                    _last_log_min = remaining_min
                time.sleep(5)

            if self._shutdown.is_set():
                log.info("Shutdown requested during pre-market wait. Exiting.")
                return
            log.info("Pre-market wait complete. Starting WebSocket now.")

        # Seed live_df with historical candles right before WebSocket starts
        # (done here — not at boot — so we always get the freshest data).
        log.info("Seeding historical candles for indicator warmup...")
        self._live_df = self.prepare_candles(3)
        if self._live_df is None or self._live_df.empty:
            log.warning("Historical candle seed failed. Proceeding with empty buffer.")
            self._live_df = pd.DataFrame()
        else:
            # Normalise to tz-naive so live candles (also tz-naive) append cleanly
            if hasattr(self._live_df.index, "tz") and self._live_df.index.tz is not None:
                self._live_df.index = self._live_df.index.tz_localize(None)
            log.info(f"Seeded {len(self._live_df)} historical candles.")

        self.check_gap_open()

        # Attempt WebSocket setup
        if not self._setup_websocket():
            log.error("WebSocket setup failed. Falling back to REST-poll mode.")
            self._run_poll_fallback(poll_interval_sec)
            return

        if not self._ticker.start():
            log.error("KiteTicker failed to start. Falling back to REST-poll mode.")
            self._run_poll_fallback(poll_interval_sec)
            return

        sys_log.event(EVENT_STARTUP, f"mode={state.mode} budget={state.daily_budget:.0f}")
        log.info("WebSocket started. Bot is live — waiting for ticks.")

        # Kill-switch watcher: Ctrl+K from dashboard → square off + halt (app stays running)
        def _kill_switch_watcher() -> None:
            while not self._shutdown.is_set():
                if state.kill_switch_active and not self._kill_switch_triggered:
                    self._kill_switch_triggered = True
                    log.warning("KILL SWITCH activated — squaring off all positions and suspending.")
                    sys_log.event(EVENT_KILL_SWITCH, "Manual kill switch triggered via Ctrl+K")
                    self.square_off_all(ExitReason.MANUAL)
                    state.halt("Kill switch activated — restart to resume trading")
                time.sleep(0.3)

        threading.Thread(target=_kill_switch_watcher, daemon=True, name="KillSwitchWatcher").start()

        # Auto-stop watcher: shuts the bot down cleanly N minutes after market close.
        # No human action needed — if you leave the laptop running the bot will
        # square off at 15:20, then disconnect the WebSocket at ~15:45 and exit.
        def _market_close_watcher() -> None:
            post_close_min = int(config.get("session.ws_post_close_minutes", 15))
            stop_at = (
                datetime.combine(now_ist().date(), market_close_time(), tzinfo=IST)
                + timedelta(minutes=post_close_min)
            )
            log.info(
                f"Market-close watcher active — WebSocket will auto-stop at "
                f"{stop_at.strftime('%H:%M:%S')} IST."
            )
            while not self._shutdown.is_set():
                if now_ist() >= stop_at:
                    log.info(
                        f"Post-close window elapsed ({post_close_min} min after market close). "
                        f"Signalling shutdown."
                    )
                    sys_log.event(
                        EVENT_SHUTDOWN,
                        f"Auto-stop: {post_close_min} min post-close window elapsed",
                    )
                    self._shutdown.set()
                    break
                time.sleep(30)   # 30-second granularity is more than enough here

        threading.Thread(target=_market_close_watcher, daemon=True, name="MarketCloseWatcher").start()

        # Watch state.shutdown_requested in a background thread so the dashboard
        # Q-key can trigger a graceful shutdown without needing a direct reference
        # to this engine instance.
        def _shutdown_watcher() -> None:
            while not self._shutdown.is_set():
                if state.shutdown_requested:
                    log.info("Shutdown requested (Q key). Triggering graceful exit.")
                    self._shutdown.set()
                    break
                time.sleep(0.5)

        threading.Thread(target=_shutdown_watcher, daemon=True, name="ShutdownWatcher").start()

        # Block the calling thread until shutdown is signalled
        try:
            self._shutdown.wait()
        except KeyboardInterrupt:
            self._shutdown.set()
        finally:
            log.info("Shutdown signal received. Squaring off all positions.")
            sys_log.event(EVENT_SHUTDOWN, f"realised_pnl={state.realised_pnl:.2f}")
            self.square_off_all(ExitReason.MANUAL)
            if self._ticker:
                self._ticker.stop()
            log.info("Shutdown complete.")
