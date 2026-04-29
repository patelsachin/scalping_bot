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
from src.broker.broker_factory import create_broker, create_ticker
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
from src.strategy.factory import StrategyFactory
from src.strategy.two_candle import PositionalTrendFilter
from src.utils.config_loader import config
from src.utils.logger import get_logger
from src.utils.market_calendar import (
    is_market_open,
    is_square_off_time,
    is_trading_day,
    last_trading_day,
    market_close_time,
    market_open_time,
    now_ist,
)
from src.utils.trade_logger import TradeLogger

log = get_logger(__name__)


class TradingEngine:
    """Main bot. Seeded from Kite historical REST, then driven by KiteTicker WebSocket."""

    def __init__(self) -> None:
        # Strategy is loaded FIRST so its timeframe drives the candle aggregator.
        # Change strategy.type in settings.yaml to switch strategies.
        self.strategy = StrategyFactory.create()
        self.trend_filter = PositionalTrendFilter()
        self.risk = RiskManager()
        self.trade_logger = TradeLogger()
        self.broker: BrokerBase = self._init_broker()

        self.underlying = config.get("instrument.symbol", "BANKNIFTY")
        self.exchange = config.get("instrument.exchange", "NFO")
        # Primary interval is owned by the strategy — not a static config value.
        self.primary_interval = self.strategy.timeframe_str
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
        self._underlying_token: Optional[int] = None  # NSE index — for LTP display / ATM
        self._futures_symbol: Optional[str] = None    # NFO futures — for candle building (has volume)
        self._candle_token: Optional[int] = None      # token used for candle building; set to futures if available
        self._ticker = None   # TickerManager (India) or AlpacaTicker (US)
        # Candle aggregator interval comes from the strategy — switches automatically.
        self._candle_agg = CandleAggregator(interval_minutes=self.strategy.timeframe_minutes)
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

        # Consecutive loss circuit breaker
        # Counts back-to-back losing trades; resets on any winning trade.
        # When it hits max_consecutive_losses the engine blocks entries for N candles.
        self._consecutive_losses: int = 0
        self._consec_loss_pause_until: Optional[datetime] = None

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
        return create_broker()

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
        to_dt   = now_ist()
        from_dt = to_dt - timedelta(minutes=lookback_minutes)
        symbol  = self.broker.get_seed_symbol(self.underlying)
        return self.broker.get_historical_candles(symbol, interval, from_dt, to_dt)

    def prepare_candles(
        self,
        interval_minutes: Optional[int] = None,
        lookback_minutes: Optional[int] = None,
    ) -> Optional[pd.DataFrame]:
        """Fetch historical candles and compute strategy indicators.
        Used by the REST-poll fallback path. In WebSocket mode, candles are
        built by CandleAggregator and indicators computed in _on_candle_close.
        """
        # Use strategy's timeframe by default
        tf_min = interval_minutes or self.strategy.timeframe_minutes
        interval_map = {1: "1minute", 3: "3minute", 5: "5minute", 15: "15minute", 60: "60minute"}
        interval = interval_map.get(tf_min, self.primary_interval)
        lookback = lookback_minutes if lookback_minutes is not None else max(120, tf_min * 40)
        df = self.fetch_candles(interval, lookback)

        if df.empty or len(df) < 15:
            log.debug(f"Insufficient candle data for {interval} (got {len(df)}).")
            return None

        df = self.strategy.compute_indicators(df)
        state.last_candle_time = df.index[-1].to_pydatetime() if len(df) else None
        return df

    def prepare_positional_candles(self) -> Optional[pd.DataFrame]:
        """Fetch 15-min candles for the positional trend filter.
        Uses indicators.positional config — shared across all strategies.
        """
        df = self.fetch_candles(self.positional_interval, 600)
        if df.empty or len(df) < 10:
            return None
        pos_cfg = config.get("indicators.positional", {})
        st_cfg  = pos_cfg.get("supertrend", {})
        return compute_all_indicators(
            df,
            st_period     = st_cfg.get("period", 7),
            st_multiplier = st_cfg.get("multiplier", 3),
            rsi_period    = pos_cfg.get("rsi", {}).get("period", 14),
        )

    # ------------------------------------------------------------------
    # Instrument token resolution (shared between KiteBroker & PaperBroker)
    # ------------------------------------------------------------------
    def _get_instrument_token(self, symbol: str, exchange: str = "") -> Optional[int]:
        return self.broker.get_instrument_token(symbol, exchange)

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

        option_type = signal.option_type   # "CE" (bullish) or "PE" (bearish)
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
            trade_type=TradeType.LONG,             # always LONG — system only buys options
            option_type=signal.option_type,        # "CE" (bullish) or "PE" (bearish)
            signal_strength=signal.strength,
            strategy=self.strategy.name,           # records which strategy fired this trade
            market=config.active_market(),         # "india" | "us"
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
        opt_token = self._get_instrument_token(symbol, self.exchange)
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

        # Consecutive loss circuit breaker tracking
        if trade.pnl < 0:
            self._consecutive_losses += 1
            max_consec = int(config.get("trade_rules.max_consecutive_losses", 2))
            pause_candles = int(config.get("trade_rules.consecutive_loss_pause_candles", 6))
            if self._consecutive_losses >= max_consec:
                pause_min = pause_candles * self.strategy.timeframe_minutes
                self._consec_loss_pause_until = now_ist() + timedelta(minutes=pause_min)
                log.warning(
                    f"Circuit breaker: {self._consecutive_losses} consecutive losses — "
                    f"entries paused for {pause_min} min "
                    f"(until {self._consec_loss_pause_until.strftime('%H:%M')} IST)."
                )
        else:
            # Profitable trade — reset streak
            if self._consecutive_losses > 0:
                log.info(f"Consecutive loss streak reset (was {self._consecutive_losses}) on winning trade.")
            self._consecutive_losses = 0
            self._consec_loss_pause_until = None

        state.close_trade(trade)
        self.trade_logger.log_trade(trade)

        log.info(
            f"EXITED {trade.trade_id} @ ₹{current_premium:.2f} reason={reason.value} "
            f"P&L=₹{trade.pnl:.2f} ({trade.pnl_points:+.2f} pts)"
        )

        # Unsubscribe and remove from option token map
        self._cleanup_option_subscription(trade.symbol)

        self._cooldown_until = now_ist() + timedelta(
            minutes=self.cooldown_candles * self.strategy.timeframe_minutes
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
    # Futures resolution + smart historical seed
    # ------------------------------------------------------------------
    def _resolve_futures_symbol(self) -> None:
        """Resolve the nearest-expiry futures symbol and instrument token.

        India: BankNifty futures carry real volume — use them for candle building.
        US:    SPY ETF ticks include native volume — no futures needed.
        """
        if config.active_market() == "us":
            log.info("US market — no futures subscription needed (SPY ETF has native volume).")
            return

        self._futures_symbol = self.broker.get_current_month_futures_symbol(self.underlying)
        if not self._futures_symbol:
            log.warning("No active futures contract found — index will be used for candle building (no volume).")
            return

        self._candle_token = self._get_instrument_token(self._futures_symbol, "NFO")  # India: NFO exchange
        if self._candle_token:
            log.info(f"Candle token set to futures: {self._futures_symbol} (token={self._candle_token})")
        else:
            log.warning(f"Could not resolve token for {self._futures_symbol} — index used for candles.")
            self._futures_symbol = None

    def _smart_seed_candles(self) -> Optional[pd.DataFrame]:
        """Smart historical seed for indicator warmup.

        Fetches the tail of the most recent completed session, plus today's
        session if market is currently open. Works for both India and US markets
        using config-driven session times.
        """
        fetch_symbol = self._futures_symbol   # India: futures; US: None → ETF directly

        prev_day    = last_trading_day()
        seed_min    = self.strategy.seed_lookback_minutes
        session_end = market_close_time()   # 15:30 India / 16:00 US

        prev_from = datetime.combine(prev_day, session_end) - timedelta(minutes=seed_min)
        prev_to   = datetime.combine(prev_day, session_end)

        frames: list[pd.DataFrame] = []

        df_prev = self._fetch_seed_candles(fetch_symbol, prev_from, prev_to)
        if df_prev is not None and not df_prev.empty:
            frames.append(df_prev)
            start_str = (
                datetime.combine(prev_day, session_end) - timedelta(minutes=seed_min)
            ).strftime("%H:%M")
            log.info(
                f"Smart seed: {len(df_prev)} candles from {prev_day} "
                f"({start_str}–{session_end.strftime('%H:%M')}, {self.strategy.timeframe_str})"
            )
        else:
            log.warning(
                f"Smart seed: no candles for {prev_day} tail — broker API may be unavailable."
            )

        # If market is open right now, also include today's session from market open to now
        if is_market_open():
            today        = now_ist().date()
            session_open = market_open_time()
            today_from   = datetime.combine(today, session_open)
            today_to     = now_ist()
            df_today     = self._fetch_seed_candles(fetch_symbol, today_from, today_to)
            if df_today is not None and not df_today.empty:
                frames.append(df_today)
                log.info(
                    f"Smart seed: {len(df_today)} candles from today's session "
                    f"({session_open.strftime('%H:%M')} – now)"
                )

        if not frames:
            return None

        # Merge, deduplicate, sort chronologically, strip tz
        df = pd.concat(frames)
        if hasattr(df.index, "tz") and df.index.tz is not None:
            df.index = df.index.tz_localize(None)
        df = df[~df.index.duplicated(keep="last")].sort_index()

        if len(df) < 10:
            log.warning(
                f"Smart seed: only {len(df)} candles after merge — insufficient for reliable indicators."
            )
            return None

        # Delegate indicator computation to the active strategy
        df = self.strategy.compute_indicators(df)
        state.last_candle_time = df.index[-1].to_pydatetime() if len(df) else None
        log.info(
            f"Smart seed complete: {len(df)} {self.strategy.timeframe_str} candles | "
            f"strategy={self.strategy.name} | indicator warmup ready."
        )
        return df

    def _fetch_seed_candles(
        self,
        symbol: Optional[str],
        from_dt: datetime,
        to_dt: datetime,
    ) -> Optional[pd.DataFrame]:
        """Fetch historical candles for seeding.
        Uses the active strategy's timeframe string (e.g. '3minute', '1minute').
        Uses futures symbol when provided (NFO), falls back to index (NSE).
        """
        interval = self.strategy.timeframe_str
        if symbol:
            df = self.broker.get_historical_candles(symbol, interval, from_dt, to_dt)
        else:
            seed_sym = self.broker.get_seed_symbol(self.underlying)
            df = self.broker.get_historical_candles(seed_sym, interval, from_dt, to_dt)
        return df if df is not None and not df.empty else None

    # ------------------------------------------------------------------
    # WebSocket event handlers
    # ------------------------------------------------------------------
    def _setup_websocket(self) -> bool:
        """Resolve underlying token, create and wire up the market-appropriate ticker."""
        market = config.active_market()

        if market == "india":
            # India: underlying token is the NSE index (display/ATM); candles come from futures
            index_sym = self.broker.get_seed_symbol(self.underlying)
            self._underlying_token = self._get_instrument_token(index_sym, "NSE")
            if not self._underlying_token:
                log.error(f"Could not resolve instrument token for {index_sym}")
                return False
        else:
            # US: underlying is the ETF itself (SPY) — has native volume and price
            self._underlying_token = self._get_instrument_token(self.underlying, self.exchange)
            if not self._underlying_token:
                log.error(f"Could not resolve token for {self.underlying}")
                return False

        self._ticker = create_ticker(self.broker)
        self._ticker.on_ticks(self._on_ticks)
        self._ticker.on_connect(self._on_ws_connect)
        self._ticker.on_close(self._on_ws_close)
        self._ticker.on_error(self._on_ws_error)

        self._ticker.subscribe([self._underlying_token], mode="full")

        if market == "india":
            # Subscribe futures token for candle building with real volume
            if self._candle_token and self._candle_token != self._underlying_token:
                self._ticker.subscribe([self._candle_token], mode="full")
                log.info(
                    f"Subscribed {self._futures_symbol} (token={self._candle_token}) "
                    f"for candle building with real volume"
                )
            else:
                self._candle_token = self._underlying_token
                log.warning("No futures token — index will be used for candle building (no volume).")

            # India VIX for market regime gate
            self._vix_token = self._get_instrument_token("INDIA VIX", "NSE")
            if self._vix_token:
                self._ticker.subscribe([self._vix_token], mode="ltp")
                log.info(f"Subscribed India VIX (token={self._vix_token}) for regime gate")
            else:
                log.warning("India VIX token not found — regime gate bypassed (fail-open)")
        else:
            # US: ETF IS the candle source; no VIX subscription needed
            self._candle_token = self._underlying_token

        log.info(
            f"WebSocket configured [{market.upper()}]. "
            f"Underlying token: {self._underlying_token} | "
            f"Candle token: {self._candle_token}"
        )
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
            age_s = (now_ist() - ts_naive).total_seconds()
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
                # Index tick: update spot LTP for ATM / dashboard display only.
                # Candle building is done from futures ticks (below) which carry volume.
                state.underlying_ltp = price
                if self._candle_token == self._underlying_token:
                    # No futures available — fall back to building candles from index
                    self._candle_agg.process_tick(token, price, volume_traded, oi, ts)

            elif self._candle_token and token == self._candle_token:
                # Futures tick: primary candle-building source with real volume
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
        # Accept candles from the futures token (primary) or index fallback
        candle_tok = self._candle_token or self._underlying_token
        if token != candle_tok:
            return

        # F32 — Hot-reload settings.yaml so SL/target tweaks take effect without restart
        try:
            config.reload()
            self.risk.target_points         = float(config.get("stop_loss.target_points", 10))
            self.risk.max_risk_points       = float(config.get("stop_loss.max_risk_points", 20))
            self.risk.trail_step            = float(config.get("stop_loss.trailing.points_trail_step", 5))
            self.risk.trail_activation      = float(config.get("stop_loss.trailing.activation_profit_pts", 10))
            self.risk.trail_activation_strong = float(
                config.get("stop_loss.trailing.strong_activation_profit_pts",
                           config.get("stop_loss.trailing.activation_profit_pts", 10))
            )
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
                # Historical seed failed — bootstrap from this first live candle.
                # Strategy evaluation is gated below by warmup_candles check,
                # so no trades fire until enough candles have accumulated.
                new_row = candle.to_frame().T
                new_row.index.name = "date"
                self._live_df = new_row
                log.info(
                    f"Live df bootstrapped from live candle {candle.name}. "
                    f"Accumulating candles for indicator warmup..."
                )
                return  # Need more candles before indicators can compute

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

            # Delegate indicator computation to the active strategy.
            # Switching strategies (scalping ↔ ichimoku) changes this automatically.
            df = self.strategy.compute_indicators(self._live_df.copy())
            self._live_df = df
            state.last_candle_time = df.index[-1].to_pydatetime()
            df_snapshot = df.copy()

        log.info(
            f"{self.strategy.timeframe_minutes}-min candle closed @ {candle.name} | "
            f"C={candle['close']:.2f} V={candle['volume']:.0f} | "
            f"open_trades={len(state.open_trades)}"
        )

        # --- Strategy-driven candle-close exit check ---
        # Scalping: SuperTrend flip.  Ichimoku: TK cross or cloud re-entry.
        # Each strategy implements exit_signal() — engine stays strategy-agnostic.
        if not df_snapshot.empty:
            for trade in list(state.open_trades):
                if not trade.is_open():
                    continue
                flip_exit = self.strategy.exit_signal(trade, df_snapshot)
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

        # --- Early session filter: skip first N minutes after market open ---
        # Price discovery in 09:15-09:30 is too noisy for reliable signals.
        # Configured via trade_rules.no_entry_before (HH:MM IST).
        try:
            no_entry_before_str = str(config.get("session.no_entry_before", "09:30"))
            _ef_hh, _ef_mm = no_entry_before_str.split(":")
            candle_ts_raw = candle.name
            if hasattr(candle_ts_raw, "to_pydatetime"):
                candle_ts_raw = candle_ts_raw.to_pydatetime()
            candle_hhmm = candle_ts_raw.hour * 60 + candle_ts_raw.minute
            filter_hhmm = int(_ef_hh) * 60 + int(_ef_mm)
            if candle_hhmm < filter_hhmm:
                log.debug(
                    f"Early session filter: skipping entry at "
                    f"{candle_ts_raw.strftime('%H:%M')} (no entries before {no_entry_before_str} IST)"
                )
                return
        except Exception:
            pass  # malformed config — allow entry rather than blocking

        # --- No-trade window filter: block entries during configured time ranges ---
        # Strategy-specific windows take priority (e.g. scalping blocks lunch,
        # Ichimoku does not). Falls back to trade_rules.no_trade_windows if the
        # active strategy has no windows declared.
        try:
            _strategy_windows = config.get(f"{self.strategy.name}.no_trade_windows", None)
            no_trade_windows = (
                _strategy_windows if _strategy_windows is not None
                else config.get("trade_rules.no_trade_windows", [])
            )
            for _window in no_trade_windows:
                _ws_hh, _ws_mm = str(_window.get("start", "00:00")).split(":")
                _we_hh, _we_mm = str(_window.get("end",   "00:00")).split(":")
                _win_start = int(_ws_hh) * 60 + int(_ws_mm)
                _win_end   = int(_we_hh) * 60 + int(_we_mm)
                if _win_start <= candle_hhmm < _win_end:
                    log.debug(
                        f"No-trade window: skipping entry at "
                        f"{candle_ts_raw.strftime('%H:%M')} "
                        f"(blocked {_window.get('start')}–{_window.get('end')} IST)"
                    )
                    return
        except Exception:
            pass  # malformed config — allow entry rather than blocking

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
                if not self.trend_filter.trend_agrees(positional_df, signal.option_type):
                    log.info(
                        f"Re-entry blocked: 15-min trend disagrees with {signal.option_type}"
                    )
                    return
            self._cooldown_until = None

        # --- Max open positions gate ---
        max_pos = int(config.get("trade_rules.max_open_positions", 0))
        if max_pos > 0 and len(state.open_trades) >= max_pos:
            log.debug(
                f"Max open positions ({max_pos}) reached — skipping entry "
                f"(open={len(state.open_trades)})"
            )
            return

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
        if self._consec_loss_pause_until and now_ist() < self._consec_loss_pause_until:
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
            elif not primary_df.empty:
                # Delegate candle-close exit to strategy (SuperTrend flip / TK cross etc.)
                exit_reason = self.strategy.exit_signal(trade, primary_df)

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
        market   = config.active_market().upper()
        currency = "$" if config.active_market() == "us" else "₹"
        log.info("=" * 60)
        log.info(f"Siva Scalping Bot starting up [{market}] (WebSocket mode)")
        log.info(f"Mode: {state.mode.upper()} | Strategy: {self.strategy.name}")
        log.info(f"Daily budget: {currency}{state.daily_budget:,.2f}")
        log.info(f"Underlying: {self.underlying} | Exchange: {self.exchange}")
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
            datetime.combine(datetime.now().date(), market_open_time())
            - timedelta(minutes=pre_open_min)
        )
        now = now_ist()
        if now < ws_start_time:
            wait_sec = (ws_start_time - now).total_seconds()
            log.info(
                f"Started early — WebSocket will begin at "
                f"{ws_start_time.strftime('%H:%M:%S')} local "
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
                        f"({ws_start_time.strftime('%H:%M')} local)."
                    )
                    _last_log_min = remaining_min
                time.sleep(5)

            if self._shutdown.is_set():
                log.info("Shutdown requested during pre-market wait. Exiting.")
                return
            log.info("Pre-market wait complete. Starting WebSocket now.")

        # Resolve futures symbol (needed for both seed and WebSocket).
        # Done here — after the pre-open wait — so we always get the current contract.
        self._resolve_futures_symbol()

        # Smart seed: previous session tail (14:45–15:30) + today if market is open.
        # Uses futures data so seed candles carry real volume.
        log.info("Seeding historical candles (smart seed)...")
        self._live_df = self._smart_seed_candles()
        if self._live_df is None or self._live_df.empty:
            log.warning(
                "Smart seed failed (Kite API returned no data). "
                "Bot will self-seed from live ticks once market opens."
            )
            self._live_df = pd.DataFrame()
        else:
            # Normalise to tz-naive so live candles (also tz-naive) append cleanly
            if hasattr(self._live_df.index, "tz") and self._live_df.index.tz is not None:
                self._live_df.index = self._live_df.index.tz_localize(None)

        self.check_gap_open()

        # Attempt WebSocket setup
        if not self._setup_websocket():
            log.error("WebSocket setup failed. Falling back to REST-poll mode.")
            self._run_poll_fallback(poll_interval_sec)
            return

        if not self._ticker.start():
            log.error("Ticker WebSocket failed to start. Falling back to REST-poll mode.")
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
                datetime.combine(datetime.now().date(), market_close_time())
                + timedelta(minutes=post_close_min)
            )
            log.info(
                f"Market-close watcher active — WebSocket will auto-stop at "
                f"{stop_at.strftime('%H:%M:%S')} local."
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
