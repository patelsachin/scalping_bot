"""Alpaca WebSocket ticker — same interface as KiteTicker's TickerManager.

Data flow:
  StockDataStream (SPY trades) → _on_stock_trade → tick dict → engine._on_ticks
  OptionDataStream (option trades) → _on_option_trade → tick dict → engine._on_ticks

Token scheme:
  Alpaca uses symbol strings; we map them to stable fake integer tokens via
  alpaca_broker._register/_token_to_symbol so the engine's token-keyed dicts
  (_option_token_to_trade, _candle_token, etc.) work unchanged.
"""
from __future__ import annotations

import asyncio
import threading
from datetime import datetime
from typing import Callable, Optional

from src.broker.alpaca_broker import _register, resolve_symbol
from src.utils.logger import get_logger

log = get_logger(__name__)


class AlpacaTicker:
    """WebSocket ticker for Alpaca Markets — mirrors TickerManager's interface."""

    def __init__(self, api_key: str, secret_key: str, paper: bool = True) -> None:
        self._api_key    = api_key
        self._secret_key = secret_key
        self._paper      = paper

        self._tick_handlers:   list[Callable[[list[dict]], None]] = []
        self._connect_handler: Optional[Callable[[], None]]       = None
        self._close_handler:   Optional[Callable[[int, str], None]] = None
        self._error_handler:   Optional[Callable[[Exception], None]] = None

        # token → "stock" | "option"
        self._subscriptions: dict[int, str] = {}
        self._stock_syms:  set[str] = set()
        self._option_syms: set[str] = set()

        self._stock_stream  = None
        self._option_stream = None
        self._threads: list[threading.Thread] = []
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Handler registration (same API as TickerManager)
    # ------------------------------------------------------------------
    def on_ticks(self, handler: Callable[[list[dict]], None]) -> None:
        self._tick_handlers.append(handler)

    def on_connect(self, handler: Callable[[], None]) -> None:
        self._connect_handler = handler

    def on_close(self, handler: Callable[[int, str], None]) -> None:
        self._close_handler = handler

    def on_error(self, handler: Callable[[Exception], None]) -> None:
        self._error_handler = handler

    # ------------------------------------------------------------------
    # Subscription management
    # ------------------------------------------------------------------
    def subscribe(self, tokens: list[int], mode: str = "full") -> None:
        """Subscribe tokens. mode is accepted for interface parity but ignored."""
        with self._lock:
            for token in tokens:
                symbol = resolve_symbol(token)
                if symbol is None:
                    log.warning(f"AlpacaTicker.subscribe: unknown token {token} — register via broker.get_instrument_token first")
                    continue
                self._subscriptions[token] = symbol
                if self._is_option_symbol(symbol):
                    self._option_syms.add(symbol)
                    if self._option_stream is not None:
                        # Stream already running — subscribe dynamically
                        self._option_stream.subscribe_trades(self._on_option_trade, symbol)
                        log.info(f"AlpacaTicker: subscribed option {symbol} (token={token})")
                else:
                    self._stock_syms.add(symbol)
                    if self._stock_stream is not None:
                        self._stock_stream.subscribe_trades(self._on_stock_trade, symbol)
                        log.info(f"AlpacaTicker: subscribed stock {symbol} (token={token})")

    def unsubscribe(self, tokens: list[int]) -> None:
        with self._lock:
            for token in tokens:
                symbol = self._subscriptions.pop(token, None)
                if symbol:
                    self._stock_syms.discard(symbol)
                    self._option_syms.discard(symbol)
                    # Alpaca streams don't have an explicit unsubscribe call for individual
                    # symbols in alpaca-py; we stop routing ticks for removed symbols by
                    # checking self._subscriptions in the handler.
                    log.debug(f"AlpacaTicker: unsubscribed {symbol} (token={token})")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> bool:
        try:
            from alpaca.data.live import StockDataStream, OptionDataStream
        except ImportError:
            log.error("alpaca-py not installed. Run: pip install alpaca-py")
            return False

        # Feed must be a Feed enum — passing a plain string causes AttributeError on .value
        try:
            from alpaca.data.enums import Feed
            feed = Feed.IEX   # free tier; use Feed.SIP for consolidated (paid)
            self._stock_stream = StockDataStream(self._api_key, self._secret_key, feed=feed)
        except (ImportError, AttributeError):
            # Older alpaca-py builds — omit feed, defaults to IEX
            self._stock_stream = StockDataStream(self._api_key, self._secret_key)
        self._option_stream = OptionDataStream(self._api_key, self._secret_key)

        # Subscribe initial stock symbols
        with self._lock:
            stock_syms  = set(self._stock_syms)
            option_syms = set(self._option_syms)

        if stock_syms:
            for sym in stock_syms:
                self._stock_stream.subscribe_trades(self._on_stock_trade, sym)
        else:
            # Subscribe a placeholder so the stream connects; it will receive
            # real subscriptions when stocks are added later.
            self._stock_stream.subscribe_trades(self._on_stock_trade, "SPY")

        for sym in option_syms:
            self._option_stream.subscribe_trades(self._on_option_trade, sym)

        # Run each stream in a daemon thread (stream.run() is blocking/asyncio)
        st = threading.Thread(
            target=self._run_stream,
            args=(self._stock_stream, "StockStream"),
            daemon=True,
            name="AlpacaStockStream",
        )
        ot = threading.Thread(
            target=self._run_stream,
            args=(self._option_stream, "OptionStream"),
            daemon=True,
            name="AlpacaOptionStream",
        )
        st.start()
        ot.start()
        self._threads = [st, ot]

        log.info(f"AlpacaTicker started ({'PAPER' if self._paper else 'LIVE'})")
        if self._connect_handler:
            self._connect_handler()
        return True

    def stop(self) -> None:
        if self._stock_stream:
            try:
                self._stock_stream.stop()
            except Exception:
                pass
        if self._option_stream:
            try:
                self._option_stream.stop()
            except Exception:
                pass
        log.info("AlpacaTicker stopped")

    # ------------------------------------------------------------------
    # Internal stream runners
    # ------------------------------------------------------------------
    @staticmethod
    def _run_stream(stream, name: str) -> None:
        try:
            stream.run()
        except Exception as e:
            log.error(f"AlpacaTicker stream {name} error: {e}")

    # ------------------------------------------------------------------
    # Alpaca event handlers → normalise to Kite-style tick dicts
    # ------------------------------------------------------------------
    async def _on_stock_trade(self, trade) -> None:
        symbol = trade.symbol
        token  = _register(symbol)
        with self._lock:
            if token not in self._subscriptions and symbol not in self._stock_syms:
                return
        tick = {
            "instrument_token": token,
            "last_price":       float(trade.price),
            "volume_traded":    int(trade.size),
            "oi":               0,
            "exchange_timestamp": trade.timestamp if isinstance(trade.timestamp, datetime)
                                  else datetime.now(),
        }
        self._dispatch([tick])

    async def _on_option_trade(self, trade) -> None:
        symbol = trade.symbol
        token  = _register(symbol)
        with self._lock:
            if token not in self._subscriptions:
                return
        tick = {
            "instrument_token": token,
            "last_price":       float(trade.price),
            "volume_traded":    int(trade.size),
            "oi":               0,
            "exchange_timestamp": trade.timestamp if isinstance(trade.timestamp, datetime)
                                  else datetime.now(),
        }
        self._dispatch([tick])

    def _dispatch(self, ticks: list[dict]) -> None:
        for handler in self._tick_handlers:
            try:
                handler(ticks)
            except Exception as e:
                log.exception(f"Tick handler raised: {e}")

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _is_option_symbol(symbol: str) -> bool:
        import re
        return bool(re.match(r"^[A-Z]+\d{6}[CP]\d+$", symbol))
