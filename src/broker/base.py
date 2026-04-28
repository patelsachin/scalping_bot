"""Abstract broker interface. Implementations: KiteBroker, AlpacaBroker, PaperBroker."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import pandas as pd

from src.core.models import Trade, TradeType


class BrokerBase(ABC):
    """Common interface for all broker implementations."""

    @abstractmethod
    def connect(self) -> bool:
        """Authenticate / validate connection."""

    @abstractmethod
    def get_ltp(self, symbol: str) -> float:
        """Last traded price for a symbol (stock or option)."""

    def get_underlying_ltp(self, underlying: str) -> float:
        """Spot/ETF LTP for the underlying. Default: delegates to get_ltp."""
        return self.get_ltp(underlying)

    @abstractmethod
    def get_historical_candles(
        self,
        symbol: str,
        interval: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        """Fetch historical OHLCV candles. Returns df with [open,high,low,close,volume]."""

    @abstractmethod
    def get_option_chain(self, underlying: str, expiry: str) -> pd.DataFrame:
        """Return option chain for the given underlying and expiry."""

    @abstractmethod
    def get_atm_strike(self, underlying: str) -> float:
        """Return the ATM strike based on current spot price."""

    @abstractmethod
    def get_option_symbol(
        self, underlying: str, expiry: str, strike: float, option_type: str
    ) -> str:
        """Build a tradeable option symbol string.

        India: BANKNIFTY26APR56000PE (Kite tradingsymbol format)
        US:    SPY260429P00500000    (OCC format)
        """

    def get_current_week_expiry(self, underlying: str) -> Optional[str]:
        """Nearest upcoming expiry date string (YYYY-MM-DD). Override per market."""
        return None

    def get_current_month_futures_symbol(self, underlying: str) -> Optional[str]:
        """Nearest futures tradingsymbol for volume data. India-only; returns None for US."""
        return None

    def get_instrument_token(self, symbol: str, exchange: str = "") -> Optional[int]:
        """Numeric (or hash-based) token used to identify a symbol in the WebSocket stream.

        India (Kite): real integer instrument tokens from NFO/NSE instrument list.
        US (Alpaca):  stable hash of the symbol string (abs(hash(sym)) % 2**31).
        Returns None if the symbol cannot be resolved.
        """
        return None

    def get_seed_symbol(self, underlying: str) -> str:
        """Symbol string to use when fetching historical candles for seeding.

        India: 'NIFTY BANK' (Kite index name) — overridden by futures symbol in engine.
        US:    'SPY' directly.
        Default: returns underlying unchanged.
        """
        return underlying

    @abstractmethod
    def place_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: str,
        order_type: str = "MARKET",
        price: Optional[float] = None,
    ) -> str:
        """Place an order. Returns order_id string."""

    @abstractmethod
    def exit_order(self, trade: Trade) -> str:
        """Place exit (SELL) order for an open trade. Returns order_id."""

    @abstractmethod
    def get_order_status(self, order_id: str) -> dict:
        """Return order status dict."""

    @abstractmethod
    def is_paper(self) -> bool:
        """True if this is a paper/simulated broker."""
