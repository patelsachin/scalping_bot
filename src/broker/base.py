"""Abstract broker interface. Implementations: KiteBroker, PaperBroker."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional

import pandas as pd

from src.core.models import Trade, TradeType


class BrokerBase(ABC):
    """Common interface for broker implementations (paper or live)."""

    @abstractmethod
    def connect(self) -> bool:
        """Authenticate / validate connection."""

    @abstractmethod
    def get_ltp(self, symbol: str) -> float:
        """Last traded price for a tradingsymbol."""

    @abstractmethod
    def get_historical_candles(
        self,
        symbol: str,
        interval: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        """Fetch historical candles. Returns df with columns [open, high, low, close, volume]."""

    @abstractmethod
    def get_option_chain(self, underlying: str, expiry: str) -> pd.DataFrame:
        """Return option chain for the given underlying and expiry."""

    @abstractmethod
    def get_atm_strike(self, underlying: str) -> float:
        """Return the ATM strike based on current spot/future price."""

    @abstractmethod
    def get_option_symbol(
        self, underlying: str, expiry: str, strike: float, option_type: str
    ) -> str:
        """Build a tradingsymbol, e.g. BANKNIFTY25JAN52000CE."""

    @abstractmethod
    def place_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: str,
        order_type: str = "MARKET",
        price: Optional[float] = None,
    ) -> str:
        """Place an order. Returns order_id."""

    @abstractmethod
    def exit_order(self, trade: Trade) -> str:
        """Place exit order for an open trade. Returns order_id."""

    @abstractmethod
    def get_order_status(self, order_id: str) -> dict:
        """Return order status dict."""

    @abstractmethod
    def is_paper(self) -> bool:
        """True if this is a paper broker."""
