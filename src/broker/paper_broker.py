"""Paper broker: wraps KiteBroker for live price feeds, but simulates order execution."""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import uuid4

import pandas as pd

from src.broker.base import BrokerBase
from src.broker.kite_broker import KiteBroker
from src.core.models import Trade
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


class PaperBroker(BrokerBase):
    """Paper trading: fetches live prices via Kite but simulates orders locally."""

    def __init__(self) -> None:
        self._kite = KiteBroker()
        self._simulated_orders: dict[str, dict] = {}
        self._connected = False
        # Slippage in basis points (1 bps = 0.01%). Applied on each simulated fill.
        # BUY fills slightly higher, SELL fills slightly lower — mimics real market impact.
        self._slippage_bps: float = float(
            config.get("paper_trading.slippage_bps", 5)
        )

    def connect(self) -> bool:
        # We still need Kite connection to fetch live prices
        ok = self._kite.connect()
        if ok:
            log.info("Paper broker connected (using Kite for live data).")
            self._connected = True
        else:
            log.warning(
                "Paper broker: Kite connection failed. Will require historical CSV fallback."
            )
            self._connected = False
        return True  # Paper mode always "connects" even if live feed is unavailable

    def is_paper(self) -> bool:
        return True

    def get_ltp(self, symbol: str) -> float:
        if self._connected:
            return self._kite.get_ltp(symbol)
        return 0.0

    def get_underlying_ltp(self, underlying: str = "BANKNIFTY") -> float:
        if self._connected:
            return self._kite.get_underlying_ltp(underlying)
        return 0.0

    def get_historical_candles(
        self,
        symbol: str,
        interval: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        if self._connected:
            return self._kite.get_historical_candles(symbol, interval, from_dt, to_dt)
        return pd.DataFrame()

    def get_atm_strike(self, underlying: str = "BANKNIFTY") -> float:
        if self._connected:
            return self._kite.get_atm_strike(underlying)
        return 0.0

    def get_current_month_futures_symbol(self, underlying: str = "BANKNIFTY") -> Optional[str]:
        if self._connected:
            return self._kite.get_current_month_futures_symbol(underlying)
        return None

    def get_current_week_expiry(self, underlying: str = "BANKNIFTY") -> Optional[str]:
        if self._connected:
            return self._kite.get_current_week_expiry(underlying)
        return None

    def get_option_chain(self, underlying: str, expiry: str) -> pd.DataFrame:
        if self._connected:
            return self._kite.get_option_chain(underlying, expiry)
        return pd.DataFrame()

    def get_option_symbol(
        self, underlying: str, expiry: str, strike: float, option_type: str
    ) -> str:
        if self._connected:
            return self._kite.get_option_symbol(underlying, expiry, strike, option_type)
        return ""

    def _apply_slippage(self, ltp: float, transaction_type: str) -> float:
        """Apply configurable BPS slippage: BUY fills higher, SELL fills lower."""
        factor = self._slippage_bps / 10_000.0
        if transaction_type == "BUY":
            return round(ltp * (1.0 + factor), 2)
        return round(ltp * (1.0 - factor), 2)

    def place_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: str,
        order_type: str = "MARKET",
        price: Optional[float] = None,
    ) -> str:
        """Simulate order: fill at LTP ± slippage_bps."""
        order_id = f"PAPER-{uuid4().hex[:8]}"
        raw_price = price or self.get_ltp(symbol)
        fill_price = self._apply_slippage(raw_price, transaction_type)
        self._simulated_orders[order_id] = {
            "order_id": order_id,
            "symbol": symbol,
            "quantity": quantity,
            "transaction_type": transaction_type,
            "order_type": order_type,
            "price": price,
            "fill_price": fill_price,
            "status": "COMPLETE",
            "timestamp": datetime.now(),
        }
        slip_pts = abs(fill_price - raw_price)
        log.info(
            f"[PAPER] {transaction_type} {symbol} qty={quantity} "
            f"@ ₹{fill_price:.2f} (LTP ₹{raw_price:.2f}, slip {slip_pts:.2f}) id={order_id}"
        )
        return order_id

    def exit_order(self, trade: Trade) -> str:
        return self.place_order(
            symbol=trade.symbol,
            quantity=trade.quantity,
            transaction_type="SELL",
            order_type="MARKET",
        )

    def get_order_status(self, order_id: str) -> dict:
        return self._simulated_orders.get(order_id, {})
