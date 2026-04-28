"""Paper broker: wraps any data broker for live price feeds, simulates order execution locally."""
from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import uuid4

import pandas as pd

from src.broker.base import BrokerBase
from src.core.models import Trade
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


class PaperBroker(BrokerBase):
    """Paper trading: fetches live prices via the supplied data_broker, simulates orders locally.

    Works for both India (data_broker=KiteBroker) and US (data_broker=AlpacaBroker).
    The factory (broker_factory.py) wires up the correct data_broker at startup.
    """

    def __init__(self, data_broker: BrokerBase) -> None:
        self._data = data_broker
        self._simulated_orders: dict[str, dict] = {}
        self._connected = False
        self._slippage_bps: float = float(config.get("paper_trading.slippage_bps", 5))

    def connect(self) -> bool:
        ok = self._data.connect()
        if ok:
            log.info(f"Paper broker connected (data via {type(self._data).__name__}).")
            self._connected = True
        else:
            log.warning("Paper broker: data broker connection failed. Live prices unavailable.")
            self._connected = False
        return True  # paper mode always "connects"

    def is_paper(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Delegate all data methods to the underlying data broker
    # ------------------------------------------------------------------
    def get_ltp(self, symbol: str) -> float:
        return self._data.get_ltp(symbol) if self._connected else 0.0

    def get_underlying_ltp(self, underlying: str) -> float:
        return self._data.get_underlying_ltp(underlying) if self._connected else 0.0

    def get_historical_candles(self, symbol: str, interval: str, from_dt: datetime, to_dt: datetime) -> pd.DataFrame:
        return self._data.get_historical_candles(symbol, interval, from_dt, to_dt) if self._connected else pd.DataFrame()

    def get_atm_strike(self, underlying: str) -> float:
        return self._data.get_atm_strike(underlying) if self._connected else 0.0

    def get_current_month_futures_symbol(self, underlying: str) -> Optional[str]:
        return self._data.get_current_month_futures_symbol(underlying) if self._connected else None

    def get_current_week_expiry(self, underlying: str) -> Optional[str]:
        return self._data.get_current_week_expiry(underlying) if self._connected else None

    def get_option_chain(self, underlying: str, expiry: str) -> pd.DataFrame:
        return self._data.get_option_chain(underlying, expiry) if self._connected else pd.DataFrame()

    def get_option_symbol(self, underlying: str, expiry: str, strike: float, option_type: str) -> str:
        return self._data.get_option_symbol(underlying, expiry, strike, option_type) if self._connected else ""

    def get_instrument_token(self, symbol: str, exchange: str = "") -> Optional[int]:
        return self._data.get_instrument_token(symbol, exchange)

    def get_seed_symbol(self, underlying: str) -> str:
        return self._data.get_seed_symbol(underlying)

    # ------------------------------------------------------------------
    # Simulated order execution
    # ------------------------------------------------------------------
    def _apply_slippage(self, ltp: float, transaction_type: str) -> float:
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
        order_id = f"PAPER-{uuid4().hex[:8]}"
        raw_price = price or self.get_ltp(symbol)
        fill_price = self._apply_slippage(raw_price, transaction_type)
        self._simulated_orders[order_id] = {
            "order_id": order_id,
            "symbol": symbol,
            "quantity": quantity,
            "transaction_type": transaction_type,
            "fill_price": fill_price,
            "status": "COMPLETE",
            "timestamp": datetime.now(),
        }
        slip_pts = abs(fill_price - raw_price)
        log.info(
            f"[PAPER] {transaction_type} {symbol} qty={quantity} "
            f"@ {fill_price:.2f} (LTP {raw_price:.2f}, slip {slip_pts:.2f}) id={order_id}"
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
