"""Zerodha Kite Connect live broker implementation."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.broker.base import BrokerBase
from src.core.models import Trade, TradeType
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


class KiteBroker(BrokerBase):
    """Live broker using Zerodha Kite Connect."""

    def __init__(self) -> None:
        self._kite = None
        self._instrument_cache: dict = {}

    def connect(self) -> bool:
        try:
            from kiteconnect import KiteConnect
        except ImportError as e:
            log.error(f"kiteconnect not installed: {e}")
            return False

        creds = config.credentials.get("kite", {})
        api_key = creds.get("api_key", "")
        access_token = creds.get("access_token", "")

        if not api_key or not access_token:
            log.error("Kite api_key or access_token missing in credentials.yaml")
            return False

        self._kite = KiteConnect(api_key=api_key)
        self._kite.set_access_token(access_token)

        try:
            profile = self._kite.profile()
            log.info(f"Kite connected. User: {profile.get('user_name', 'unknown')}")
            return True
        except Exception as e:
            log.error(f"Kite authentication failed: {e}")
            return False

    def is_paper(self) -> bool:
        return False

    # ------------------------------------------------------------
    # Instrument lookup
    # ------------------------------------------------------------
    def _load_instruments(self, exchange: str = "NFO") -> pd.DataFrame:
        if exchange in self._instrument_cache:
            return self._instrument_cache[exchange]
        try:
            data = self._kite.instruments(exchange)
            df = pd.DataFrame(data)
            self._instrument_cache[exchange] = df
            return df
        except Exception as e:
            log.error(f"Failed to load instruments for {exchange}: {e}")
            return pd.DataFrame()

    def get_instrument_token(self, tradingsymbol: str, exchange: str = "NFO") -> Optional[int]:
        df = self._load_instruments(exchange)
        if df.empty:
            return None
        match = df[df["tradingsymbol"] == tradingsymbol]
        if match.empty:
            return None
        return int(match.iloc[0]["instrument_token"])

    # ------------------------------------------------------------
    # Quotes
    # ------------------------------------------------------------
    def get_ltp(self, symbol: str) -> float:
        try:
            exch = "NFO" if not symbol.startswith("NSE:") else "NSE"
            key = symbol if ":" in symbol else f"{exch}:{symbol}"
            data = self._kite.ltp([key])
            return float(data[key]["last_price"])
        except Exception as e:
            log.error(f"get_ltp failed for {symbol}: {e}")
            return 0.0

    def get_underlying_ltp(self, underlying: str = "BANKNIFTY") -> float:
        """Spot LTP for the index (NFO index)."""
        try:
            key = f"NSE:NIFTY BANK" if underlying == "BANKNIFTY" else f"NSE:NIFTY 50"
            data = self._kite.ltp([key])
            return float(data[key]["last_price"])
        except Exception as e:
            log.error(f"get_underlying_ltp failed: {e}")
            return 0.0

    # ------------------------------------------------------------
    # Historical data
    # ------------------------------------------------------------
    def get_historical_candles(
        self,
        symbol: str,
        interval: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        """Fetch historical candles. interval: minute, 3minute, 5minute, 15minute, 60minute, day."""
        exchange = "NSE" if "NIFTY BANK" in symbol or "NIFTY 50" in symbol else "NFO"
        token = self.get_instrument_token(symbol, exchange)
        if not token:
            log.error(f"No instrument token for {symbol}")
            return pd.DataFrame()

        try:
            data = self._kite.historical_data(
                instrument_token=token,
                from_date=from_dt,
                to_date=to_dt,
                interval=interval,
                oi=True,
            )
            df = pd.DataFrame(data)
            if df.empty:
                return df
            df["date"] = pd.to_datetime(df["date"])
            df = df.set_index("date")
            df = df.rename(columns={"oi": "open_interest"})
            return df[["open", "high", "low", "close", "volume", "open_interest"]]
        except Exception as e:
            log.error(f"historical_data failed: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------
    # Option chain helpers
    # ------------------------------------------------------------
    def get_atm_strike(self, underlying: str = "BANKNIFTY") -> float:
        spot = self.get_underlying_ltp(underlying)
        if spot == 0:
            return 0.0
        # BankNifty strikes are in 100 increments
        step = 100 if underlying == "BANKNIFTY" else 50
        return round(spot / step) * step

    def get_current_week_expiry(self, underlying: str = "BANKNIFTY") -> Optional[str]:
        df = self._load_instruments("NFO")
        if df.empty:
            return None
        mask = (df["name"] == underlying) & (df["segment"] == "NFO-OPT")
        options = df[mask].copy()
        if options.empty:
            return None
        options["expiry"] = pd.to_datetime(options["expiry"])
        today = pd.Timestamp.now().normalize()
        future = options[options["expiry"] >= today].sort_values("expiry")
        if future.empty:
            return None
        return future.iloc[0]["expiry"].strftime("%Y-%m-%d")

    def get_option_chain(self, underlying: str, expiry: str) -> pd.DataFrame:
        df = self._load_instruments("NFO")
        if df.empty:
            return df
        df["expiry"] = pd.to_datetime(df["expiry"])
        expiry_dt = pd.to_datetime(expiry)
        mask = (
            (df["name"] == underlying)
            & (df["expiry"] == expiry_dt)
            & (df["segment"] == "NFO-OPT")
        )
        return df[mask].copy()

    def get_option_symbol(
        self, underlying: str, expiry: str, strike: float, option_type: str
    ) -> str:
        """option_type: 'CE' or 'PE'. Returns tradingsymbol."""
        chain = self.get_option_chain(underlying, expiry)
        if chain.empty:
            return ""
        match = chain[
            (chain["strike"] == strike) & (chain["instrument_type"] == option_type)
        ]
        if match.empty:
            return ""
        return match.iloc[0]["tradingsymbol"]

    # ------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------
    def place_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: str,
        order_type: str = "MARKET",
        price: Optional[float] = None,
    ) -> str:
        try:
            order_id = self._kite.place_order(
                variety=self._kite.VARIETY_REGULAR,
                exchange=self._kite.EXCHANGE_NFO,
                tradingsymbol=symbol,
                transaction_type=transaction_type,
                quantity=quantity,
                product=self._kite.PRODUCT_MIS,  # intraday
                order_type=order_type,
                price=price,
                validity=self._kite.VALIDITY_DAY,
            )
            log.info(f"Order placed: {symbol} {transaction_type} qty={quantity} id={order_id}")
            return str(order_id)
        except Exception as e:
            log.error(f"place_order failed: {e}")
            return ""

    def exit_order(self, trade: Trade) -> str:
        """Exit an open position by placing an opposite order.
        For options we always BUY to enter and SELL to exit (we're long the premium).
        """
        return self.place_order(
            symbol=trade.symbol,
            quantity=trade.quantity,
            transaction_type="SELL",
            order_type="MARKET",
        )

    def get_order_status(self, order_id: str) -> dict:
        try:
            history = self._kite.order_history(order_id)
            if not history:
                return {}
            return history[-1]
        except Exception as e:
            log.error(f"get_order_status failed: {e}")
            return {}

    def get_positions(self) -> list[dict]:
        try:
            positions = self._kite.positions()
            return positions.get("net", [])
        except Exception as e:
            log.error(f"get_positions failed: {e}")
            return []
