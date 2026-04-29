"""Alpaca Markets broker — US equities and options (paper and live).

Implements BrokerBase so the engine is fully market-agnostic.

Token scheme:
  Alpaca uses symbol strings, not numeric tokens. We generate a stable
  fake integer token via abs(hash(symbol)) % 2**31 and maintain a
  registry so the AlpacaTicker can reverse-map token → symbol.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd

from src.broker.base import BrokerBase
from src.core.models import Trade
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)

# Shared symbol ↔ token registry used by both AlpacaBroker and AlpacaTicker
_symbol_to_token: dict[str, int] = {}
_token_to_symbol: dict[int, str] = {}


def _register(symbol: str) -> int:
    """Register a symbol and return its stable fake token."""
    if symbol not in _symbol_to_token:
        token = abs(hash(symbol)) % (2 ** 31)
        _symbol_to_token[symbol] = token
        _token_to_symbol[token] = symbol
    return _symbol_to_token[symbol]


def resolve_symbol(token: int) -> Optional[str]:
    """Reverse-lookup: token → symbol (used by AlpacaTicker)."""
    return _token_to_symbol.get(token)


def _is_option(symbol: str) -> bool:
    """Detect OCC option symbol: e.g. SPY260429C00500000."""
    return bool(re.match(r"^[A-Z]+\d{6}[CP]\d+$", symbol))


_INTERVAL_MAP: dict[str, tuple] = {
    # Kite interval string → (TimeFrameUnit, amount)
    "minute":   ("Minute", 1),
    "3minute":  ("Minute", 3),
    "5minute":  ("Minute", 5),
    "15minute": ("Minute", 15),
    "60minute": ("Hour",   1),
    "day":      ("Day",    1),
}


class AlpacaBroker(BrokerBase):
    """Live (or paper-API) broker using Alpaca Markets."""

    def __init__(self) -> None:
        self._paper = config.is_paper_mode()
        self._trading_client = None
        self._stock_client = None
        self._option_client = None

    # ------------------------------------------------------------------
    # Connection
    # ------------------------------------------------------------------
    def connect(self) -> bool:
        try:
            from alpaca.trading.client import TradingClient
            from alpaca.data.historical import StockHistoricalDataClient, OptionHistoricalDataClient
        except ImportError:
            log.error("alpaca-py not installed. Run: pip install alpaca-py")
            return False

        creds = config.credentials.get("alpaca", {})
        api_key    = creds.get("api_key", "")
        secret_key = creds.get("secret_key", "")

        if not api_key or not secret_key:
            log.error("Alpaca api_key / secret_key missing in credentials.yaml")
            return False

        try:
            self._trading_client = TradingClient(
                api_key, secret_key, paper=self._paper
            )
            self._stock_client  = StockHistoricalDataClient(api_key, secret_key)
            self._option_client = OptionHistoricalDataClient(api_key, secret_key)
            acct = self._trading_client.get_account()
            log.info(
                f"Alpaca connected ({'PAPER' if self._paper else 'LIVE'}) | "
                f"account={acct.id} | equity=${float(acct.equity):,.2f}"
            )
            return True
        except Exception as e:
            log.error(f"Alpaca connection failed: {e}")
            return False

    def is_paper(self) -> bool:
        return self._paper

    # ------------------------------------------------------------------
    # Token registry
    # ------------------------------------------------------------------
    def get_instrument_token(self, symbol: str, exchange: str = "") -> Optional[int]:
        return _register(symbol)

    def get_seed_symbol(self, underlying: str) -> str:
        return underlying  # SPY → "SPY" directly

    # ------------------------------------------------------------------
    # Quotes
    # ------------------------------------------------------------------
    def get_ltp(self, symbol: str) -> float:
        try:
            if _is_option(symbol):
                return self._get_option_mid(symbol)
            return self._get_stock_last(symbol)
        except Exception as e:
            log.error(f"get_ltp failed for {symbol}: {e}")
            return 0.0

    def get_underlying_ltp(self, underlying: str) -> float:
        return self._get_stock_last(underlying)

    def _get_stock_last(self, symbol: str) -> float:
        from alpaca.data.requests import StockLatestTradeRequest
        resp = self._stock_client.get_stock_latest_trade(
            StockLatestTradeRequest(symbol_or_symbols=symbol)
        )
        return float(resp[symbol].price)

    def _get_option_mid(self, symbol: str) -> float:
        from alpaca.data.requests import OptionLatestQuoteRequest
        resp = self._option_client.get_option_latest_quote(
            OptionLatestQuoteRequest(symbol_or_symbols=symbol)
        )
        q = resp[symbol]
        bid = float(q.bid_price or 0)
        ask = float(q.ask_price or 0)
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2, 2)
        return float(q.ask_price or q.bid_price or 0)

    # ------------------------------------------------------------------
    # Historical candles
    # ------------------------------------------------------------------
    def get_historical_candles(
        self,
        symbol: str,
        interval: str,
        from_dt: datetime,
        to_dt: datetime,
    ) -> pd.DataFrame:
        try:
            from alpaca.data.requests import StockBarsRequest
            from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

            unit_name, amount = _INTERVAL_MAP.get(interval, ("Minute", 1))
            unit = getattr(TimeFrameUnit, unit_name)
            tf   = TimeFrame(amount, unit)

            req = StockBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=tf,
                start=from_dt,
                end=to_dt,
                adjustment="raw",
            )
            bars_resp = self._stock_client.get_stock_bars(req)
            try:
                bars = bars_resp[symbol]
            except (KeyError, TypeError):
                bars = []
            if not bars:
                return pd.DataFrame()

            rows = []
            for b in bars:
                rows.append({
                    "date":   b.timestamp,
                    "open":   float(b.open),
                    "high":   float(b.high),
                    "low":    float(b.low),
                    "close":  float(b.close),
                    "volume": float(b.volume),
                    "open_interest": 0.0,
                })
            df = pd.DataFrame(rows).set_index("date")
            df.index = pd.to_datetime(df.index)
            return df
        except Exception as e:
            log.error(f"get_historical_candles failed for {symbol}: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Strike / expiry helpers
    # ------------------------------------------------------------------
    def get_atm_strike(self, underlying: str = "SPY") -> float:
        spot  = self.get_underlying_ltp(underlying)
        if spot == 0:
            return 0.0
        step  = int(config.get("instrument.strike_step", 1))
        return round(round(spot / step) * step, 2)

    def get_current_week_expiry(self, underlying: str = "SPY") -> Optional[str]:
        expiry_type = config.get("instrument.expiry_type", "0dte")
        today = datetime.now().date()
        if expiry_type == "0dte":
            return today.strftime("%Y-%m-%d")
        # weekly → next Friday (or today if already Friday)
        days_ahead = (4 - today.weekday()) % 7
        expiry = today + timedelta(days=days_ahead)
        return expiry.strftime("%Y-%m-%d")

    def get_option_symbol(
        self, underlying: str, expiry: str, strike: float, option_type: str
    ) -> str:
        """Build OCC option symbol: SPY260429C00500000."""
        exp_dt  = datetime.strptime(expiry, "%Y-%m-%d")
        exp_str = exp_dt.strftime("%y%m%d")           # 260429
        right   = "C" if option_type == "CE" else "P"  # C for call, P for put
        strike_int = int(round(strike * 1000))          # $500.00 → 500000
        return f"{underlying}{exp_str}{right}{strike_int:08d}"

    def get_option_chain(self, underlying: str, expiry: str) -> pd.DataFrame:
        try:
            from alpaca.trading.requests import GetOptionContractsRequest
            from alpaca.trading.enums import ContractType
            exp_date = datetime.strptime(expiry, "%Y-%m-%d").date()
            req = GetOptionContractsRequest(
                underlying_symbols=[underlying],
                expiration_date=exp_date,
                limit=200,
            )
            contracts = self._trading_client.get_option_contracts(req).option_contracts
            if not contracts:
                return pd.DataFrame()
            rows = [
                {
                    "symbol":      c.symbol,
                    "strike":      float(c.strike_price),
                    "option_type": "CE" if c.type == ContractType.CALL else "PE",
                    "expiry":      expiry,
                }
                for c in contracts
            ]
            return pd.DataFrame(rows)
        except Exception as e:
            log.error(f"get_option_chain failed: {e}")
            return pd.DataFrame()

    # ------------------------------------------------------------------
    # Orders
    # ------------------------------------------------------------------
    def place_order(
        self,
        symbol: str,
        quantity: int,
        transaction_type: str,
        order_type: str = "MARKET",
        price: Optional[float] = None,
    ) -> str:
        try:
            from alpaca.trading.requests import MarketOrderRequest, LimitOrderRequest
            from alpaca.trading.enums import OrderSide, TimeInForce

            side = OrderSide.BUY if transaction_type == "BUY" else OrderSide.SELL

            if order_type == "LIMIT" and price:
                req = LimitOrderRequest(
                    symbol=symbol,
                    qty=quantity,
                    side=side,
                    time_in_force=TimeInForce.DAY,
                    limit_price=str(price),
                )
            else:
                req = MarketOrderRequest(
                    symbol=symbol,
                    qty=quantity,
                    side=side,
                    time_in_force=TimeInForce.DAY,
                )
            order = self._trading_client.submit_order(req)
            log.info(f"Alpaca order: {transaction_type} {symbol} qty={quantity} id={order.id}")
            return str(order.id)
        except Exception as e:
            log.error(f"place_order failed for {symbol}: {e}")
            return ""

    def exit_order(self, trade: Trade) -> str:
        return self.place_order(
            symbol=trade.symbol,
            quantity=trade.quantity,
            transaction_type="SELL",
            order_type="MARKET",
        )

    def get_order_status(self, order_id: str) -> dict:
        try:
            order = self._trading_client.get_order_by_id(order_id)
            return {"status": order.status.value, "filled_qty": order.filled_qty}
        except Exception as e:
            log.error(f"get_order_status failed for {order_id}: {e}")
            return {}
