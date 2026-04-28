"""Broker and Ticker factory — reads active_market from config and returns the right implementation.

  active_market: india + trading_mode: paper  → PaperBroker(KiteBroker())
  active_market: india + trading_mode: live   → KiteBroker()
  active_market: us    + trading_mode: paper  → PaperBroker(AlpacaBroker())
  active_market: us    + trading_mode: live   → AlpacaBroker()

Ticker factory:
  india → TickerManager (KiteTicker WebSocket)
  us    → AlpacaTicker  (Alpaca WebSocket)
"""
from __future__ import annotations

from src.broker.base import BrokerBase
from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


def create_broker() -> BrokerBase:
    """Instantiate and return the correct broker for the active market."""
    market = config.active_market()
    mode   = config.get("mode.trading_mode", "paper").lower()

    if market == "us":
        from src.broker.alpaca_broker import AlpacaBroker
        live_broker = AlpacaBroker()
        if mode == "paper":
            from src.broker.paper_broker import PaperBroker
            log.info("Broker: US market — PaperBroker(AlpacaBroker) [simulated orders, live Alpaca prices]")
            return PaperBroker(data_broker=live_broker)
        log.info("Broker: US market — AlpacaBroker (live orders via Alpaca)")
        return live_broker

    # India (default)
    from src.broker.kite_broker import KiteBroker
    live_broker = KiteBroker()
    if mode == "paper":
        from src.broker.paper_broker import PaperBroker
        log.info("Broker: India market — PaperBroker(KiteBroker) [simulated orders, live Kite prices]")
        return PaperBroker(data_broker=live_broker)
    log.info("Broker: India market — KiteBroker (live orders via Zerodha)")
    return live_broker


def create_ticker(broker: BrokerBase):
    """Instantiate and return the correct WebSocket ticker for the active market."""
    market = config.active_market()

    if market == "us":
        from src.broker.alpaca_ticker import AlpacaTicker
        creds      = config.credentials.get("alpaca", {})
        api_key    = creds.get("api_key", "")
        secret_key = creds.get("secret_key", "")
        paper      = config.is_paper_mode()
        log.info(f"Ticker: AlpacaTicker ({'PAPER' if paper else 'LIVE'})")
        return AlpacaTicker(api_key=api_key, secret_key=secret_key, paper=paper)

    # India
    from src.broker.kite_ticker import TickerManager
    creds        = config.credentials.get("kite", {})
    api_key      = creds.get("api_key", "")
    access_token = creds.get("access_token", "")
    log.info("Ticker: KiteTicker (India / Zerodha)")
    return TickerManager(api_key=api_key, access_token=access_token)
