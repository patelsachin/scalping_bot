"""Market calendar and trading session time utilities.

All times are naive (no tzinfo) — interpreted against OS local time.
Set the OS timezone to the active market before starting the bot:
  - India trading: set OS to IST (UTC+5:30)
  - US trading:    set OS to Eastern Time (UTC-5 / UTC-4 with DST)
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta

from src.utils.config_loader import config


def now_ist() -> datetime:
    """Current local time (naive). OS must be set to the active market's timezone."""
    return datetime.now()


# Alias for market-agnostic code
now_market = now_ist


def today_ist() -> date:
    return datetime.now().date()


def _parse_hhmm(value: str) -> time:
    """Parse HH:MM string into a naive time object."""
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


def market_open_time() -> time:
    return _parse_hhmm(config.get("session.market_open", "09:15"))


def market_close_time() -> time:
    return _parse_hhmm(config.get("session.market_close", "15:30"))


def square_off_time() -> time:
    return _parse_hhmm(config.get("session.square_off_time", "15:20"))


def is_market_open(ts: datetime | None = None) -> bool:
    """Is it currently within market hours? (Mon-Fri, within configured session)"""
    ts = ts or datetime.now()
    if ts.weekday() >= 5:
        return False
    current = ts.time()
    return market_open_time() <= current <= market_close_time()


def is_square_off_time(ts: datetime | None = None) -> bool:
    """Have we passed the hard square-off time?"""
    ts = ts or datetime.now()
    return ts.time() >= square_off_time()


def is_trading_day(d: date | None = None) -> bool:
    """Mon-Fri check."""
    d = d or datetime.now().date()
    return d.weekday() < 5


def seconds_to_market_open() -> int:
    """Seconds until market opens today (or 0 if already open/past)."""
    ts = datetime.now()
    open_ts = datetime.combine(ts.date(), market_open_time())
    if ts >= open_ts:
        return 0
    return int((open_ts - ts).total_seconds())


def last_trading_day(reference: date | None = None) -> date:
    """Most recent completed trading weekday before `reference` (default: today).

    Mon morning → Friday, any other day → yesterday.
    Does NOT account for market holidays.
    """
    d = (reference or datetime.now().date()) - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def candle_start_time(ts: datetime, minutes: int) -> datetime:
    """Return the start time of the N-minute candle containing ts."""
    ts = ts.replace(tzinfo=None) if ts.tzinfo else ts
    market_start = datetime.combine(ts.date(), market_open_time())
    if ts < market_start:
        return market_start
    delta = ts - market_start
    candle_index = int(delta.total_seconds() // (minutes * 60))
    return market_start + timedelta(minutes=candle_index * minutes)
