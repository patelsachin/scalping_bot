"""Market calendar and trading session time utilities."""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.utils.config_loader import config

IST = ZoneInfo("Asia/Kolkata")


def now_ist() -> datetime:
    """Current time in IST."""
    return datetime.now(tz=IST)


def today_ist() -> date:
    return now_ist().date()


def _parse_hhmm(value: str) -> time:
    hh, mm = value.split(":")
    return time(int(hh), int(mm), tzinfo=IST)


def market_open_time() -> time:
    return _parse_hhmm(config.get("session.market_open", "09:15"))


def market_close_time() -> time:
    return _parse_hhmm(config.get("session.market_close", "15:30"))


def square_off_time() -> time:
    return _parse_hhmm(config.get("session.square_off_time", "15:20"))


def is_market_open(ts: datetime | None = None) -> bool:
    """Is it currently within market hours? (Mon-Fri, 09:15-15:30 IST)"""
    ts = ts or now_ist()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    if ts.weekday() >= 5:
        return False
    current = ts.timetz()
    return market_open_time() <= current <= market_close_time()


def is_square_off_time(ts: datetime | None = None) -> bool:
    """Have we passed the hard square-off time? (15:20 by default)."""
    ts = ts or now_ist()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    return ts.timetz() >= square_off_time()


def is_trading_day(d: date | None = None) -> bool:
    """Mon-Fri check. (TODO: integrate NSE holiday calendar.)"""
    d = d or today_ist()
    return d.weekday() < 5


def seconds_to_market_open() -> int:
    """Seconds until market opens today (or 0 if already open/past)."""
    ts = now_ist()
    open_ts = datetime.combine(ts.date(), market_open_time(), tzinfo=IST)
    if ts >= open_ts:
        return 0
    return int((open_ts - ts).total_seconds())


def candle_start_time(ts: datetime, minutes: int) -> datetime:
    """Return the start time of the N-minute candle containing ts."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=IST)
    # Align to market open
    market_start = datetime.combine(ts.date(), market_open_time(), tzinfo=IST)
    if ts < market_start:
        return market_start
    delta = ts - market_start
    candle_index = int(delta.total_seconds() // (minutes * 60))
    return market_start + timedelta(minutes=candle_index * minutes)
