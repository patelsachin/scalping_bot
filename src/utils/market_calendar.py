"""Market calendar and trading session time utilities.

Timezone is read from config (timezone: Asia/Kolkata for India,
America/New_York for US). All session time strings in config are
interpreted in that market's local timezone.
"""
from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from src.utils.config_loader import config


def market_tz() -> ZoneInfo:
    """Active market timezone — read from merged config at call time."""
    return ZoneInfo(config.get("timezone", "Asia/Kolkata"))


# Keep IST as a module-level alias for backward compat; engine.py
# now calls market_tz() directly so this is rarely used externally.
IST = ZoneInfo("Asia/Kolkata")


def now_ist() -> datetime:
    """Current time in the active market's timezone."""
    return datetime.now(tz=market_tz())


# Alias for clarity in market-agnostic code
now_market = now_ist


def today_ist() -> date:
    return now_ist().date()


def _parse_hhmm(value: str) -> time:
    hh, mm = value.split(":")
    return time(int(hh), int(mm), tzinfo=market_tz())


def _parse_hhmm_naive(value: str) -> time:
    """Parse HH:MM without tzinfo (for use in datetime.combine)."""
    hh, mm = value.split(":")
    return time(int(hh), int(mm))


def market_open_time() -> time:
    return _parse_hhmm(config.get("session.market_open", "09:15"))


def market_open_time_naive() -> time:
    return _parse_hhmm_naive(config.get("session.market_open", "09:15"))


def market_close_time() -> time:
    return _parse_hhmm(config.get("session.market_close", "15:30"))


def market_close_time_naive() -> time:
    return _parse_hhmm_naive(config.get("session.market_close", "15:30"))


def square_off_time() -> time:
    return _parse_hhmm(config.get("session.square_off_time", "15:20"))


def is_market_open(ts: datetime | None = None) -> bool:
    """Is it currently within market hours? (Mon-Fri, within configured session)"""
    ts = ts or now_ist()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=market_tz())
    if ts.weekday() >= 5:
        return False
    current = ts.timetz()
    return market_open_time() <= current <= market_close_time()


def is_square_off_time(ts: datetime | None = None) -> bool:
    """Have we passed the hard square-off time?"""
    ts = ts or now_ist()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=market_tz())
    return ts.timetz() >= square_off_time()


def is_trading_day(d: date | None = None) -> bool:
    """Mon-Fri check."""
    d = d or today_ist()
    return d.weekday() < 5


def seconds_to_market_open() -> int:
    """Seconds until market opens today (or 0 if already open/past)."""
    ts = now_ist()
    mtz = market_tz()
    open_ts = datetime.combine(ts.date(), market_open_time(), tzinfo=mtz)
    if ts >= open_ts:
        return 0
    return int((open_ts - ts).total_seconds())


def last_trading_day(reference: date | None = None) -> date:
    """Most recent completed trading weekday before `reference` (default: today).

    Mon morning → Friday, any other day → yesterday.
    Does NOT account for market holidays.
    """
    d = (reference or today_ist()) - timedelta(days=1)
    while d.weekday() >= 5:
        d -= timedelta(days=1)
    return d


def candle_start_time(ts: datetime, minutes: int) -> datetime:
    """Return the start time of the N-minute candle containing ts."""
    mtz = market_tz()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=mtz)
    market_start = datetime.combine(ts.date(), market_open_time(), tzinfo=mtz)
    if ts < market_start:
        return market_start
    delta = ts - market_start
    candle_index = int(delta.total_seconds() // (minutes * 60))
    return market_start + timedelta(minutes=candle_index * minutes)
