"""Kite access token freshness checker.

Kite tokens expire at 6 AM the following day. This module checks whether the
stored token was generated today. If stale, it logs a warning and returns False
so the caller can abort startup or prompt re-login.

The token date is written to credentials.yaml by kite_login.py each time a new
session is generated. Run ``python -m src.broker.kite_login`` to refresh it.
"""
from __future__ import annotations

from datetime import date

from src.utils.config_loader import config
from src.utils.logger import get_logger

log = get_logger(__name__)


def is_token_fresh() -> bool:
    """Return True if credentials.yaml contains a token dated today."""
    creds     = config.credentials.get("kite", {})
    token     = creds.get("access_token", "")
    token_date = creds.get("token_date", "")   # written by kite_login.py

    if not token:
        log.error("No access_token found in credentials.yaml.")
        return False

    today_str = date.today().isoformat()        # e.g. "2026-04-21"

    if not token_date:
        # token_date missing → old credentials file; assume token *may* be valid
        # but warn so the user knows they should run kite_login.py once
        log.warning(
            "credentials.yaml has no 'token_date'. Cannot verify token freshness. "
            "Run: python -m src.broker.kite_login  to refresh and stamp the date."
        )
        return True   # fail-open: don't block the bot over a missing field

    if token_date != today_str:
        log.error(
            f"Access token is stale (token_date={token_date}, today={today_str}). "
            "Run:  python -m src.broker.kite_login  or use  daily_start.bat"
        )
        return False

    log.info(f"Access token is fresh (token_date={token_date}).")
    return True


def check_and_warn() -> None:
    """Check token freshness and log an appropriate message. Does not raise."""
    if not is_token_fresh():
        log.warning(
            "=" * 60 + "\n"
            "  STALE TOKEN — bot will likely fail to connect.\n"
            "  Run daily_start.bat or:  python -m src.broker.kite_login\n"
            + "=" * 60
        )
